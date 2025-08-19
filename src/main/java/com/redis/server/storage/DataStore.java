package com.redis.server.storage;

import com.redis.server.model.QueuedCommand;
import com.redis.server.model.RedisSortedSet;
import com.redis.server.model.RedisStream;
import com.redis.server.model.SortedSetMember;

import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DataStore {
    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> expiry = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RedisStream> streams = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> clientMultiStates = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Queue<QueuedCommand>> clientQueuedCommands = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RedisSortedSet> zsets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Map<String, Integer>> clientChannels = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> clientSubStates = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Map<String, OutputStream>> channelClients = new ConcurrentHashMap<>();
    private final HashSet<String> allowedCommandsInSubMode = new HashSet<>() {
        {
            add("SUBSCRIBE");
            add("UNSUBSCRIBE");
            add("PSUBSCRIBE");
            add("PUNSUBSCRIBE");
            add("PING");
            add("QUIT");
        }
    };

    public void set(String key, String value) {
        store.put(key, value);
        expiry.remove(key);
    }

    public void set(String key, String value, long expiryTime) {
        store.put(key, value);
        expiry.put(key, expiryTime);
    }

    public List<String> getAllKeys(){
        return new ArrayList<>(store.keySet());
    }

    public String getValue(String key) {
        if (isExpired(key)) {
            cleanupExpiredKey(key);
            return null;
        }
        return store.get(key);
    }

    public boolean hasKey(String key) {
        if (isExpired(key)) {
            cleanupExpiredKey(key);
            return false;
        }
        return store.containsKey(key);
    }

    public List<String> getList(String key) {
        return lists.get(key);
    }

    public void setList(String key, List<String> list) {
        lists.put(key, list);
    }

    public boolean hasListKey(String key) {
        return lists.containsKey(key);
    }

    public RedisStream getStream(String key) {
        return streams.get(key);
    }

    public void setStream(String key, RedisStream stream) {
        streams.put(key, stream);
    }

    public boolean hasStreamKey(String key) {
        return streams.containsKey(key);
    }

    private boolean isExpired(String key) {
        Long expiryTime = expiry.get(key);
        return expiryTime != null && System.currentTimeMillis() > expiryTime;
    }

    private void cleanupExpiredKey(String key) {
        store.remove(key);
        expiry.remove(key);
    }

    public String getKeyType(String key) {
        if (hasKey(key)) {
            return "string";
        } else if (hasStreamKey(key)) {
            return "stream";
        }
        return "none";
    }

    public void putCommandInQueue(String clientId, List<String> command, OutputStream out) {
        clientQueuedCommands.computeIfAbsent(clientId, k -> new LinkedList<>()).offer(new QueuedCommand(command, out));
    }

    public boolean isMultiEnabled(String clientID) {
        return clientMultiStates.getOrDefault(clientID, false);
    }

    public void enableMulti(String clientId) {
        clientMultiStates.put(clientId, true);
        clientQueuedCommands.computeIfAbsent(clientId, k -> new LinkedList<>());
    }

    public void disableMulti(String clientId) {
        clientMultiStates.put(clientId, false);
    }

    public boolean hasQueuedCommand(String clientId){
        Queue<QueuedCommand> queue = clientQueuedCommands.get(clientId);
        return queue != null && !queue.isEmpty();
    }

    public QueuedCommand pollQueuedCommand(String clientId){
        Queue<QueuedCommand> queue = clientQueuedCommands.get(clientId);
        return queue != null ? queue.poll() : null;
    }

    public int getQueuedCommandSize(String clientId){
        Queue<QueuedCommand> queue = clientQueuedCommands.get(clientId);
        return queue != null ? queue.size() : 0;
    }

    public void discardQueuedCommands(String clientId) {
        clientQueuedCommands.remove(clientId);
        disableMulti(clientId);
    }

    // To remove client data when client disconnects
    public void cleanupClient(String clientId) {
        clientMultiStates.remove(clientId);
        clientQueuedCommands.remove(clientId);
    }

    public RedisSortedSet getSortedSet(String key) {
        return zsets.get(key);
    }

    public int addZsetMember(String key, SortedSetMember member) {
        if(!zsets.containsKey(key)) zsets.put(key, new RedisSortedSet());
        return zsets.get(key).addMember(member);
    }

    public double getZsetMemberScore(String key, String member) {
        return !zsets.containsKey(key) ? -1 : zsets.get(key).getScore(member);
    }

    public int getZsetMemberRank(String key, String member) {
        if(!zsets.containsKey(key)) return -1;
        return zsets.get(key).getRank(member);
    }

    public List<String> getZsetMembers(String key, int start, int end) {
        if(!zsets.containsKey(key)) return new ArrayList<>();
        return zsets.get(key).getMembersInRange(start, end);
    }

    public int removeZsetMember(String key, String member){
        return !zsets.containsKey(key) ? 0 : zsets.get(key).remove(member);
    }

    public int getZsetMemberCount(String key) {
        return !zsets.containsKey(key) ? 0 : zsets.get(key).size();
    }

    public boolean hasZsetKey(String key) {
        return zsets.containsKey(key);
    }

    public void subscribeChannel(String clientId, String channel, OutputStream out){
        if(!channelClients.containsKey(channel)) channelClients.put(channel, new HashMap<>());
        Map<String, OutputStream> clients = channelClients.get(channel);
        if(!clients.containsKey(clientId)) {
            clients.put(clientId, out);
        }

        if(!clientChannels.containsKey(clientId)) clientChannels.put(clientId, new HashMap<>());
        Map<String, Integer> channels = clientChannels.get(clientId);
        if(!channels.containsKey(channel)) {
            channels.put(channel, channels.size()+1);
        }
    }

    public int unsubscribeChannel(String clientId, String channel) {
        int count = clientChannels.get(clientId).size();

        channelClients.get(channel).remove(clientId);
        if(!clientChannels.get(clientId).containsKey(channel)) {
            clientChannels.get(clientId).remove(channel);
            return count;
        }

        return count - 1;
    }

    public int getSubCount(String clientId, String channel){
        return clientChannels.get(clientId).get(channel);
    }

    public int getSubscribedClientsCount(String channel){
        return channelClients.getOrDefault(channel, new HashMap<>()).size();
    }

    public List<OutputStream> getSubscribedClients(String channel) {
        Map<String, OutputStream> clients = channelClients.getOrDefault(channel, Collections.emptyMap());
        return new ArrayList<>(clients.values());
    }

    public boolean isClientSubscribed(String clientID) {
        return clientSubStates.getOrDefault(clientID, false);
    }

    public void subscribeClient(String clientId) {
        clientSubStates.put(clientId, true);
    }

    public void unsubscribeClient(String clientId) {
        clientSubStates.put(clientId, false);
    }

    public boolean isAllowedInSubMode(String command){
        return allowedCommandsInSubMode.contains(command);
    }
}
