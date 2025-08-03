package com.redis.server.storage;

import com.redis.server.model.QueuedCommand;
import com.redis.server.model.RedisStream;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

public class DataStore {
    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> expiry = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RedisStream> streams = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> clientMultiStates = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Queue<QueuedCommand>> clientQueuedCommands = new ConcurrentHashMap<>();


    public void setValue(String key, String value) {
        store.put(key, value);
        expiry.remove(key);
    }

    public void setValue(String key, String value, long expiryTime) {
        store.put(key, value);
        expiry.put(key, expiryTime);
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
}
