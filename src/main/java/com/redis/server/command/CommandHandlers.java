package com.redis.server.command;

import com.redis.server.RedisConstants;
import com.redis.server.blocking.BlockingOperationsManager;
import com.redis.server.model.*;
import com.redis.server.protocol.RespProtocol;
import com.redis.server.storage.DataStore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

import static com.redis.server.protocol.RespProtocol.*;


public class CommandHandlers {

    private final DataStore dataStore;
    private final BlockingOperationsManager blockingManager;
    private final ServerConfig serverConfig;

    public CommandHandlers(DataStore dataStore, BlockingOperationsManager blockingManager, ServerConfig serverConfig) {
        this.dataStore = dataStore;
        this.blockingManager = blockingManager;
        this.serverConfig = serverConfig;
    }

    public void handlePing(String clientId, List<String> command, OutputStream out) throws IOException {
        if(dataStore.isClientSubscribed(clientId)){
            writeArray(2, out);
            writeBulkString("pong", out);
            writeBulkString("", out);
            return;
        }
        else if (serverConfig.isMaster()) writeSimpleString(RedisConstants.PONG, out);
        else serverConfig.setReplicaOffset(serverConfig.getReplicaOffset() + RedisConstants.PING_COMMAND_BYTE_SIZE);
    }

    public void handleEcho(String clientId, List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'ECHO' command", out);
            return;
        }

        if(dataStore.isClientSubscribed(clientId)) {
            writeError(RedisConstants.ERR_CAN_NOT_EXECUTE + " 'ECHO' command in subscribed mode", out);
            return;
        }

        String arg = command.get(1);
        writeBulkString(arg, out);
    }

    public void handleType(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'TYPE' command", out);
            return;
        }

        String key = command.get(1);
        String keyType = dataStore.getKeyType(key);
        writeSimpleString(keyType, out);
    }

    public void handleSet(String clientId, List<String> command, OutputStream out) throws IOException {
        if (command.size() < 3) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'SET' command", out);
            return;
        }

        if(dataStore.isClientSubscribed(clientId)) {
            writeError(RedisConstants.ERR_CAN_NOT_EXECUTE + " 'SET' command in subscribed mode", out);
            return;
        }

        // propagate command to replicas
        if (serverConfig.hasReplicas()) {
            List<OutputStream> replicas = serverConfig.getReplicaOutputStreams();

            for (OutputStream replicaOutputStream : replicas) {
                writeArray(command.size(), replicaOutputStream);
                for (String arg : command) writeBulkString(arg, replicaOutputStream);
            }
        }

        if (dataStore.isMultiEnabled(clientId)) {
            dataStore.putCommandInQueue(clientId, command, out);
            writeSimpleString("QUEUED", out);
            System.out.println("no");
            return;
        }

        String key = command.get(1);
        String value = command.get(2);

        if (command.size() >= 5 && "PX".equalsIgnoreCase(command.get(3))) {
            long expiryMs = Long.parseLong(command.get(4));
            long expiryTime = System.currentTimeMillis() + expiryMs;
            dataStore.setValue(key, value, expiryTime);
        } else {
            dataStore.setValue(key, value);
        }

        // System.out.println("Replica? " + serverConfig.isReplica() + " Set value: " + dataStore.getValue(key));
        int bytes = RespProtocol.calculateRespCommandBytes(command);

        if (serverConfig.isMaster()) {
            serverConfig.setMasterOffset(serverConfig.getMasterOffset() + bytes);
            System.out.println("master offset: " + serverConfig.getMasterOffset());
            writeSimpleString(RedisConstants.OK, out);
        } else {
            serverConfig.setReplicaOffset(serverConfig.getReplicaOffset() + bytes);
        }
    }

    public void handleGet(String clientId, List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'GET' command", out);
            return;
        }

        if(dataStore.isClientSubscribed(clientId)) {
            writeError(RedisConstants.ERR_CAN_NOT_EXECUTE + " 'GET' command in subscribed mode", out);
            return;
        }

        if (dataStore.isMultiEnabled(clientId)) {
            dataStore.putCommandInQueue(clientId, command, out);
            writeSimpleString("QUEUED", out);
            return;
        }

        String key = command.get(1);
        String value = dataStore.getValue(key);

        //System.out.println("Replica? " + serverConfig.isReplica() + " value: " + value);
        writeBulkString(value, out);
    }

    public void handleLPush(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 3) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'LPUSH' command", out);
            return;
        }

        String key = command.get(1);
        List<String> list = dataStore.getList(key);
        if (list == null) list = new ArrayList<>();

        for (int i = 2; i < command.size(); i++) {
            list.add(0, command.get(i));
        }

        dataStore.setList(key, list);
        writeInteger(list.size(), out);

        blockingManager.notifyBlockedClients(key);
    }

    public void handleRPush(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 3) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'RPUSH' command", out);
            return;
        }

        String key = command.get(1);
        List<String> list = dataStore.getList(key);
        if (list == null) list = new ArrayList<>();

        for (int i = 2; i < command.size(); i++) {
            list.add(command.get(i));
        }

        dataStore.setList(key, list);
        writeInteger(list.size(), out);

        blockingManager.notifyBlockedClients(key);
    }

    public void handleLRange(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 4) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'LRANGE' command", out);
            return;
        }

        String key = command.get(1);
        int start_index = Integer.parseInt(command.get(2));
        int end_index = Integer.parseInt(command.get(3));

        List<String> list = dataStore.getList(key);

        if (list == null || list.isEmpty()) {
            writeArray(0, out);
            return;
        }

        if (start_index < 0) start_index = Math.max(list.size() + start_index, 0);
        if (end_index >= list.size()) end_index = list.size() - 1;
        else if (end_index < 0) end_index = Math.max(list.size() + end_index, 0);

        if (start_index >= list.size() || start_index > end_index) {
            writeArray(0, out);
            return;
        }

        int responseArrayLength = end_index - start_index + 1;
        writeArray(responseArrayLength, out);

        for (int i = start_index; i <= end_index; i++) {
            writeBulkString(list.get(i), out);
        }
    }

    public void handleLLen(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'LLEN' command", out);
            return;
        }

        String key = command.get(1);
        List<String> list = dataStore.getList(key);

        int size = (list == null || list.isEmpty()) ? 0 : list.size();
        writeInteger(size, out);
    }

    public void handleLPop(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'LPOP' command", out);
            return;
        }

        String key = command.get(1);
        List<String> list = dataStore.getList(key);

        if (list == null || list.isEmpty()) {
            if (command.size() == 2) {
                writeNullBulkString(out);
            } else {
                writeArray(0, out);
            }
            return;
        }

        int limit = 1;
        if (command.size() > 2) limit = Integer.parseInt(command.get(2));
        if (limit > list.size()) limit = list.size();

        if (limit > 1) {
            writeArray(limit, out);
        }

        for (int i = 0; i < limit; i++) {
            String poppedString = list.remove(0);
            writeBulkString(poppedString, out);
        }
    }

    public void handleBLPop(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 3) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'BLPOP' command", out);
            return;
        }

        String key = command.get(1);
        double timeOut = Double.parseDouble(command.get(2));

        List<String> list = dataStore.getList(key);

        // If the list is not empty, pop the element
        if (list != null && !list.isEmpty()) {
            String poppedElement = list.remove(0);

            // return array - [key, value]
            writeArray(2, out);
            writeBulkString(key, out);
            writeBulkString(poppedElement, out);

            return;
        }

        // If the list is empty, the command blocks until timeout reached or an element is pushed
        blockingManager.addBlockedClient(key, timeOut, out);
    }

    public void handleXAdd(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 5) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'XADD' command", out);
            return;
        }

        if ((command.size() - 3) % 2 != 0) {
            writeError("ERR wrong number of arguments for XADD", out);
            return;
        }

        String streamKey = command.get(1);
        String entryId = command.get(2);

        RedisStream stream = dataStore.getStream(streamKey);
        if (stream == null) stream = new RedisStream();

        Map<String, String> fields = new HashMap<>();
        for (int i = 3; i < command.size(); i += 2) {
            String key = command.get(i);
            String value = command.get(i + 1);
            fields.put(key, value);
        }

        StreamEntry newEntry;
        try {
            if ("*".equals(entryId) || entryId.endsWith("-*"))
                newEntry = StreamEntry.createWithAutoSequence(entryId, fields, stream);
            else newEntry = new StreamEntry(entryId, fields);
        } catch (IllegalArgumentException e) {
            writeError("ERR Invalid stream ID specified as stream command argument", out);
            return;
        }

        if (!newEntry.isIdGreaterThanZero()) {
            writeError("ERR The ID specified in XADD must be greater than 0-0", out);
        }

        if (!"*".equals(entryId) && !entryId.endsWith("-*")) {
            StreamEntry lastEntry = stream.getLastEntry();
            if (lastEntry != null && !newEntry.isIdGreaterThan(lastEntry)) {
                writeError("ERR The ID specified in XADD is equal or smaller than the target stream top item", out);
                return;
            }
        }

        stream.addEntry(newEntry);
        dataStore.setStream(streamKey, stream);

        blockingManager.notifyBlockedStreamClients(streamKey);
        writeBulkString(newEntry.getId(), out);
    }

    public void handleXRange(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 4) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'XRANGE' command", out);
            return;
        }

        String streamKey = command.get(1);
        String startId = command.get(2);
        String endId = command.get(3);

        RedisStream stream = dataStore.getStream(streamKey);
        if (stream == null) {
            writeArray(0, out);
            return;
        }

        List<StreamEntry> entries = stream.getEntriesInRange(startId, endId, false);
        writeArray(entries.size(), out);

        for (StreamEntry entry : entries) {
            writeArray(2, out); // id, list of pairs
            writeBulkString(entry.getId(), out);

            Map<String, String> fields = entry.getFields();
            writeArray(2 * fields.size(), out); // key-value

            for (Map.Entry<String, String> it : fields.entrySet()) {
                writeBulkString(it.getKey(), out);
                writeBulkString(it.getValue(), out);
            }
        }
    }

    public void handleXRead(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 4) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'XREAD' command", out);
            return;
        }

        int currentIndex = 1;
        double blockTimeout = -1;

        if ("BLOCK".equalsIgnoreCase(command.get(currentIndex))) {
            blockTimeout = Double.parseDouble(command.get(currentIndex + 1));
            currentIndex += 2;
        }

        if (!"STREAMS".equalsIgnoreCase(command.get(currentIndex))) {
            writeError("ERR Syntax error in XREAD command", out);
            return;
        }

        currentIndex++;

        List<String> streamKeys = new ArrayList<>();
        List<String> startIds = new ArrayList<>();

        while (currentIndex < command.size() && !command.get(currentIndex).contains("-") && !"$".equals(command.get(currentIndex)))
            streamKeys.add(command.get(currentIndex++));
        while (currentIndex < command.size()) startIds.add(command.get(currentIndex++));

        if (streamKeys.size() != startIds.size()) {
            writeError("ERR Unbalanced XREAD streams and IDs count", out);
            return;
        }

        List<StreamReadResult> readResults = new ArrayList<>();
        boolean hasData = false;

        for (int i = 0; i < streamKeys.size(); i++) {
            String streamKey = streamKeys.get(i);
            String startId = startIds.get(i);

            RedisStream stream = dataStore.getStream(streamKey);
            if (stream == null) {
                writeArray(0, out);
                continue;
            }

            if ("$".equals(startId)) {
                if (!stream.isEmpty()) startId = stream.getLastEntry().getId();
                else startId = "0-0";
                startIds.set(i, startId);
            }

            List<StreamEntry> entries = stream.getEntriesInRange(startId, "+", true);
            if (!entries.isEmpty()) {
                readResults.add(new StreamReadResult(streamKey, entries));
                hasData = true;
            }
        }

        if (hasData || blockTimeout == -1) {
            RespProtocol.writeXReadResults(readResults, out);
            return;
        }

        blockingManager.addBlockedStreamClient(streamKeys, startIds, blockTimeout, out);
    }

    public void handleIncr(String clientId, List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'INCR' command", out);
            return;
        }

        if (dataStore.isMultiEnabled(clientId)) {
            dataStore.putCommandInQueue(clientId, command, out);
            writeSimpleString("QUEUED", out);
            return;
        }

        String key = command.get(1);
        String value = dataStore.getValue(key);

        if (value != null) {
            try {
                int incrValue = Integer.parseInt(value) + 1;
                dataStore.setValue(key, String.valueOf(incrValue));
                writeInteger(incrValue, out);
            } catch (NumberFormatException e) {
                writeError("ERR value is not an integer or out of range", out);
            }
        } else {
            dataStore.setValue(key, "1");
            writeInteger(1, out);
        }
    }

    public void handleMulti(String clientId, List<String> command, OutputStream out) throws IOException {
        if (command.isEmpty()) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'MULTI' command", out);
            return;
        }
        dataStore.enableMulti(clientId);
        writeSimpleString("OK", out);
    }

    public void handleExec(String clientId, List<String> command, OutputStream out) throws IOException {
        if (command.isEmpty()) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'EXEC' command", out);
            return;
        }

        if (!dataStore.isMultiEnabled(clientId)) {
            writeError("ERR EXEC without MULTI", out);
            return;
        }

        dataStore.disableMulti(clientId);

        if (!dataStore.hasQueuedCommand(clientId)) {
            writeArray(0, out);
            return;
        }

        writeArray(dataStore.getQueuedCommandSize(clientId), out);
        while (dataStore.hasQueuedCommand(clientId)) {
            QueuedCommand queuedCommand = dataStore.pollQueuedCommand(clientId);
            List<String> commandArray = queuedCommand.getCommand();
            OutputStream commandOutPutStream = queuedCommand.getOutputStream();

            if (RedisConstants.SET.equals(commandArray.get(0)))
                handleSet(clientId, commandArray, commandOutPutStream);
            if (RedisConstants.GET.equals(commandArray.get(0)))
                handleGet(clientId, commandArray, commandOutPutStream);
            if (RedisConstants.INCR.equals(commandArray.get(0)))
                handleIncr(clientId, commandArray, commandOutPutStream);
        }
    }

    public void handleDiscard(String clientId, List<String> command, OutputStream out) throws IOException {
        if (command.isEmpty()) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'DISCARD' command", out);
            return;
        }

        if (!dataStore.hasQueuedCommand(clientId)) {
            writeError("ERR DISCARD without MULTI", out);
            return;
        }

        dataStore.discardQueuedCommands(clientId);
        writeSimpleString("OK", out);
    }

    public void handleInfo(String clientId, List<String> command, OutputStream out) throws IOException {
        if (command.isEmpty()) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'INFO' command", out);
            return;
        }

        if (serverConfig.isReplica()) writeBulkString("role:slave", out);
        else {
            writeBulkString("role:master\r\nmaster_repl_offset:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", out);
        }
    }

    public void handleReplconf(String clientId, List<String> command, OutputStream out) throws IOException {
        if (command.size() < 3) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'REPLCONF' command", out);
            return;
        }

        String arg1 = command.get(1);
        String arg2 = command.get(2);

        System.out.println("Handling REPLCONF command: " + arg1 + " " + arg2);

        switch (arg1) {
            case RedisConstants.LISTENING_PORT:
                serverConfig.addReplica(out, Integer.parseInt(arg2));
                writeSimpleString("OK", out);
                break;

            case RedisConstants.CAPABILITIES:
                writeSimpleString("OK", out);
                break;

            case RedisConstants.GETACK:
                String offset = String.valueOf(serverConfig.getReplicaOffset());

                writeArray(3, out);
                writeBulkString("REPLCONF", out);
                writeBulkString("ACK", out);
                writeBulkString(offset, out);

                serverConfig.setReplicaOffset(serverConfig.getReplicaOffset() + RedisConstants.GETACK_COMMAND_BYTE_SIZE);
                System.out.println("getack: " + command + " replica offset: " + serverConfig.getReplicaOffset());
                break;

            case RedisConstants.ACK:
                System.out.println("Received ACK from replica");
                int receivedReplicaOffset = Integer.parseInt(arg2);

                if (receivedReplicaOffset >= serverConfig.getMasterOffset()) {
                    serverConfig.setUpToDateReplicas(serverConfig.getUpToDateReplicas() + 1);
                }

                System.out.println("Replica acknowledged offset: " + receivedReplicaOffset +
                        ", Master offset: " + serverConfig.getMasterOffset());
                break;
        }

    }

    public void handlePsync(String clientId, List<String> command, OutputStream out) throws IOException {
        if (command.size() < 3) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'PSYNC' command", out);
            return;
        }

        String replId = command.get(1);
        String psyncOffset = command.get(2);

        if ("?".equals(replId)) {
            writeSimpleString("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0", out);

            byte[] rdbFileBytes = Base64.getDecoder().decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==");

            String header = "$" + rdbFileBytes.length + "\r\n";
            out.write(header.getBytes());
            out.write(rdbFileBytes);
        }
    }

    public void handleWait(String clientId, List<String> command, OutputStream out) throws IOException, InterruptedException {
        if (command.size() < 3) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'WAIT' command", out);
            return;
        }

        if (!serverConfig.hasReplicas()) {
            writeInteger(0, out);
            return;
        }

        if(serverConfig.isFresh()) {
            writeInteger(serverConfig.getReplicaCount(), out);
            return;
        }

        serverConfig.setUpToDateReplicas(0);
        serverConfig.getAck();

        int minimumUpToDateReplica = Integer.parseInt(command.get(1));
        long duration = Long.parseLong(command.get(2));

        long startTime = System.currentTimeMillis();
        long endTime = startTime + duration;

        while (System.currentTimeMillis() < endTime) {
            if (serverConfig.getUpToDateReplicas() >= minimumUpToDateReplica) {
                System.out.println("wait res sent: " + serverConfig.getUpToDateReplicas());
                writeInteger(serverConfig.getUpToDateReplicas(), out);
                return;
            }
            Thread.sleep(1);
        }

        writeInteger(serverConfig.getUpToDateReplicas(), out);
    }

    public void handleZadd(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 4) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'ZADD' command", out);
            return;
        }

        String zsetKey = command.get(1);
        double score = Double.parseDouble(command.get(2));
        String zsetMember = command.get(3);

        int res = dataStore.addZsetMember(zsetKey, new SortedSetMember(zsetMember, score));
        writeInteger(res, out);
    }

    public void handleZrank(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 3) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'ZRANK' command", out);
            return;
        }

        String zsetKey = command.get(1);
        String zsetMember = command.get(2);

        int rank = dataStore.getZsetMemberRank(zsetKey, zsetMember);

        if(rank == -1) {
            writeNullBulkString(out);
            return;
        }

        writeInteger(rank, out);
    }

    public void handleZrange(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 4) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'ZRANGE' command", out);
            return;
        }

        String zsetKey = command.get(1);
        int startIndex = Integer.parseInt(command.get(2));
        int endIndex = Integer.parseInt(command.get(3));

        List<String> members = dataStore.getZsetMembers(zsetKey, startIndex, endIndex);

        writeArray(members.size(), out);
        for(String s: members) writeBulkString(s, out);
    }

    public void handleZcard(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'ZCARD' command", out);
            return;
        }

        String zsetKey = command.get(1);
        int cardinality = dataStore.getZsetMemberCount(zsetKey);

        writeInteger(cardinality, out);
    }

    public void handleZscore(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 3) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'ZSCORE' command", out);
            return;
        }

        String zsetKey = command.get(1);
        String member = command.get(2);
        double score = dataStore.getZsetMemberScore(zsetKey, member);

        writeBulkString(String.valueOf(score), out);
    }

    public void handleZrem(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 3) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'ZREM' command", out);
            return;
        }

        String zsetKey = command.get(1);
        String member = command.get(2);
        int res = dataStore.removeZsetMember(zsetKey, member);

        writeInteger(res, out);
    }

    public void handleSubscribe(String clientId, List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'SUBSCRIBE' command", out);
            return;
        }

        if(!dataStore.isClientSubscribed(clientId)) dataStore.subscribeClient(clientId);

        String channel = command.get(1);
        dataStore.addChannel(channel);
        int count = dataStore.getSubCount(channel);

        writeArray(3, out);
        writeBulkString(RedisConstants.SUBSCRIBE.toLowerCase(), out);
        writeBulkString(channel, out);
        writeInteger(count, out);
    }
}
