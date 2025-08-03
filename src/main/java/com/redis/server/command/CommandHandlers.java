package com.redis.server.command;

import com.redis.server.RedisConstants;
import com.redis.server.blocking.BlockingOperationsManager;
import com.redis.server.model.QueuedCommand;
import com.redis.server.model.RedisStream;
import com.redis.server.model.StreamEntry;
import com.redis.server.model.StreamReadResult;
import com.redis.server.protocol.RespProtocol;
import com.redis.server.storage.DataStore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.redis.server.protocol.RespProtocol.*;


public class CommandHandlers {

    private final DataStore dataStore;
    private final BlockingOperationsManager blockingManager;

    public CommandHandlers(DataStore dataStore, BlockingOperationsManager blockingManager) {
        this.dataStore = dataStore;
        this.blockingManager = blockingManager;
    }

    public void handlePing(List<String> command, OutputStream out) throws IOException {
        writeSimpleString(RedisConstants.PONG, out);
    }

    public void handleEcho(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'ECHO' command", out);
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

    public void handleSet(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 3) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'SET' command", out);
            return;
        }

        if(dataStore.isMultiEnabled()) {
            dataStore.putCommandInQueue(command, out);
            writeSimpleString("QUEUED", out);
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

        writeSimpleString(RedisConstants.OK, out);
    }

    public void handleGet(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'GET' command", out);
            return;
        }

        if(dataStore.isMultiEnabled()) {
            dataStore.putCommandInQueue(command, out);
            System.out.println("hiii");
            writeSimpleString("QUEUED", out);
            return;
        }

        String key = command.get(1);
        String value = dataStore.getValue(key);
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

            if("$".equals(startId)) {
                if(stream != null && !stream.isEmpty()) startId = stream.getLastEntry().getId();
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

    public void handleIncr(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'INCR' command", out);
            return;
        }

        if(dataStore.isMultiEnabled()) {
            dataStore.putCommandInQueue(command, out);
            writeSimpleString("QUEUED", out);
            return;
        }

        String key = command.get(1);
        String value = dataStore.getValue(key);

        if(value != null) {
            try{
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

    public void handleMulti(List<String> command, OutputStream out) throws IOException {
        if (command.isEmpty()) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'MULTI' command", out);
            return;
        }
        dataStore.enableMulti();
        writeSimpleString("OK", out);
    }

    public void handleExec(List<String> command, OutputStream out) throws IOException {
        if (command.isEmpty()) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'EXEC' command", out);
            return;
        }

        if(!dataStore.isMultiEnabled()) {
            writeError("ERR EXEC without MULTI", out);
            return;
        }

        dataStore.disableMulti();

        if(!dataStore.hasQueuedCommand()) {
            writeArray(0, out);
            return;
        }

        writeArray(dataStore.getQueuedCommandSize(), out);
        while (dataStore.hasQueuedCommand()){
            QueuedCommand queuedCommand = dataStore.pollQueuedCommand();
            List<String> commandArray = queuedCommand.getCommand();
            OutputStream commandOutPutStream = queuedCommand.getOutputStream();

            if(RedisConstants.SET.equals(commandArray.get(0))) handleSet(commandArray, commandOutPutStream);
            if(RedisConstants.GET.equals(commandArray.get(0))) handleGet(commandArray, commandOutPutStream);
            if(RedisConstants.INCR.equals(commandArray.get(0))) handleIncr(commandArray, commandOutPutStream);
        }
    }
}
