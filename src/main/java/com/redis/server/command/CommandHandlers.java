package com.redis.server.command;

import com.redis.server.Main;
import com.redis.server.RedisConstants;
import com.redis.server.blocking.BlockingOperationsManager;
import com.redis.server.storage.DataStore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

        if (dataStore.hasEntryKey(key)) {
            writeSimpleString("stream", out);
            return;
        }

        if (dataStore.hasKey(key)) {
            writeSimpleString(RedisConstants.STRING_TYPE, out);
            return;
        }
        writeSimpleString(RedisConstants.NONE_TYPE, out);
    }

    public void handleSet(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 3) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'SET' command", out);
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

        writeSimpleString("OK", out);
    }

    public void handleGet(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            writeError(RedisConstants.ERR_WRONG_NUMBER_ARGS + " 'GET' command", out);
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
            out.write("-ERR wrong number of arguments for 'XADD' command\r\n".getBytes());
            return;
        }
        String streamKey = command.get(1);
        if (!dataStore.hasEntryKey(streamKey)) dataStore.setEntry(streamKey, new HashMap<>());

        String keyId = command.get(2);
        writeBulkString(keyId, out);
    }
}
