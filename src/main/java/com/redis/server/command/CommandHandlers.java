package com.redis.server.command;

import com.redis.server.Main;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static com.redis.server.protocol.RespProtocol.*;


public class CommandHandlers {
    public static void handleEcho(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            out.write("-ERR wrong number of arguments for 'ECHO' command\r\n".getBytes());
            return;
        }
        String arg = command.get(1);
        writeBulkString(arg, out);
    }
//
//    public static void handleType(List<String> command, OutputStream out) throws IOException {
//        if (command.size() < 2) {
//            out.write("-ERR wrong number of arguments for 'TYPE' command\r\n".getBytes());
//            return;
//        }
//        String key = command.get(1);
//
//        if (entries.containsKey(key)) {
//            writeSimpleString("stream", out);
//            return;
//        }
//
//        if (store.containsKey(key)) {
//            writeSimpleString("string", out);
//            return;
//        }
//        writeSimpleString("none", out);
//    }
//
//    public static void handleSet(List<String> command, OutputStream out) throws IOException {
//        if (command.size() < 3) {
//            out.write("-ERR wrong number of arguments for 'SET' command\r\n".getBytes());
//            return;
//        }
//
//        String key = command.get(1);
//        String value = command.get(2);
//
//        if (command.size() >= 5 && "PX".equalsIgnoreCase(command.get(3))) {
//            long expiryMs = Long.parseLong(command.get(4));
//            long expiryTime = System.currentTimeMillis() + expiryMs;
//            store.put(key, value);
//            expiry.put(key, expiryTime);
//        } else {
//            store.put(key, value);
//            expiry.remove(key); // Remove any existing expiry
//        }
//
//        writeSimpleString("OK", out);
//    }
//
//    public static void handleGet(List<String> command, OutputStream out) throws IOException {
//        if (command.size() < 2) {
//            out.write("-ERR wrong number of arguments for 'GET' command\r\n".getBytes());
//            return;
//        }
//
//        String key = command.get(1);
//
//        if (isExpired(key)) {
//            cleanupExpiredKey(key);
//            writeNullBulkString(out);
//            return;
//        }
//
//        String value = store.get(key);
//        if (value != null) {
//            writeBulkString(value, out);
//        } else {
//            writeNullBulkString(out);
//        }
//    }
//
//    private static boolean isExpired(String key) {
//        Long expiryTime = expiry.get(key);
//        return expiryTime != null && System.currentTimeMillis() > expiryTime;
//    }
//
//    private static void cleanupExpiredKey(String key) {
//        store.remove(key);
//        expiry.remove(key);
//    }
//
//    public static void handleLPush(List<String> command, OutputStream out) throws IOException {
//        if (command.size() < 3) {
//            out.write("-ERR wrong number of arguments for 'LPUSH' command\r\n".getBytes());
//            return;
//        }
//
//        String key = command.get(1);
//        if (!lists.containsKey(key)) lists.put(key, new ArrayList<>());
//        List<String> list = lists.get(key);
//
//        int elements = command.size();
//        for (int i = 2; i < elements; i++) {
//            String element = command.get(i);
//            list.add(0, element);
//            lists.put(key, list);
//        }
//
//        writeInteger(list.size(), out);
//        notifyBlockedClients(key);
//    }
//
//    public static void handleRPush(List<String> command, OutputStream out) throws IOException {
//        if (command.size() < 3) {
//            out.write("-ERR wrong number of arguments for 'RPUSH' command\r\n".getBytes());
//            return;
//        }
//
//        String key = command.get(1);
//        if (!lists.containsKey(key)) lists.put(key, new ArrayList<>());
//        List<String> list = lists.get(key);
//
//        int elements = command.size();
//        for (int i = 2; i < elements; i++) {
//            String element = command.get(i);
//            list.add(element);
//            lists.put(key, list);
//        }
//
//        writeSimpleString(":", String.valueOf(list.size()), out);
//        notifyBlockedClients(key);
//    }
//
//    public static void handleLRange(List<String> command, OutputStream out) throws IOException {
//        if (command.size() < 4) {
//            out.write("-ERR wrong number of arguments for 'LRANGE' command\r\n".getBytes());
//            return;
//        }
//
//        String key = command.get(1);
//        int start_index = Integer.parseInt(command.get(2));
//        int end_index = Integer.parseInt(command.get(3));
//
//        List<String> list = lists.get(key);
//
//        if (list == null || list.isEmpty()) {
//            writeArray(0, out);
//            return;
//        }
//
//        if (start_index < 0) start_index = Math.max(list.size() + start_index, 0);
//
//        if (end_index >= list.size()) end_index = list.size() - 1;
//        else if (end_index < 0) end_index = Math.max(list.size() + end_index, 0);
//
//        if (start_index >= list.size() || start_index > end_index) {
//            writeArray(0, out);
//            return;
//        }
//
//        int responseArrayLength = end_index - start_index + 1;
//        writeArray(responseArrayLength, out);
//
//        for (int i = start_index; i <= end_index; i++) {
//            String element = list.get(i);
//            writeBulkString(element, out);
//        }
//    }
//
//    public static void handleLLen(List<String> command, OutputStream out) throws IOException {
//        if (command.size() < 2) {
//            out.write("-ERR wrong number of arguments for 'ECHO' command\r\n".getBytes());
//            return;
//        }
//
//        String key = command.get(1);
//        List<String> list = lists.get(key);
//
//        if (list == null || list.isEmpty()) {
//            writeInteger(0, out);
//            return;
//        }
//
//        writeInteger(list.size(), out);
//    }
//
//    public static void handleLPop(List<String> command, OutputStream out) throws IOException {
//        if (command.size() < 2) {
//            out.write("-ERR wrong number of arguments for 'LPOP' command\r\n".getBytes());
//            return;
//        }
//
//        String key = command.get(1);
//        List<String> list = lists.get(key);
//
//        if (list == null || list.isEmpty()) {
//            if (command.size() == 2) {
//                writeNullBulkString(out);
//            } else {
//                writeArray(0, out);
//            }
//            return;
//        }
//
//        int limit = 1;
//        if (command.size() > 2) limit = Integer.parseInt(command.get(2));
//        if (limit > list.size()) limit = list.size();
//
//        if (limit > 1) {
//            writeArray(limit, out);
//        }
//
//        for (int i = 0; i < limit; i++) {
//            String poppedString = list.remove(0);
//            writeBulkString(poppedString, out);
//        }
//    }
//
//    public static void handleBLPop(List<String> command, OutputStream out) throws IOException {
//        if (command.size() < 3) {
//            out.write("-ERR wrong number of arguments for 'BLPOP' command\r\n".getBytes());
//            return;
//        }
//
//        String key = command.get(1);
//        double timeOut = Double.parseDouble(command.get(2));
//
//        List<String> list = lists.get(key);
//
//        // If the list is not empty, pop the element
//        if (list != null && !list.isEmpty()) {
//            String poppedElement = list.remove(0);
//
//            // return array - [key, value]
//            writeArray(2, out);
//            writeBulkString(key, out);
//            writeBulkString(poppedElement, out);
//
//            return;
//        }
//
//        // If the list is empty, the command blocks until timeout reached or an element is pushed
//
//        blockedClients.offer(new Main.BlockedClient(key, timeOut, out));
//    }
//
//    public static void handleXAdd(List<String> command, OutputStream out) throws IOException {
//        if (command.size() < 5) {
//            out.write("-ERR wrong number of arguments for 'XADD' command\r\n".getBytes());
//            return;
//        }
//        String streamKey = command.get(1);
//        if (!entries.containsKey(streamKey)) entries.put(streamKey, new String[]{});
//
//        String keyId = command.get(2);
//        writeBulkString(keyId, out);
//    }
}
