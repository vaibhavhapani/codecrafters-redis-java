package com.redis.server;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

public class Main {
    private static final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Long> expiry = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, String[]> entries = new ConcurrentHashMap<>();

    private static final PriorityBlockingQueue<BlockedClient> blockedClients = new PriorityBlockingQueue<>(5, Comparator.comparingLong(c -> c.blockTime));

    private static class BlockedClient {
        final String key;
        final OutputStream out;
        final long blockTime;
        final long timeoutTime;

        BlockedClient(String key, double timeoutSeconds, OutputStream out) {
            this.key = key;
            this.out = out;
            this.blockTime = System.currentTimeMillis();
            this.timeoutTime = timeoutSeconds == 0 ? 0 : blockTime + (long) (timeoutSeconds * 1000);
        }

        boolean isTimedOut() {
            return timeoutTime > 0 && System.currentTimeMillis() > timeoutTime;
        }
    }

    public static void main(String[] args) {
        int port = 6379;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(50); // check every 50ms
                        checkTimedOutClients();
                    } catch (Exception e) {
                        System.err.println("Timeout checker error: " + e.getMessage());
                    }
                }
            }).start();

            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    new Thread(() -> {
                        try {
                            handleClient(clientSocket);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).start();
                } catch (IOException e) {
                    throw new RuntimeException(e);

                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static void handleClient(Socket clientSocket) throws IOException {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             OutputStream out = clientSocket.getOutputStream()) {

            String line;
            while ((line = in.readLine()) != null) {
                if (line.startsWith("*")) {
                    List<String> command = parseRespArray(line, in);
                    if (!command.isEmpty()) {
                        processCommand(command, out);
                        out.flush();
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error handling client: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }

    private static List<String> parseRespArray(String arrayLine, BufferedReader in) throws IOException {
        int arrayLength = Integer.parseInt(arrayLine.substring(1));
        List<String> command = new ArrayList<>();

        for (int i = 0; i < arrayLength; i++) {
            String lengthLine = in.readLine();

            if (lengthLine != null && lengthLine.startsWith("$")) {
                int commandLength = Integer.parseInt(lengthLine.substring(1));
                if (commandLength >= 0) {
                    String element = in.readLine();
                    if (element != null) {
                        command.add(element);
                    }
                }
            }
        }
        return command;
    }

    private static void processCommand(List<String> command, OutputStream out) throws IOException {
        String commandName = command.get(0).toUpperCase();

        switch (commandName) {
            case "PING":
                writeSimpleString("+", "PONG", out);
                break;
            case "ECHO":
                handleEcho(command, out);
                break;
            case "TYPE":
                handleType(command, out);
                break;
            case "SET":
                handleSet(command, out);
                break;
            case "GET":
                handleGet(command, out);
                break;
            case "RPUSH":
                handleRPush(command, out);
                break;
            case "LPUSH":
                handleLPush(command, out);
                break;
            case "LRANGE":
                handleLRange(command, out);
                break;
            case "LLEN":
                handleLLen(command, out);
                break;
            case "LPOP":
                handleLPop(command, out);
                break;
            case "BLPOP":
                handleBLPop(command, out);
                break;
            case "XADD":
                handleXAdd(command, out);
                break;
            default:
                out.write(("-ERR unknown command '" + commandName + "'\r\n").getBytes());
                break;
        }
    }

    public static void handleEcho(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            out.write("-ERR wrong number of arguments for 'ECHO' command\r\n".getBytes());
            return;
        }
        String arg = command.get(1);
        writeBulkString(arg, out);
    }

    public static void handleType(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            out.write("-ERR wrong number of arguments for 'TYPE' command\r\n".getBytes());
            return;
        }
        String key = command.get(1);

        if(entries.containsKey(key)){
            writeSimpleString("+", "stream", out);
            return;
        }

        if (store.containsKey(key)) {
            writeSimpleString("+", "string", out);
            return;
        }
        writeSimpleString("+", "none", out);
    }

    public static void handleSet(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 3) {
            out.write("-ERR wrong number of arguments for 'SET' command\r\n".getBytes());
            return;
        }

        String key = command.get(1);
        String value = command.get(2);

        if (command.size() >= 5 && "PX".equalsIgnoreCase(command.get(3))) {
            long expiryMs = Long.parseLong(command.get(4));
            long expiryTime = System.currentTimeMillis() + expiryMs;
            store.put(key, value);
            expiry.put(key, expiryTime);
        } else {
            store.put(key, value);
            expiry.remove(key); // Remove any existing expiry
        }

        writeSimpleString("+", "OK", out);
    }

    public static void handleGet(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            out.write("-ERR wrong number of arguments for 'GET' command\r\n".getBytes());
            return;
        }

        String key = command.get(1);

        if (isExpired(key)) {
            cleanupExpiredKey(key);
            writeNullBulkString(out);
            return;
        }

        String value = store.get(key);
        if (value != null) {
            writeBulkString(value, out);
        } else {
            writeNullBulkString(out);
        }
    }

    private static boolean isExpired(String key) {
        Long expiryTime = expiry.get(key);
        return expiryTime != null && System.currentTimeMillis() > expiryTime;
    }

    private static void cleanupExpiredKey(String key) {
        store.remove(key);
        expiry.remove(key);
    }

    public static void handleLPush(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 3) {
            out.write("-ERR wrong number of arguments for 'LPUSH' command\r\n".getBytes());
            return;
        }

        String key = command.get(1);
        if (!lists.containsKey(key)) lists.put(key, new ArrayList<>());
        List<String> list = lists.get(key);

        int elements = command.size();
        for (int i = 2; i < elements; i++) {
            String element = command.get(i);
            list.add(0, element);
            lists.put(key, list);
        }

        writeSimpleString(":", String.valueOf(list.size()), out);
        notifyBlockedClients(key);
    }

    public static void handleRPush(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 3) {
            out.write("-ERR wrong number of arguments for 'RPUSH' command\r\n".getBytes());
            return;
        }

        String key = command.get(1);
        synchronized (lists){
            if (!lists.containsKey(key)) lists.putIfAbsent(key, Collections.synchronizedList(new ArrayList<>()));
            List<String> list = lists.get(key);

            int elements = command.size();
            for (int i = 2; i < elements; i++) {
                String element = command.get(i);
                list.add(element);
                lists.put(key, list);
            }

            writeSimpleString(":", String.valueOf(list.size()), out);
        }
        notifyBlockedClients(key);
    }

    public static void handleLRange(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 4) {
            out.write("-ERR wrong number of arguments for 'LRANGE' command\r\n".getBytes());
            return;
        }

        String key = command.get(1);
        int start_index = Integer.parseInt(command.get(2));
        int end_index = Integer.parseInt(command.get(3));

        List<String> list = lists.get(key);

        if (list == null || list.isEmpty()) {
            writeSimpleString("*", "0", out);
            return;
        }

        if (start_index < 0) start_index = Math.max(list.size() + start_index, 0);

        if (end_index >= list.size()) end_index = list.size() - 1;
        else if (end_index < 0) end_index = Math.max(list.size() + end_index, 0);

        if (start_index >= list.size() || start_index > end_index) {
            writeSimpleString("*", "0", out);
            return;
        }

        String response = "*" + (end_index - start_index + 1) + "\r\n";
        out.write(response.getBytes());

        for (int i = start_index; i <= end_index; i++) {
            String element = list.get(i);
            writeBulkString(element, out);
        }
    }

    public static void handleLLen(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            out.write("-ERR wrong number of arguments for 'ECHO' command\r\n".getBytes());
            return;
        }

        String key = command.get(1);
        List<String> list = lists.get(key);

        if (list == null || list.isEmpty()) {
            writeSimpleString(":", "0", out);
            return;
        }

        writeSimpleString(":", String.valueOf(list.size()), out);
    }

    public static void handleLPop(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            out.write("-ERR wrong number of arguments for 'LPOP' command\r\n".getBytes());
            return;
        }

        String key = command.get(1);
        List<String> list = lists.get(key);

        if (list == null || list.isEmpty()) {
            if (command.size() == 2) {
                writeNullBulkString(out);
            } else {
                writeSimpleString("*", "0", out);
            }
            return;
        }

        int limit = 1;
        if (command.size() > 2) limit = Integer.parseInt(command.get(2));
        if (limit > list.size()) limit = list.size();

        if (limit > 1) {
            writeSimpleString("*", String.valueOf(limit), out);
        }

        for (int i = 0; i < limit; i++) {
            String poppedString = list.remove(0);
            writeBulkString(poppedString, out);
        }
    }

    public static void handleBLPop(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 3) {
            out.write("-ERR wrong number of arguments for 'BLPOP' command\r\n".getBytes());
            return;
        }

        String key = command.get(1);
        double timeOut = Double.parseDouble(command.get(2));

        synchronized (lists){
            System.out.println("lists: " + lists);
            List<String> list = lists.get(key);
            System.out.println("[BLPOP Handler] Key: " + key + "=========================");


            // If the list is not empty, pop the element
            if (list != null && !list.isEmpty()) {
                String poppedElement = list.remove(0);

                System.out.println("[BLPOP Handler] popped:  " + poppedElement);

                // return array - [key, value]
                writeSimpleString("*", "2", out);
                writeBulkString(key, out);
                writeBulkString(poppedElement, out);

                System.out.print("=================== out ===============");

                return;
            }
        }

        // If the list is empty, the command blocks until timeout reached or an element is pushed

        blockedClients.offer(new BlockedClient(key, timeOut, out));
    }

    private static void notifyBlockedClients(String key) throws IOException {
        synchronized (blockedClients) {
            Iterator<BlockedClient> it = blockedClients.iterator();

            while (it.hasNext()) {
                BlockedClient client = it.next();

                if (client.key.equals(key)) {
                    List<String> list = lists.get(key);

                    if (list != null && !list.isEmpty()) {
                        String poppedElement = list.remove(0);
                        System.out.println("[Notifier] popped:  " + poppedElement);

                        writeSimpleString("*", "2", client.out);
                        writeBulkString(key, client.out);
                        writeBulkString(poppedElement, client.out);

                        client.out.flush();

                        System.out.print("=================== Notifier out ===============");

                        it.remove();

                    }
                }
            }
        }
    }

    private static void checkTimedOutClients() throws IOException {
        synchronized (blockedClients) {
            Iterator<BlockedClient> it = blockedClients.iterator();
            while (it.hasNext()) {
                BlockedClient client = it.next();
                if (client.isTimedOut()) {
                    writeNullBulkString(client.out);
                    it.remove();
                }
            }
        }
    }

    public static void handleXAdd(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 5) {
            out.write("-ERR wrong number of arguments for 'XADD' command\r\n".getBytes());
            return;
        }
        String streamKey = command.get(1);
        if(!entries.containsKey(streamKey)) entries.put(streamKey, new String[]{});

        String keyId = command.get(2);
        writeBulkString(keyId, out);
    }

    private static void writeSimpleString(String firstByte, String message, OutputStream out) throws IOException {
        out.write((firstByte + message + "\r\n").getBytes());
    }

    private static void writeBulkString(String value, OutputStream out) throws IOException {
        out.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
    }

    private static void writeNullBulkString(OutputStream out) throws IOException {
        out.write("$-1\r\n".getBytes());
    }
}
