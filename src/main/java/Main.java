import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
    private static final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Long> expiry = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        int port = 6379;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

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
                out.write("+PONG\r\n".getBytes());
                break;
            case "ECHO":
                handleEcho(command, out);
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
        String response = "$" + arg.length() + "\r\n" + arg + "\r\n";
        out.write(response.getBytes());
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

        out.write("+OK\r\n".getBytes());
    }

    public static void handleGet(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 2) {
            out.write("-ERR wrong number of arguments for 'GET' command\r\n".getBytes());
            return;
        }

        String key = command.get(1);

        if (isExpired(key)) {
            cleanupExpiredKey(key);
            out.write("$-1\r\n".getBytes());
            return;
        }

        String value = store.get(key);
        if (value != null) {
            String response = "$" + value.length() + "\r\n" + value + "\r\n";
            out.write(response.getBytes());
        } else {
            out.write("$-1\r\n".getBytes());
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

    public static void handleRPush(List<String> command, OutputStream out) throws IOException {
        if (command.size() < 3) {
            out.write("-ERR wrong number of arguments for 'RPUSH' command\r\n".getBytes());
        }

        String key = command.get(1);
        String element = command.get(2);

        if (!lists.containsKey(key)) lists.put(key, new ArrayList<>());
        List<String> list = lists.get(key);
        list.add(element);
        lists.put(key, list);

        String response = ":" + list.size() + "\r\n";
        out.write(response.getBytes());
    }
}
