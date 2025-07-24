import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    private static final Map<String, String> store = new HashMap<>();
    private static final Map<String, Long> expiry = new HashMap<>();

    public static void main(String[] args) {
        int port = 6379;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            while (true) {
                try{
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
        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        OutputStream out = clientSocket.getOutputStream();

        String line;
        while((line = in.readLine()) != null){
            if(line.startsWith("*")) {
                int arrayLength = Integer.parseInt(line.substring(1));
                List<String> command = new ArrayList<>();

                for(int i = 0; i < arrayLength; i++) {
                    String newLine = in.readLine();
                    if(newLine != null && newLine.startsWith("$")){
                        int commandLength = Integer.parseInt(newLine.substring(1));
                        if(commandLength >= 0){
                            String element = in.readLine();
                            if(element != null) command.add(element);
                        }
                    }
                }

                if(!command.isEmpty()){
                    String commandName = command.get(0).toUpperCase();

                    switch (commandName) {
                        case "PING":
                            out.write("+PONG\r\n".getBytes());
                            break;
                        case "ECHO":
                            if(command.size() > 1) {
                                String arg = command.get(1);
                                String response = "$" + arg.length() + "\r\n" + arg + "\r\n";
                                out.write(response.getBytes());
                            } else {
                                out.write("-ERR wrong number of arguments for 'ECHO' command\r\n".getBytes());
                            }
                            break;
                        case "SET":
                            if(command.size() >= 3){
                                String key = command.get(1);
                                String value = command.get(2);
                                store.put(key, value);

                                if(command.size() > 3) {
                                    String expiryType = command.get(4);
                                    if(expiryType.equalsIgnoreCase("PX")) {
                                        Long expiryTime = Long.parseLong(command.get(5));
                                        expiry.put(key, expiryTime);
                                        System.out.println("Expiry set for key: " + key + " is: " + expiryTime);
                                    }
                                }
                                out.write("+OK\r\n".getBytes());
                            } else {
                                out.write("-ERR wrong number of arguments for 'SET' command\r\n".getBytes());
                            }
                            break;
                        case "GET":
                            if(command.size() > 1){
                                String key = command.get(1);
                                if(store.containsKey(key)) {
                                    System.out.println("Current time: " + System.currentTimeMillis());
                                    if(expiry.get(key) == null || expiry.get(key) >= System.currentTimeMillis()) {
                                        String value = store.get(key);
                                        String response = "$" + value.length() + "\r\n" + value + "\r\n";
                                        out.write(response.getBytes());
                                    } else {
                                        store.remove(key);
                                        expiry.remove(key);
                                        out.write("$-1\\r\\n".getBytes());
                                    }
                                }
                                else out.write("$-1\\r\\n".getBytes());
                            } else {
                                out.write("-ERR wrong number of arguments for 'GET' command\r\n".getBytes());
                            }
                            break;
                        default:
                            out.write(("-ERR unknown command '" + commandName + "'\r\n").getBytes());
                            break;
                    }
                    out.flush();
                }
            }
        }
        out.flush();
    }
}
