package com.redis.server.replication;

import com.redis.server.RedisConstants;
import com.redis.server.command.CommandProcessor;
import com.redis.server.model.ServerConfig;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class ReplicaConnectionManager {
    private final ServerConfig serverConfig;
    private final CommandProcessor commandProcessor;
    private Socket masterSocket;
    private OutputStream masterOutput;
    private BufferedReader masterInput;
    private InputStream masterInputStream;

    public ReplicaConnectionManager(ServerConfig serverConfig, CommandProcessor commandProcessor) {
        this.serverConfig = serverConfig;
        this.commandProcessor = commandProcessor;
    }

    public void connectToMaster() throws IOException {
        System.out.println("Connecting to master at " + serverConfig.getMasterHost() + ":" + serverConfig.getMasterPort());

        try {
            // Connect to master
            masterSocket = new Socket(serverConfig.getMasterHost(), serverConfig.getMasterPort());
            masterOutput = masterSocket.getOutputStream();
            masterInputStream = masterSocket.getInputStream();
            masterInput = new BufferedReader(new InputStreamReader(masterInputStream));

            System.out.println("Connected to master successfully");

            // Start handshake process
            performHandshake();

            // Start listening for propagated commands
            startCommandListener();

        } catch (IOException e) {
            System.err.println("Failed to connect to master: " + e.getMessage());
            throw e;
        }
    }

    private void performHandshake() throws IOException {
        // Step 1: Send PING to master
        sendPingToMaster();

        // Step 2: Send REPLCONF
        sendReplconfListeningPort();
        sendReplconfCapabilities();

        // Step 3: Send PSYNC
        sendPsync();
    }

    private void sendPingToMaster() throws IOException {
        System.out.println("Sending PING to master");

        String pingCommand = "*1\r\n$4\r\nPING\r\n";
        masterOutput.write(pingCommand.getBytes());
        masterOutput.flush();

        System.out.println("PING sent to master");

        try {
            String response = masterInput.readLine();
            if (response != null) {
                System.out.println("Received response from master: " + response);

                if ("+PONG".equals(response)) {
                    System.out.println("PING handshake successful");
                } else {
                    System.err.println("Unexpected PING response: " + response);
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading PING response: " + e.getMessage());
            throw e;
        }
    }

    private void sendReplconfListeningPort() throws IOException {
        System.out.println("Sending REPLCONF listening-port " + serverConfig.getPort());

        String portStr = String.valueOf(serverConfig.getPort());
        String command = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + portStr.length() + "\r\n" + serverConfig.getPort() + "\r\n";
        masterOutput.write(command.getBytes());

        System.out.println("REPLCONF listening-port sent: " + command.replace("\r\n", "\\r\\n"));

        String response = masterInput.readLine();
        if (response != null) {
            System.out.println("Received REPLCONF listening-port response: " + response);
            if (!"+OK".equals(response)) {
                System.err.println("Unexpected REPLCONF listening-port response: " + response);
            }
        }
    }

    private void sendReplconfCapabilities() throws IOException {
        System.out.println("Sending REPLCONF capa psync2");

        String command = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        masterOutput.write(command.getBytes());

        System.out.println("REPLCONF capa sent: " + command.replace("\r\n", "\\r\\n"));

        String response = masterInput.readLine();
        if (response != null) {
            System.out.println("Received REPLCONF capa response: " + response);
            if (!"+OK".equals(response)) {
                System.err.println("Unexpected REPLCONF capa response: " + response);
            }
        }
    }

    private void sendPsync() throws IOException {
        System.out.println("Sending PSYNC");

        String command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        masterOutput.write(command.getBytes());

        System.out.println("PSYNC sent: " + command.replace("\r\n", "\\r\\n"));

        String response = masterInput.readLine();
        if (response != null) {
            System.out.println("Received PSYNC response: " + response);

            if (response.startsWith("+FULLRESYNC")) {
                System.out.println("Full resync initiated, reading RDB file...");
                skipRDBFile();
            }
        }
    }

    private void skipRDBFile() throws IOException {
        String lengthLine = masterInput.readLine();

        if (lengthLine != null && lengthLine.startsWith("$")) {
            int rdbLength = Integer.parseInt(lengthLine.substring(1));
            System.out.println("RDB file length: " + rdbLength + " bytes");

            // Read and discard the RDB file bytes
            byte[] rdbData = new byte[rdbLength];
            int totalRead = 0;
            while (totalRead < rdbLength) {
                int bytesRead = masterInputStream.read(rdbData, totalRead, rdbLength - totalRead);
                if (bytesRead == -1) {
                    throw new IOException("Unexpected end of stream while reading RDB file");
                }
                totalRead += bytesRead;
            }
            System.out.println("RDB file read and discarded (" + totalRead + " bytes)");
        }
    }

    private void startCommandListener() {
        System.out.println("Starting command listener for propagated commands...");

        new Thread(() -> {
            try {
                while (!masterSocket.isClosed()) {
                    try {
                        // Parse incoming RESP command from master
                        List<String> command = parseRespArray(masterInput.readLine(), masterInput);

                        if (command != null && !command.isEmpty()) {
                            System.out.println("Received propagated command: " + command);

                            OutputStream dummyOut = new OutputStream() {
                                @Override
                                public void write(int b) throws IOException {
                                    // Discard output - replicas don't respond to propagated commands
                                }
                            };

                            String replicationClientId = "replication-" + System.currentTimeMillis();
                            commandProcessor.processCommand(replicationClientId, command, dummyOut);
                        }
                    } catch (IOException e) {
                        System.err.println("Error reading propagated command: " + e.getMessage());
                        break;
                    }
                }
            } catch (Exception e) {
                System.err.println("Command listener error: " + e.getMessage());
                e.printStackTrace();
            }
        }).start();
    }

    private List<String> parseRespArray(String arrayLine, BufferedReader in) throws IOException {
        System.out.println("parse====== " + arrayLine.startsWith("*"));
        int arrayLength = Integer.parseInt(arrayLine.substring(1));
        List<String> command = new ArrayList<>();

        for (int i = 0; i < arrayLength; i++) {
            String lengthLine = in.readLine();
            if (lengthLine != null && lengthLine.startsWith(RedisConstants.BULK_STRING_PREFIX)) {
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
}

