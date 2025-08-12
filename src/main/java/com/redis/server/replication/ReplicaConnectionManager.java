package com.redis.server.replication;

import com.redis.server.command.CommandProcessor;
import com.redis.server.model.ServerConfig;
import com.redis.server.protocol.RespProtocol;

import java.io.*;
import java.net.Socket;
import java.util.List;

public class ReplicaConnectionManager {
    private final ServerConfig serverConfig;
    private final CommandProcessor commandProcessor;
    private Socket masterSocket;
    private OutputStream masterOutput;
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

            System.out.println("Connected to master successfully");

            // Start handshake process
            performHandshake();

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
            String response = RespProtocol.readLineFromInputStream(masterInputStream);
            if (!response.isEmpty()) {
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

        String response = RespProtocol.readLineFromInputStream(masterInputStream);
        if (!response.isEmpty()) {
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

        String response = RespProtocol.readLineFromInputStream(masterInputStream);
        if (!response.isEmpty()) {
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

        String response = RespProtocol.readLineFromInputStream(masterInputStream);
        if (!response.isEmpty()) {
            System.out.println("Received PSYNC response: " + response);

            if (response.startsWith("+FULLRESYNC")) {
                System.out.println("Full resync initiated, reading RDB file...");
                skipRDBFile();

                System.out.println("Starting command listener for propagated commands...");
                startCommandListener();
            }
        }
    }

    private void skipRDBFile() throws IOException {
        String lengthLine = RespProtocol.readLineFromInputStream(masterInputStream);
        System.out.println("RDB length line: '" + lengthLine + "'");

        if (lengthLine.startsWith("$")) {
            int rdbLength = Integer.parseInt(lengthLine.substring(1));
            System.out.println("RDB file length: " + rdbLength + " bytes");

            // Read and discard the RDB file bytes
            byte[] rdbData = new byte[rdbLength];
            int totalRead = 0;
            while (totalRead < rdbLength) {
                int bytes = masterInputStream.read(rdbData, totalRead, rdbLength - totalRead);
                if (bytes == -1) {
                    throw new IOException("Unexpected end of stream while reading RDB file");
                }
                totalRead += bytes;
            }
            System.out.println("RDB file read and discarded (" + totalRead + " bytes)");
        }
    }

    private void startCommandListener() {
        new Thread(() -> {
            try {
                while (!masterSocket.isClosed()) {
                    try {
                        // Parse incoming RESP command from master
                        List<String> command = RespProtocol.parseRespArrayFromInputStream(masterInputStream);

                        if (command != null && !command.isEmpty()) {
                            System.out.println("Received propagated command: " + command);

                            String replicationClientId = "replication-" + System.currentTimeMillis();
                            commandProcessor.processCommand(replicationClientId, command, masterOutput);
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
}

