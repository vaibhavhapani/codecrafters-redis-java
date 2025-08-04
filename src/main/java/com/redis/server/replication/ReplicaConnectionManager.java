package com.redis.server.replication;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

public class ReplicaConnectionManager {
    private final String masterHost;
    private final int masterPort;
    private final int replicaPort;
    private Socket masterSocket;
    private OutputStream masterOutput;
    private BufferedReader masterInput;

    public ReplicaConnectionManager(String masterHost, int masterPort, int replicaPort){
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.replicaPort = replicaPort;
    }

    public void connectToMaster() throws IOException {
        System.out.println("Connecting to master at " + masterHost + ":" + masterPort);

        try {
            // Connect to master
            masterSocket = new Socket(masterHost, masterPort);
            masterOutput = masterSocket.getOutputStream();
            masterInput = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));

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
        System.out.println("Sending REPLCONF listening-port " + replicaPort);

        String portStr = String.valueOf(replicaPort);
        String command = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + portStr.length() + "\r\n" + replicaPort + "\r\n";
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
        }
    }
}
