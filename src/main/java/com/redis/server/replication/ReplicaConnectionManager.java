package com.redis.server.replication;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

public class ReplicaConnectionManager {
    private final String masterHost;
    private final int masterPort;
    private Socket masterSocket;
    private OutputStream masterOutput;
    private BufferedReader masterInput;

    public ReplicaConnectionManager(String masterHost, int masterPort){
        this.masterHost = masterHost;
        this.masterPort = masterPort;
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
    }

    private void sendPingToMaster() throws IOException {
        System.out.println("Sending PING to master");

        String pingCommand = "*1\r\n$4\r\nPING\r\n";
        masterOutput.write(pingCommand.getBytes());
        masterOutput.flush();

        System.out.println("PING sent to master");
    }
}
