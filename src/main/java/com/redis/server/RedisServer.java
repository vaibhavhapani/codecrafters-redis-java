package com.redis.server;

import com.redis.server.blocking.BlockingOperationsManager;
import com.redis.server.client.ClientHandler;
import com.redis.server.command.CommandProcessor;
import com.redis.server.replication.ReplicaConnectionManager;
import com.redis.server.storage.DataStore;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class RedisServer {
    private final int port;
    private final boolean isReplica;
    private final String masterHost;
    private final int masterPort;
    private final DataStore dataStore;
    private final BlockingOperationsManager blockingManager;
    private final CommandProcessor commandProcessor;
    private ReplicaConnectionManager replicaManager;

    public RedisServer(int port, String masterHost, int masterPort, boolean isReplica) {
        this.port = port;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.isReplica = isReplica;
        this.dataStore = new DataStore();
        this.blockingManager = new BlockingOperationsManager(dataStore);
        this.commandProcessor = new CommandProcessor(isReplica, masterHost, masterPort, dataStore, blockingManager);

        if(isReplica) replicaManager = new ReplicaConnectionManager(masterHost, masterPort);
    }

    public void start() {
        startTimeoutChecker();

        if(isReplica) connectToMasterAsync();

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.out.println("Redis server started on port " + port);

            if (isReplica) {
                System.out.println("Running as replica of " + masterHost + ":" + masterPort);
            } else {
                System.out.println("Running as master");
            }

            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    new Thread(new ClientHandler(clientSocket, commandProcessor)).start();
                } catch (IOException e) {
                    System.err.println("Error accepting client connection: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
    }

    private void startTimeoutChecker() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(RedisConstants.TIMEOUT_CHECK_INTERVAL);
                    blockingManager.checkTimedOutClients();
                } catch (Exception e) {
                    System.err.println("Timeout checker error: " + e.getMessage());
                }
            }
        }).start();
    }

    private void connectToMasterAsync() {
        new Thread(() -> {
            try {
                Thread.sleep(1000);
                replicaManager.connectToMaster();
            } catch (Exception e) {
                System.err.println("Failed to connect to master: " + e.getMessage());
            }
        }).start();
    }
}
