package com.redis.server;

import com.redis.server.blocking.BlockingOperationsManager;
import com.redis.server.client.ClientHandler;
import com.redis.server.command.CommandProcessor;
import com.redis.server.model.ServerConfig;
import com.redis.server.replication.ReplicaConnectionManager;
import com.redis.server.storage.DataStore;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class RedisServer {
    private final ServerConfig serverConfig;
    private final DataStore dataStore;
    private final BlockingOperationsManager blockingManager;
    private final CommandProcessor commandProcessor;
    private ReplicaConnectionManager replicaManager;

    public RedisServer(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
        this.dataStore = new DataStore();
        this.blockingManager = new BlockingOperationsManager(dataStore);
        this.commandProcessor = new CommandProcessor(serverConfig, dataStore, blockingManager);

        if(serverConfig.isReplica()) replicaManager = new ReplicaConnectionManager(serverConfig);
    }

    public void start() {
        startTimeoutChecker();

        if(serverConfig.isReplica()) connectToMasterAsync();

        try (ServerSocket serverSocket = new ServerSocket(serverConfig.getPort())) {
            serverSocket.setReuseAddress(true);
            System.out.println("Redis server started on port " + serverConfig.getPort());

            if (serverConfig.isReplica()) {
                System.out.println("Running as replica of " + serverConfig.getMasterHost() + ":" + serverConfig.getMasterPort());
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
