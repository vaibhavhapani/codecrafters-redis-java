package com.redis.server;

import com.redis.server.blocking.BlockingOperationsManager;
import com.redis.server.client.ClientHandler;
import com.redis.server.command.CommandProcessor;
import com.redis.server.storage.DataStore;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

public class RedisServer {
    private final int port;
    private final DataStore dataStore;
    private final BlockingOperationsManager blockingManager;
    private final CommandProcessor commandProcessor;
    private int num = 1;

    public RedisServer(int port) {
        this.port = port;
        this.dataStore = new DataStore();
        this.blockingManager = new BlockingOperationsManager(dataStore);
        this.commandProcessor = new CommandProcessor(dataStore, blockingManager);
    }

    public void start() {
        startTimeoutChecker();

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.out.println("Redis server started on port " + port);

            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    new Thread(new ClientHandler(clientSocket, commandProcessor, num++)).start();
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
}
