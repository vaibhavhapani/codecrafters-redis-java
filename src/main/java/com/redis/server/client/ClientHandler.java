package com.redis.server.client;

import com.redis.server.RedisConstants;
import com.redis.server.command.CommandProcessor;
import com.redis.server.protocol.RespProtocol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.UUID;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final CommandProcessor commandProcessor;
    private final String clientId;

    public ClientHandler(Socket clientSocket, CommandProcessor commandProcessor) {
        this.clientSocket = clientSocket;
        this.commandProcessor = commandProcessor;
        clientId = generateClientId();
    }
    private String generateClientId() {
        return UUID.randomUUID().toString(); // Or alternatively: return clientSocket.getRemoteSocketAddress().toString() + "-" + System.currentTimeMillis();
    }
    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             OutputStream out = clientSocket.getOutputStream()) {

            String line;
            while ((line = in.readLine()) != null) {
                if (line.startsWith(RedisConstants.ARRAY_PREFIX)) {
                    List<String> command = RespProtocol.parseRespArray(line, in);
                    if (!command.isEmpty()) {
                        commandProcessor.processCommand(clientId, command, out);
                        out.flush();
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error handling client: " + e.getMessage());
        } finally {
            commandProcessor.cleanupClient(clientId);
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }
}