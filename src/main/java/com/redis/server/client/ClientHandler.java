package com.redis.server.client;

import com.redis.server.RedisConstants;
import com.redis.server.command.CommandProcessor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
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
                    List<String> command = parseRespArray(line, in);
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

    private List<String> parseRespArray(String arrayLine, BufferedReader in) throws IOException {
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