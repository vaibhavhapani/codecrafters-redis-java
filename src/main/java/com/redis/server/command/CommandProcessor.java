package com.redis.server.command;

import com.redis.server.blocking.BlockingOperationsManager;
import com.redis.server.storage.DataStore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import static com.redis.server.protocol.RespProtocol.writeSimpleString;

public class CommandProcessor {

    private final CommandHandlers handlers;

    public CommandProcessor(DataStore dataStore, BlockingOperationsManager blockingManager) {
        this.handlers = new CommandHandlers(dataStore, blockingManager);
    }

    public void processCommand(List<String> command, OutputStream out) throws IOException {
        String commandName = command.get(0).toUpperCase();

        switch (commandName) {
            case "PING":
                handlers.handlePing(command, out);
                break;
            case "ECHO":
                handlers.handleEcho(command, out);
                break;
            case "TYPE":
                handlers.handleType(command, out);
                break;
            case "SET":
                handlers.handleSet(command, out);
                break;
            case "GET":
                handlers.handleGet(command, out);
                break;
            case "RPUSH":
                handlers.handleRPush(command, out);
                break;
            case "LPUSH":
                handlers.handleLPush(command, out);
                break;
            case "LRANGE":
                handlers.handleLRange(command, out);
                break;
            case "LLEN":
                handlers.handleLLen(command, out);
                break;
            case "LPOP":
                handlers.handleLPop(command, out);
                break;
            case "BLPOP":
                handlers.handleBLPop(command, out);
                break;
            case "XADD":
                handlers.handleXAdd(command, out);
                break;
            default:
                out.write(("-ERR unknown command '" + commandName + "'\r\n").getBytes());
                break;
        }
    }
}
