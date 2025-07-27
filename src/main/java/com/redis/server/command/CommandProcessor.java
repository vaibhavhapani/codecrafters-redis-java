package com.redis.server.command;

import com.redis.server.blocking.BlockingOperationsManager;
import com.redis.server.storage.DataStore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static com.redis.server.command.CommandHandlers.*;
import static com.redis.server.protocol.RespProtocol.writeSimpleString;

public class CommandProcessor {

    private final DataStore dataStore;
    private final BlockingOperationsManager blockingManager;

    public CommandProcessor(DataStore dataStore, BlockingOperationsManager blockingManager) {
        this.dataStore = dataStore;
        this.blockingManager = blockingManager;
    }

    public static void processCommand(List<String> command, OutputStream out) throws IOException {
        String commandName = command.get(0).toUpperCase();

        switch (commandName) {
            case "PING":
                writeSimpleString("PONG", out);
                break;
            case "ECHO":
                handleEcho(command, out);
                break;
//            case "TYPE":
//                handleType(command, out);
//                break;
//            case "SET":
//                handleSet(command, out);
//                break;
//            case "GET":
//                handleGet(command, out);
//                break;
//            case "RPUSH":
//                handleRPush(command, out);
//                break;
//            case "LPUSH":
//                handleLPush(command, out);
//                break;
//            case "LRANGE":
//                handleLRange(command, out);
//                break;
//            case "LLEN":
//                handleLLen(command, out);
//                break;
//            case "LPOP":
//                handleLPop(command, out);
//                break;
//            case "BLPOP":
//                handleBLPop(command, out);
//                break;
//            case "XADD":
//                handleXAdd(command, out);
//                break;
            default:
                out.write(("-ERR unknown command '" + commandName + "'\r\n").getBytes());
                break;
        }
    }
}
