package com.redis.server.command;

import com.redis.server.RedisConstants;
import com.redis.server.blocking.BlockingOperationsManager;
import com.redis.server.protocol.RespProtocol;
import com.redis.server.storage.DataStore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class CommandProcessor {

    private final CommandHandlers handlers;
    private final DataStore dataStore;
    private final boolean isReplica;
    private final String masterHost;
    private final int masterPort;

    public CommandProcessor(Boolean isReplica, String masterHost, int masterPort, DataStore dataStore, BlockingOperationsManager blockingManager) {
        this.isReplica = isReplica;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.dataStore = dataStore;
        this.handlers = new CommandHandlers(dataStore, blockingManager);
    }

    public void processCommand(String clientId, List<String> command, OutputStream out) throws IOException {
        String commandName = command.get(0).toUpperCase();

        switch (commandName) {
            case RedisConstants.PING:
                handlers.handlePing(command, out);
                break;
            case RedisConstants.ECHO:
                handlers.handleEcho(command, out);
                break;
            case RedisConstants.TYPE:
                handlers.handleType(command, out);
                break;
            case RedisConstants.SET:
                handlers.handleSet(clientId, command, out);
                break;
            case RedisConstants.GET:
                handlers.handleGet(clientId, command, out);
                break;
            case RedisConstants.RPUSH:
                handlers.handleRPush(command, out);
                break;
            case RedisConstants.LPUSH:
                handlers.handleLPush(command, out);
                break;
            case RedisConstants.LRANGE:
                handlers.handleLRange(command, out);
                break;
            case RedisConstants.LLEN:
                handlers.handleLLen(command, out);
                break;
            case RedisConstants.LPOP:
                handlers.handleLPop(command, out);
                break;
            case RedisConstants.BLPOP:
                handlers.handleBLPop(command, out);
                break;
            case RedisConstants.XADD:
                handlers.handleXAdd(command, out);
                break;
            case RedisConstants.XRANGE:
                handlers.handleXRange(command, out);
                break;
            case RedisConstants.XREAD:
                handlers.handleXRead(command, out);
                break;
            case RedisConstants.INCR:
                handlers.handleIncr(clientId, command, out);
                break;
            case RedisConstants.MULTI:
                handlers.handleMulti(clientId, command, out);
                break;
            case RedisConstants.EXEC:
                handlers.handleExec(clientId, command, out);
                break;
            case RedisConstants.DISCARD:
                handlers.handleDiscard(clientId, command, out);
                break;
            case RedisConstants.INFO:
                handlers.handleInfo(clientId, command, out, isReplica, masterHost, masterPort);
                break;
            default:
                RespProtocol.writeError((RedisConstants.ERR_UNKNOWN_COMMAND + commandName), out);
                break;
        }
    }

    public void cleanupClient(String clientId) {
        dataStore.cleanupClient(clientId);
    }
}
