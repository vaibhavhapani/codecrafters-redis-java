package com.redis.server.command;

import com.redis.server.RedisConstants;
import com.redis.server.blocking.BlockingOperationsManager;
import com.redis.server.model.ServerConfig;
import com.redis.server.protocol.RespProtocol;
import com.redis.server.storage.DataStore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class CommandProcessor {

    private final CommandHandlers handlers;
    private final DataStore dataStore;
    private final ServerConfig serverConfig;

    public CommandProcessor(ServerConfig serverConfig, DataStore dataStore, BlockingOperationsManager blockingManager) {
        this.serverConfig = serverConfig;
        this.dataStore = dataStore;
        this.handlers = new CommandHandlers(dataStore, blockingManager, serverConfig);
    }

    public void processCommand(String clientId, List<String> command, OutputStream out) throws IOException, InterruptedException {
        String commandName = command.get(0).toUpperCase();

        switch (commandName) {
            case RedisConstants.PING:
                handlers.handlePing(clientId, command, out);
                break;
            case RedisConstants.ECHO:
                handlers.handleEcho(clientId, command, out);
                break;
            case RedisConstants.TYPE:
                handlers.handleType(clientId, command, out);
                break;
            case RedisConstants.SET:
                handlers.handleSet(clientId, command, out);
                break;
            case RedisConstants.GET:
                handlers.handleGet(clientId, command, out);
                break;
            case RedisConstants.RPUSH:
                handlers.handleRPush(clientId, command, out);
                break;
            case RedisConstants.LPUSH:
                handlers.handleLPush(clientId, command, out);
                break;
            case RedisConstants.LRANGE:
                handlers.handleLRange(clientId, command, out);
                break;
            case RedisConstants.LLEN:
                handlers.handleLLen(clientId, command, out);
                break;
            case RedisConstants.LPOP:
                handlers.handleLPop(clientId, command, out);
                break;
            case RedisConstants.BLPOP:
                handlers.handleBLPop(clientId, command, out);
                break;
            case RedisConstants.XADD:
                handlers.handleXAdd(clientId, command, out);
                break;
            case RedisConstants.XRANGE:
                handlers.handleXRange(clientId, command, out);
                break;
            case RedisConstants.XREAD:
                handlers.handleXRead(clientId, command, out);
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
                handlers.handleInfo(clientId, command, out);
                break;
            case RedisConstants.REPLCONF:
                handlers.handleReplconf(clientId, command, out);
                break;
            case RedisConstants.PSYNC:
                handlers.handlePsync(clientId, command, out);
                break;
            case RedisConstants.WAIT:
                handlers.handleWait(clientId, command, out);
                break;
            case RedisConstants.ZADD:
                handlers.handleZadd(clientId, command, out);
                break;
            case RedisConstants.ZRANK:
                handlers.handleZrank(clientId, command, out);
                break;
            case RedisConstants.ZRANGE:
                handlers.handleZrange(clientId, command, out);
                break;
            case RedisConstants.ZCARD:
                handlers.handleZcard(clientId, command, out);
                break;
            case RedisConstants.ZSCORE:
                handlers.handleZscore(clientId, command, out);
                break;
            case RedisConstants.ZREM:
                handlers.handleZrem(clientId, command, out);
                break;
            case RedisConstants.SUBSCRIBE:
                handlers.handleSubscribe(clientId, command, out);
                break;
            case RedisConstants.PUBLISH:
                handlers.handlePublish(clientId, command, out);
                break;
            case RedisConstants.UNSUBSCRIBE:
                handlers.handleUnsubscribe(clientId, command, out);
                break;
            case RedisConstants.CONFIG:
                handlers.handleConfig(clientId, command, out);
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
