package com.redis.server.blocking;

import com.redis.server.RedisConstants;
import com.redis.server.command.CommandHandlers;
import com.redis.server.model.*;
import com.redis.server.protocol.RespProtocol;
import com.redis.server.storage.DataStore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

public class BlockingOperationsManager {
    private final PriorityBlockingQueue<BlockedClient> blockedClients;
    private final PriorityBlockingQueue<BlockedStreamClient> blockedStreamClients;
    private final DataStore dataStore;

    public BlockingOperationsManager(DataStore dataStore) {
        this.dataStore = dataStore;
        this.blockedClients = new PriorityBlockingQueue<>(
                RedisConstants.BLOCKED_CLIENTS_INITIAL_CAPACITY,
                Comparator.comparingLong(BlockedClient::getBlockTime)
        );
        this.blockedStreamClients = new PriorityBlockingQueue<>(
                RedisConstants.BLOCKED_CLIENTS_INITIAL_CAPACITY,
                Comparator.comparingLong(BlockedStreamClient::getBlockTime)
        );
    }

    public void addBlockedClient(String key, double timeoutSeconds, OutputStream out) {
        blockedClients.offer(new BlockedClient(key, timeoutSeconds, out));
    }

    public void addBlockedStreamClient(List<String> streamKeys, List<String> startIds, double timeoutSeconds, OutputStream out) {
        blockedStreamClients.offer(new BlockedStreamClient(streamKeys, startIds, timeoutSeconds, out));
    }

    public void notifyBlockedClients(String key) throws IOException {
        synchronized (blockedClients) {
            Iterator<BlockedClient> it = blockedClients.iterator();

            while (it.hasNext()) {
                BlockedClient client = it.next();

                if (client.getKey().equals(key)) {
                    List<String> list = dataStore.getList(key);

                    if (list != null && !list.isEmpty()) {
                        String poppedElement = list.remove(0);

                        RespProtocol.writeArray(2, client.getOutputStream());
                        RespProtocol.writeBulkString(key, client.getOutputStream());
                        RespProtocol.writeBulkString(poppedElement, client.getOutputStream());

                        it.remove();
                        return;
                    }
                }
            }
        }
    }

    public void notifyBlockedStreamClients(String streamKey) throws IOException {
        synchronized (blockedStreamClients) {
            Iterator<BlockedStreamClient> it = blockedStreamClients.iterator();

            while (it.hasNext()) {
                BlockedStreamClient client = it.next();

                if (client.getStreamKeys().contains(streamKey)) {
                    List<StreamReadResult> readResults = new ArrayList<>();
                    boolean hasData = false;

                    for (int i = 0; i < client.getStreamKeys().size(); i++) {
                        String clientStreamKey = client.getStreamKeys().get(i);
                        String startId = client.getStartIds().get(i);

                        RedisStream stream = dataStore.getStream(clientStreamKey);
                        if (stream != null) {
                            List<StreamEntry> entries = stream.getEntriesInRange(startId, "+", true);
                            if (!entries.isEmpty()) {
                                readResults.add(new StreamReadResult(clientStreamKey, entries));
                                hasData = true;
                            }
                        }
                    }

                    if (hasData) {
                        RespProtocol.writeXReadResults(readResults, client.getOutputStream());
                        it.remove();
                        return;
                    }
                }
            }
        }
    }

    public void checkTimedOutClients() throws IOException {
        synchronized (blockedClients) {
            Iterator<BlockedClient> it = blockedClients.iterator();
            while (it.hasNext()) {
                BlockedClient client = it.next();
                if (client.isTimedOut()) {
                    RespProtocol.writeNullBulkString(client.getOutputStream());
                    it.remove();
                }
            }
        }

        synchronized (blockedStreamClients) {
            Iterator<BlockedStreamClient> it = blockedStreamClients.iterator();
            while (it.hasNext()) {
                BlockedStreamClient client = it.next();
                if (client.isTimedOut()) {
                    RespProtocol.writeNullBulkString(client.getOutputStream());
                    it.remove();
                }
            }
        }
    }
}
