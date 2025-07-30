package com.redis.server.blocking;

import com.redis.server.RedisConstants;
import com.redis.server.model.BlockedClient;
import com.redis.server.protocol.RespProtocol;
import com.redis.server.storage.DataStore;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

public class BlockingOperationsManager {
    private final PriorityBlockingQueue<BlockedClient> blockedClients;
    private final DataStore dataStore;

    public BlockingOperationsManager(DataStore dataStore) {
        this.dataStore = dataStore;
        this.blockedClients = new PriorityBlockingQueue<>(
                RedisConstants.BLOCKED_CLIENTS_INITIAL_CAPACITY,
                Comparator.comparingLong(BlockedClient::getBlockTime)
        );
    }

    public void addBlockedClient(String key, double timeoutSeconds, java.io.OutputStream out) {
        blockedClients.offer(new BlockedClient(key, timeoutSeconds, out));
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
    }
}
