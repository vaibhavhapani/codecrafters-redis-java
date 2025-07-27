package com.redis.server.model;

import java.io.OutputStream;

public class BlockedClient {
    private final String key;
    private final OutputStream out;
    private final long blockTime;
    private final long timeoutTime;

    public BlockedClient(String key, double timeoutSeconds, OutputStream out) {
        this.key = key;
        this.out = out;
        this.blockTime = System.currentTimeMillis();
        this.timeoutTime = timeoutSeconds == 0 ? 0 : blockTime + (long) (timeoutSeconds * 1000);
    }

    public boolean isTimedOut() {
        return timeoutTime > 0 && System.currentTimeMillis() > timeoutTime;
    }

    public String getKey() {
        return this.key;
    }

    public OutputStream getOutputStream() {
        return this.out;
    }

    public long getBlockTime() {
        return this.blockTime;
    }
}