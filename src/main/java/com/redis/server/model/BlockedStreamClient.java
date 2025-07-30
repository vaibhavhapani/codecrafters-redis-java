package com.redis.server.model;

import java.io.OutputStream;
import java.util.List;

public class BlockedStreamClient {
    private final List<String> streamKeys;
    private final List<String> startIds;
    private final OutputStream out;
    private final long blockTime;
    private final long timeoutTime;

    public BlockedStreamClient(List<String> streamKeys, List<String> startIds, double timeoutMillis, OutputStream out) {
        this.streamKeys = streamKeys;
        this.startIds = startIds;
        this.out = out;
        this.blockTime = System.currentTimeMillis();
        this.timeoutTime = timeoutMillis == 0 ? 0 : blockTime + (long) (timeoutMillis);
    }

    public boolean isTimedOut() {
        return timeoutTime > 0 && System.currentTimeMillis() > timeoutTime;
    }

    public List<String> getStreamKeys() {
        return this.streamKeys;
    }

    public List<String> getStartIds() {
        return this.startIds;
    }

    public OutputStream getOutputStream() {
        return this.out;
    }

    public long getBlockTime() {
        return this.blockTime;
    }
}
