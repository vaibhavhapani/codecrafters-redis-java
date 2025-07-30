package com.redis.server.model;

import java.util.List;

public class StreamReadResult {
    private final String streamKey;
    private final List<StreamEntry> entries;

    public StreamReadResult(String streamKey, List<StreamEntry> entries) {
        this.streamKey = streamKey;
        this.entries = entries;
    }

    public String getStreamKey() {
        return streamKey;
    }

    public List<StreamEntry> getEntries() {
        return entries;
    }
}
