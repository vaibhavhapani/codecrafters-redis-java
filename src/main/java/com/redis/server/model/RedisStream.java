package com.redis.server.model;

import java.util.ArrayList;
import java.util.List;

public class RedisStream {
    private final List<StreamEntry> entries;

    public RedisStream() {
        this.entries = new ArrayList<>();
    }

    public void addEntry(StreamEntry entry) {
        entries.add(entry);
    }

    public List<StreamEntry> getEntries() {
        return entries;
    }

    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public String toString() {
        return "RedisStream{entries=" + entries.size() + "}";
    }
}
