package com.redis.server.model;

import java.util.ArrayList;
import java.util.HashMap;
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

    public StreamEntry getLastEntry() {
        if (entries.isEmpty()) {
            return null;
        }
        return entries.get(entries.size() - 1);
    }

    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public List<StreamEntry> getEntriesInRange(String startId, String endId) {
        StreamEntry startEntry = new StreamEntry(startId, new HashMap<>());
        StreamEntry endEntry = new StreamEntry(endId, new HashMap<>());

        List<StreamEntry> entriesInRange = new ArrayList<>();

        for (StreamEntry entry : entries) {
            if (entry.getId().equals(startId) ||
                    entry.getId().equals(endId) ||
                    entry.isIdGreaterThan(startEntry) ||
                    endEntry.isIdGreaterThan(entry)) {
                entriesInRange.add(entry);
            }
        }

        return entriesInRange;
    }

    @Override
    public String toString() {
        return "RedisStream{entries=" + entries.size() + "}";
    }
}
