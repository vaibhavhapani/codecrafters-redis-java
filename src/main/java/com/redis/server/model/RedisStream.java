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

    public List<StreamEntry> getEntriesInRange(String startId, String endId, boolean isStartExclusive) {
        List<StreamEntry> entriesInRange = new ArrayList<>();
        if(entries.isEmpty()) {
            System.out.println("stream empty");
            return entriesInRange;
        }

        String normalizedStartId = normalizeId(startId, true);
        String normalizedEndId = normalizeId(endId, false);

        StreamEntry startEntry = new StreamEntry(normalizedStartId, new HashMap<>());
        StreamEntry endEntry = new StreamEntry(normalizedEndId, new HashMap<>());

        for (StreamEntry entry : entries) {
            boolean afterStart = isStartExclusive ? entry.compareId(startEntry) > 0 : entry.compareId(startEntry) >= 0;
            boolean beforeEnd = entry.compareId(endEntry) <= 0;

            if (afterStart && beforeEnd) {
                entriesInRange.add(entry);
            }
        }

        return entriesInRange;
    }

    private String normalizeId(String id, boolean isStart) {
        if("-".equals(id) && isStart) return "0-0";
        if("+".equals(id) && !isStart) return getLastEntry().getId();

        if (id.contains("-")) {
            return id; // Already complete
        }

        // Incomplete ID - add sequence number
        if (isStart) {
            return id + "-0";  // Start with sequence 0
        } else {
            return id + "-" + Long.MAX_VALUE;  // End with maximum sequence
        }
    }

    @Override
    public String toString() {
        return "RedisStream{entries=" + entries.size() + "}";
    }
}
