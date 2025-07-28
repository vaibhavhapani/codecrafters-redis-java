package com.redis.server.model;

import java.util.Map;

public class StreamEntry {
    private final String id;
    private final Map<String, String> fields;
    private final long millisecondsTime;
    private final long sequenceNumber;

    public StreamEntry(String id, Map<String, String> fields) {
        this.id = id;
        this.fields = fields;

        String[] parts = id.split("-");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid stream entry ID format: " + id);
        }

        try {
            this.millisecondsTime = Long.parseLong(parts[0]);
            this.sequenceNumber = Long.parseLong(parts[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid stream entry ID format: " + id);
        }
    }

    public String getId() {
        return id;
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public String getField(String key) {
        return fields.get(key);
    }

    public long getMillisecondsTime() {
        return millisecondsTime;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public boolean isIdGreaterThan(StreamEntry other) {
        if (this.millisecondsTime != other.millisecondsTime) {
            return this.millisecondsTime > other.millisecondsTime;
        }
        return this.sequenceNumber > other.sequenceNumber;
    }

    public boolean isIdGreaterThanZero() {
        return millisecondsTime > 0 || (millisecondsTime == 0 && sequenceNumber > 0);
    }

    @Override
    public String toString() {
        return "StreamEntry{id='" + id + "', fields=" + fields + "}";
    }
}
