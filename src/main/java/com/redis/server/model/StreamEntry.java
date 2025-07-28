package com.redis.server.model;

import java.util.List;
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

    public int compareId(StreamEntry other) {
        if (this.millisecondsTime != other.millisecondsTime) {
            return Long.compare(this.millisecondsTime, other.millisecondsTime);
        }
        return Long.compare(this.sequenceNumber, other.sequenceNumber);
    }

    public boolean isIdGreaterThan(StreamEntry other) {
        return compareId(other) > 0;
    }

    public boolean isIdGreaterThanZero() {
        return millisecondsTime > 0 || (millisecondsTime == 0 && sequenceNumber > 0);
    }

    public static StreamEntry createWithAutoSequence(String idTemplate, Map<String, String> fields,
                                                     RedisStream stream)
            throws IllegalArgumentException {

        if("*".equals(idTemplate)) {
            return new StreamEntry(generateFullSequence(stream), fields);
        }

        String[] parts = idTemplate.split("-");
        if (parts.length != 2 || !"*".equals(parts[1])) {
            throw new IllegalArgumentException("Invalid ID template for auto-sequence: " + idTemplate);
        }

        long millisecondsTime;
        try {
            millisecondsTime = Long.parseLong(parts[0]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid milliseconds time in ID template: " + idTemplate);
        }

        long nextSequence = getNextSequenceNumber(millisecondsTime, stream);
        String actualId = millisecondsTime + "-" + nextSequence;
        return new StreamEntry(actualId, fields);
    }

    public static long getNextSequenceNumber(long millisecondsTime, RedisStream stream) {
        if (stream == null || stream.isEmpty()) return millisecondsTime == 0 ? 1 : 0;

        long maxSequence = -1;
        for (StreamEntry entry : stream.getEntries()) {
            if (entry.getMillisecondsTime() == millisecondsTime) {
                maxSequence = Math.max(maxSequence, entry.getSequenceNumber());
            }
        }

        if (maxSequence == -1) return millisecondsTime == 0 ? 1 : 0;

        return maxSequence + 1;
    }

    public static String generateFullSequence(RedisStream stream) {
        long millisecondsTime = System.currentTimeMillis();
        long sequenceNumber = -1;

        for(StreamEntry entry: stream.getEntries()) {
            if(entry.getMillisecondsTime() == millisecondsTime) sequenceNumber = Math.max(sequenceNumber, entry.getSequenceNumber());
        }

        if(sequenceNumber == -1) sequenceNumber = 0;
        else sequenceNumber++;

        String id = "" + millisecondsTime + "-" + sequenceNumber;
        return id;
    }

    @Override
    public String toString() {
        return "StreamEntry{id='" + id + "', fields=" + fields + "}";
    }
}
