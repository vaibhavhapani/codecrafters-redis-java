package com.redis.server.storage;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class DataStore {
    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> expiry = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, List<String[]>> entries = new ConcurrentHashMap<>();

    public void setValue(String key, String value) {
        store.put(key, value);
        expiry.remove(key);
    }

    public void setValue(String key, String value, long expiryTime) {
        store.put(key, value);
        expiry.put(key, expiryTime);
    }

    public String getValue(String key) {
        if (isExpired(key)) {
            cleanupExpiredKey(key);
            return null;
        }
        return store.get(key);
    }

    public boolean hasKey(String key) {
        if (isExpired(key)) {
            cleanupExpiredKey(key);
            return false;
        }
        return store.containsKey(key);
    }

    public List<String> getList(String key) {
        return lists.get(key);
    }

    public void setList(String key, List<String> list) {
        lists.put(key, list);
    }

    public boolean hasListKey(String key) {
        return lists.containsKey(key);
    }

    public List<String[]> getEntry(String key) {
        return entries.get(key);
    }

//    public void setEntry(String key, List<String> list) {
//        lists.put(key, list);
//    }

    public boolean hasEntryKey(String key) {
        return entries.containsKey(key);
    }

    private boolean isExpired(String key) {
        Long expiryTime = expiry.get(key);
        return expiryTime != null && System.currentTimeMillis() > expiryTime;
    }

    private void cleanupExpiredKey(String key) {
        store.remove(key);
        expiry.remove(key);
    }
}
