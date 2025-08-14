package com.redis.server.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class RedisSortedSet {
    private final TreeSet<SortedSetMember> sortedMembers;

    public RedisSortedSet() {
        this.sortedMembers = new TreeSet<>();
    }

    public void addMember(SortedSetMember member) {
        sortedMembers.add(member);
    }

    public boolean containsMember(String member) {
        return sortedMembers.contains(member);
    }

    public int size() {
        return sortedMembers.size();
    }

    public boolean isEmpty() {
        return sortedMembers.isEmpty();
    }

    public Set<SortedSetMember> getAllMembers() {
        return sortedMembers;
    }

    @Override
    public String toString() {
        return String.format("RedisSortedSet{size=%d, members=%s}", size(), sortedMembers);
    }
}
