package com.redis.server.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class RedisSortedSet {
    private final TreeSet<SortedSetMember> sortedMembers;
    private final Map<String, Double> memberToScore;

    public RedisSortedSet() {
        this.sortedMembers = new TreeSet<>();
        this.memberToScore = new HashMap<>();
    }

    public int addMember(SortedSetMember member) {
        Double existingScore = memberToScore.get(member.getMemberName());

        if (existingScore != null) {
            // Member exists, remove old entry and add new one with updated score
            sortedMembers.remove(new SortedSetMember(member.getMemberName(), existingScore));
            sortedMembers.add(member);
            memberToScore.put(member.getMemberName(), member.getScore());
            return 0; // Existing member updated
        } else {
            // New member
            sortedMembers.add(member);
            memberToScore.put(member.getMemberName(), member.getScore());
            return 1; // New member added
        }
    }

    public boolean containsMember(SortedSetMember member) {
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
