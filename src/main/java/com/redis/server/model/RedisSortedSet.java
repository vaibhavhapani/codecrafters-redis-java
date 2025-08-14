package com.redis.server.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class RedisSortedSet {
    private final TreeSet<SortedSetMember> sortedMembers;
    private final Map<String, Double> members;

    public RedisSortedSet() {
        this.sortedMembers = new TreeSet<>();
        this.members = new HashMap<>();
    }

    public int addMember(SortedSetMember member) {
        Double existingScore = members.get(member.getMemberName());

        if (existingScore != null) {
            // Member exists, remove old entry and add new one with updated score
            sortedMembers.remove(new SortedSetMember(member.getMemberName(), existingScore));
            sortedMembers.add(member);
            members.put(member.getMemberName(), member.getScore());
            return 0; // Existing member updated
        } else {
            // New member
            sortedMembers.add(member);
            members.put(member.getMemberName(), member.getScore());
            return 1; // New member added
        }
    }

    public int getRank(String member) {
        if(!members.containsKey(member)) return -1;

        int i = 0;
        for(SortedSetMember it: sortedMembers){
            if(it.getMemberName().equals(member)) return i;
            i++;
        }
        return 0;
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
