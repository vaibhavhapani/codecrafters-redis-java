package com.redis.server.model;

import java.util.*;

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

    public List<String> getMembersInRange(int start, int end){
        List<String> res = new ArrayList<>();

        if(start < 0) start = members.size() + start;
        if(end < 0) end = members.size() + end;

        int i = 0;
        for(SortedSetMember it: sortedMembers){
            if(i >= start && i <= end) res.add(it.getMemberName());
            i++;
        }

        return res;
    }

    public Double getScore(String member) {
        return members.get(member);
    }

    public boolean containsMember(SortedSetMember member) {
        return sortedMembers.contains(member);
    }

    public int size() {
        return sortedMembers.size();
    }

    public int remove(String member) {
        if(!members.containsKey(member)) return 0;

        double score = members.get(member);
        sortedMembers.remove(new SortedSetMember(member, score));
        members.remove(member);
        return 1;
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
