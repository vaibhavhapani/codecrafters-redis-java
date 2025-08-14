package com.redis.server.model;

public class SortedSetMember implements Comparable<SortedSetMember> {
    private final String member;
    private final double score;

    public SortedSetMember(String member, double score) {
        this.member = member;
        this.score = score;
    }

    public String getMember() {
        return member;
    }

    public double getScore() {
        return score;
    }

    @Override
    public int compareTo(SortedSetMember other) {
        int scoreCompare = Double.compare(this.score, other.score);
        return scoreCompare != 0 ? scoreCompare : this.member.compareTo(other.member);
    }

    @Override
    public String toString() {
        return String.format("SortedSetMember{member='%s', score=%f}", member, score);
    }

}
