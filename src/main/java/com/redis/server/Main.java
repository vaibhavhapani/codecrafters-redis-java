package com.redis.server;

public class Main {
    public static void main(String[] args) {
        RedisServer server = new RedisServer(RedisConstants.DEFAULT_PORT);
        server.start();
    }
}