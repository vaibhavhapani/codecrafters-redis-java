package com.redis.server;

public class Main {
    public static void main(String[] args) {
        int len = args.length;
        int port = RedisConstants.DEFAULT_PORT;

        if(len > 0 && RedisConstants.PORT_ARG.equals(args[0])) port = Integer.parseInt(args[1]);

        RedisServer server = new RedisServer(port);
        server.start();
    }
}