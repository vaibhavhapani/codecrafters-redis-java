package com.redis.server;

import com.redis.server.model.ServerConfig;

public class Main {
    public static void main(String[] args) {
        int len = args.length;
        int port = RedisConstants.DEFAULT_PORT;
        String masterHost = null;
        int masterPort = -1;
        boolean isReplica = false;

        for (int i = 0; i < len; i++) {
            switch (args[i]) {
                case RedisConstants.PORT_ARG:
                    if (i + 1 < len) {
                        port = Integer.parseInt(args[i + 1]);
                        i++;
                    }
                    break;
                case RedisConstants.IS_REPLICA_OF_ARG:
                    if (i + 1 < len) {
                        String[] masterInfo = args[i + 1].split(" ");

                        if (masterInfo.length == 2) {
                            masterHost = masterInfo[0];
                            masterPort = Integer.parseInt(masterInfo[1]);
                            isReplica = true;
                        }
                        i++;
                    }
                    break;
            }
        }

        ServerConfig serverConfig = new ServerConfig(port, isReplica, masterHost, masterPort);
        RedisServer server = new RedisServer(serverConfig);
        server.start();
    }
}