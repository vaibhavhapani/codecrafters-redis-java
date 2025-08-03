package com.redis.server;

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
                            masterHost = masterInfo[i + 1];
                            masterPort = Integer.parseInt(masterInfo[i + 2]);
                            isReplica = true;
                        }
                        i++;
                    }
                    break;
            }
        }

        RedisServer server = new RedisServer(port, masterHost, masterPort, isReplica);
        server.start();
    }
}