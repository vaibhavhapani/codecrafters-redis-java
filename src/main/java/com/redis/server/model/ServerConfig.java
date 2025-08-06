package com.redis.server.model;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class ServerConfig {
    private final int port;
    private final boolean isReplica;
    private final String masterHost;
    private final int masterPort;
    private int replicaPort;
    private List<OutputStream> slaves;

    public ServerConfig(int port, boolean isReplica, String masterHost, int masterPort) {
        this.port = port;
        this.isReplica = isReplica;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.replicaPort = -1;
        this.slaves = new ArrayList<>();
    }

    public int getPort(){
        return port;
    }

    public boolean isReplica() {
        return isReplica;
    }

    public String getMasterHost(){
        return masterHost;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public int getReplicaPort(){
        return replicaPort;
    }

    public void setReplicaPort(int port){
        this.replicaPort = port;
    }

    public boolean hasReplica() {
        return replicaPort != -1;
    }

    public List<OutputStream> getSlaves() {
        return slaves;
    }

    public void addSlave(OutputStream out){
        slaves.add(out);
    }
}
