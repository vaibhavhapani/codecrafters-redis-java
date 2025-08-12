package com.redis.server.model;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

// COULD HAVE CREATED MASTER AND REPLICA CLASSES!!!

public class ServerConfig {
    private final int port;
    private final boolean isReplica;

    // for master server
    private int masterOffset = 0;
    private final ConcurrentHashMap<OutputStream, Integer> replicas;
    private int upToDateReplicas = 0;

    // for replica server
    private final String masterHost;
    private final int masterPort;
    private int replicaOffset = 0;

    public ServerConfig(int port, boolean isReplica, String masterHost, int masterPort) {
        this.port = port;
        this.isReplica = isReplica;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.replicas = new ConcurrentHashMap<>();
    }

    public int getPort() {
        return port;
    }

// ********************************************************* master **********************************************************

    public boolean isMaster() {
        return !isReplica;
    }

    public boolean hasReplicas() {
        return !replicas.isEmpty();
    }

    public int getReplicaCount() {
        return replicas.size();
    }

    public Integer getReplicaPort(OutputStream outputStream) {
        return replicas.get(outputStream);
    }

    public void addReplica(OutputStream out, int port) {
        if (isMaster()) {
            replicas.put(out, port);
            System.out.println("Added replica on port " + port + ". Total replicas: " + getReplicaCount());
        }
    }

    public List<OutputStream> getReplicaOutputStreams() {
        return new ArrayList<>(replicas.keySet());
    }

    public void setMasterOffset(int val){
        this.masterOffset = val;
    }

    public int getMasterOffset(){
        return masterOffset;
    }

    public void setUpToDateReplicas(int upToDateReplicas) {
        this.upToDateReplicas = upToDateReplicas;
    }

    public int getUpToDateReplicas() {
        return upToDateReplicas;
    }

    // ********************************************************* replica **********************************************************

    public boolean isReplica() {
        return isReplica;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public void setReplicaOffset(int val){
        this.replicaOffset = val;
    }

    public int getReplicaOffset(){
        return replicaOffset;
    }
}
