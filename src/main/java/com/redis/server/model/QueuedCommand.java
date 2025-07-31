package com.redis.server.model;

import java.io.OutputStream;
import java.util.List;
import java.util.Queue;

public class QueuedCommand {
    private final List<String> command;
    private final OutputStream out;

    public QueuedCommand(List<String> command, OutputStream out) {
        this.command = command;
        this.out = out;
    }

    public OutputStream getOutputStream() {
        return this.out;
    }

    public List<String> getCommand(){
        return this.command;
    }
}
