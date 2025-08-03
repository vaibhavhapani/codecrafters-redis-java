package com.redis.server;

public class RedisConstants {
    public static final String PORT_ARG = "--port";
    public static final int DEFAULT_PORT = 6379;
    public static final String IS_REPLICA_OF_ARG = "--replicaof";

    public static final long TIMEOUT_CHECK_INTERVAL = 50; // ms
    public static final int BLOCKED_CLIENTS_INITIAL_CAPACITY = 5;

    // RESP Protocol constants
    public static final String CRLF = "\r\n";
    public static final String SIMPLE_STRING_PREFIX = "+";
    public static final String ERROR_PREFIX = "-";
    public static final String INTEGER_PREFIX = ":";
    public static final String BULK_STRING_PREFIX = "$";
    public static final String ARRAY_PREFIX = "*";
    public static final String NULL_BULK_STRING = "$-1\r\n";

    // Commands
    public static final String PING = "PING";
    public static final String ECHO = "ECHO";
    public static final String TYPE = "TYPE";
    public static final String SET = "SET";
    public static final String GET = "GET";
    public static final String RPUSH = "RPUSH";
    public static final String LPUSH = "LPUSH";
    public static final String LRANGE = "LRANGE";
    public static final String LLEN = "LLEN";
    public static final String LPOP = "LPOP";
    public static final String BLPOP = "BLPOP";
    public static final String XADD = "XADD";
    public static final String XRANGE = "XRANGE";
    public static final String XREAD = "XREAD";
    public static final String INCR = "INCR";
    public static final String MULTI = "MULTI";
    public static final String EXEC = "EXEC";
    public static final String DISCARD = "DISCARD";
    public static final String INFO = "INFO";

    // Response messages
    public static final String PONG = "PONG";
    public static final String OK = "OK";
    public static final String NONE_TYPE = "none";
    public static final String STRING_TYPE = "string";

    // Error messages
    public static final String ERR_UNKNOWN_COMMAND = "ERR unknown command";
    public static final String ERR_WRONG_NUMBER_ARGS = "ERR wrong number of arguments for";
}
