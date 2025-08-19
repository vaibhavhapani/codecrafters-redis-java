package com.redis.server.rdb;

public class RdbConstants {
    public static final String REDIS_MAGIC = "REDIS";
    public static final int REDIS_VERSION_LENGTH = 4;

    // RDB opcodes
    public static final int SELECTDB = 0xFE;
    public static final int EXPIRE_TIME_SECONDS = 0xFD;
    public static final int EXPIRE_TIME_MILLISECONDS = 0xFC;
    public static final int HASH_TABLE_SIZE_INFO = 0xFB;
    public static final int METADATA = 0xFA;
    public static final int EOF = 0xFF;

    // Value types
    public static final int STRING_TYPE = 0x00;
}
