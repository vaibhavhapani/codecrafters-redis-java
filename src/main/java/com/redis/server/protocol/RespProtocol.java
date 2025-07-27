package com.redis.server.protocol;

import com.redis.server.RedisConstants;

import java.io.IOException;
import java.io.OutputStream;

public class RespProtocol {
    public static void writeSimpleString(String message, OutputStream out) throws IOException {
        out.write((RedisConstants.SIMPLE_STRING_PREFIX + message + RedisConstants.CRLF).getBytes());
    }

    public static void writeError(String message, OutputStream out) throws IOException {
        out.write((RedisConstants.ERROR_PREFIX + message + RedisConstants.CRLF).getBytes());
    }

    public static void writeInteger(int value, OutputStream out) throws IOException {
        out.write((RedisConstants.INTEGER_PREFIX + value + RedisConstants.CRLF).getBytes());
    }

    public static void writeBulkString(String value, OutputStream out) throws IOException {
        if (value == null) {
            writeNullBulkString(out);
        } else {
            out.write((RedisConstants.BULK_STRING_PREFIX + value.length() + RedisConstants.CRLF + value + RedisConstants.CRLF).getBytes());
        }
    }

    public static void writeNullBulkString(OutputStream out) throws IOException {
        out.write(RedisConstants.NULL_BULK_STRING.getBytes());
    }

    public static void writeArray(int length, OutputStream out) throws IOException {
        out.write((RedisConstants.ARRAY_PREFIX + length + RedisConstants.CRLF).getBytes());
    }
}
