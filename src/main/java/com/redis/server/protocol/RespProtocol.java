package com.redis.server.protocol;

import com.redis.server.RedisConstants;
import com.redis.server.model.StreamEntry;
import com.redis.server.model.StreamReadResult;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    public static void writeEntry(StreamEntry entry, OutputStream out) throws IOException {
        Map<String, String> fields = entry.getFields();
        writeInteger(fields.size(), out);
        for(Map.Entry<String, String> it: fields.entrySet()){
            writeBulkString(it.getKey(), out);
            writeBulkString(it.getValue(), out);
        }
    }

    public static void writeXReadResults(List<StreamReadResult> results, OutputStream out) throws IOException {
        if (results.isEmpty()) {
            RespProtocol.writeNullBulkString(out);
            return;
        }

        RespProtocol.writeArray(results.size(), out);
        for (StreamReadResult result : results) {
            writeXReadResponse(result.getStreamKey(), result.getEntries(), out);
        }
    }

    public static void writeXReadResponse(String streamKey, List<StreamEntry> entries, OutputStream out) throws IOException {
        RespProtocol.writeArray(2, out); // [streamKey, array of entries]
        RespProtocol.writeBulkString(streamKey, out);
        RespProtocol.writeArray(entries.size(), out); // no of entries in an array

        for(StreamEntry entry: entries) {
            RespProtocol.writeArray(2, out); // id, list of pairs
            RespProtocol.writeBulkString(entry.getId(), out);

            Map<String, String> fields = entry.getFields();
            RespProtocol.writeArray(2*fields.size(), out); // key-value

            for(Map.Entry<String, String> it: fields.entrySet()){
                RespProtocol.writeBulkString(it.getKey(), out);
                RespProtocol.writeBulkString(it.getValue(), out);
            }
        }
    }

    public static List<String> parseRespArray(String arrayLine, BufferedReader in) throws IOException {
        int arrayLength = Integer.parseInt(arrayLine.substring(1));
        List<String> command = new ArrayList<>();

        for (int i = 0; i < arrayLength; i++) {
            String lengthLine = in.readLine();
            if (lengthLine != null && lengthLine.startsWith(RedisConstants.BULK_STRING_PREFIX)) {
                int commandLength = Integer.parseInt(lengthLine.substring(1));
                if (commandLength >= 0) {
                    String element = in.readLine();
                    if (element != null) {
                        command.add(element);
                    }
                }
            }
        }
        return command;
    }

    public static String readLineFromInputStream(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int next = in.read();
                if (next == '\n') break;
                sb.append((char)b).append((char)next);
            } else {
                sb.append((char)b);
            }
        }
        return sb.toString();
    }

//    public static List<String> parseRespArrayFromInputStream(InputStream inputStream) throws IOException {
//        String firstLine = readLineFromInputStream(inputStream);
//        if (firstLine == null) {
//            return null;
//        }
//
//        if (!firstLine.startsWith("*")) {
//            throw new IOException("Unexpected RESP array, got: " + firstLine);
//        }
//
//        int arrayLength = Integer.parseInt(firstLine.substring(1));
//        List<String> result = new ArrayList<>();
//
//        for (int i = 0; i < arrayLength; i++) {
//            String lengthLine = readLineFromInputStream(inputStream);
//            if (lengthLine == null || !lengthLine.startsWith("$")) {
//                throw new IOException("Expected bulk string length, got: " + lengthLine);
//            }
//
//            int stringLength = Integer.parseInt(lengthLine.substring(1));
//            if (stringLength == -1) {
//                result.add(null);
//            } else {
//                byte[] data = new byte[stringLength];
//                int totalRead = 0;
//                while (totalRead < stringLength) {
//                    int bytesRead = inputStream.read(data, totalRead, stringLength - totalRead);
//                    if (bytesRead == -1) {
//                        throw new IOException("Unexpected end of stream while reading bulk string");
//                    }
//                    totalRead += bytesRead;
//                }
//
//                int cr = inputStream.read();
//                int lf = inputStream.read();
//                if (cr != '\r' || lf != '\n') {
//                    throw new IOException("Expected \\r\\n after bulk string data");
//                }
//
//                result.add(new String(data));
//            }
//        }
//
//        return result;
//    }

}
