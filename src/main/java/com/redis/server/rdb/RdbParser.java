package com.redis.server.rdb;

import com.redis.server.storage.DataStore;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.redis.server.rdb.RdbConstants.*;

public class RdbParser {

    private final DataStore dataStore;

    public RdbParser(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    public void loadRdbFile(String dir, String filename) {
        if (dir == null || filename == null) {
            System.out.println("RDB: No database file specified");
            return;
        }

        Path rdbFilePath = Paths.get(dir, filename);

        if (!Files.exists(rdbFilePath)) {
            System.out.println("RDB: Database file does not exist: " + rdbFilePath);
            return;
        }

        try (FileInputStream fis = new FileInputStream(rdbFilePath.toFile());
             DataInputStream dis = new DataInputStream(fis)) {

            System.out.println("RDB: Loading database from " + rdbFilePath);
            parseRdbFile(dis);
            System.out.println("RDB: Database loaded successfully");

        } catch (IOException e) {
            System.err.println("RDB: Error loading database file: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void parseRdbFile(DataInputStream dis) throws IOException {
        parseHeader(dis);

        parseMetadata(dis);

        parseDatabase(dis);

        parseEof(dis);
    }

    private void parseHeader(DataInputStream dis) throws IOException {
        byte[] header = new byte[REDIS_MAGIC.length() + REDIS_VERSION_LENGTH];
        dis.readFully(header);

        String headerStr = new String(header);
        if (!headerStr.startsWith(REDIS_MAGIC)) {
            throw new IOException("Invalid RDB file: missing REDIS magic string");
        }

        String version = headerStr.substring(REDIS_MAGIC.length());
        System.out.println("RDB: File version " + version);
    }

    private void parseMetadata(DataInputStream dis) throws IOException {
        while (true) {
            int opcode = dis.readUnsignedByte();

            if (opcode == METADATA) {
                String key = readStringEncoded(dis);
                String value = readStringEncoded(dis);
                System.out.println("RDB: Metadata " + key + " = " + value);
            } else {
                parseDatabase(dis, opcode);
                return;
            }
        }
    }

    private void parseDatabase(DataInputStream dis) throws IOException {
        int opcode = dis.readUnsignedByte();
        parseDatabase(dis, opcode);
    }

    private void parseDatabase(DataInputStream dis, int firstOpcode) throws IOException {
        int opcode = firstOpcode;

        while (opcode != EOF) {
            if (opcode == SELECTDB) {
                int dbIndex = readSizeEncoded(dis);
                System.out.println("RDB: Selecting database " + dbIndex);

            } else if (opcode == HASH_TABLE_SIZE_INFO) {
                int keyValueHashTableSize = readSizeEncoded(dis);
                int expireHashTableSize = readSizeEncoded(dis);
                System.out.println("RDB: Hash table sizes - keys: " + keyValueHashTableSize + ", expires: " + expireHashTableSize);

            } else if (opcode == EXPIRE_TIME_SECONDS) {
                long expireTimeSeconds = readLittleEndianInt(dis);
                long expireTimeMs = expireTimeSeconds * 1000;
                int valueType = dis.readUnsignedByte();
                parseKeyValue(dis, valueType, expireTimeMs);

            } else if (opcode == EXPIRE_TIME_MILLISECONDS) {
                long expireTimeMs = readLittleEndianLong(dis);
                int valueType = dis.readUnsignedByte();
                parseKeyValue(dis, valueType, expireTimeMs);

            } else {
                parseKeyValue(dis, opcode, -1);
            }

            try {
                opcode = dis.readUnsignedByte();
            } catch (EOFException e) {
                break;
            }
        }
    }

    private void parseKeyValue(DataInputStream dis, int valueType, long expireTime) throws IOException {
        if (valueType == STRING_TYPE) {

            String key = readStringEncoded(dis);
            String value = readStringEncoded(dis);

            System.out.println("RDB: Loading key '" + key + "' = '" + value + "'");

            dataStore.set(key, value);
        }
    }

    private void parseEof(DataInputStream dis) throws IOException {
        byte[] checksum = new byte[8];
        try {
            dis.readFully(checksum);
            System.out.println("RDB: File checksum present");
        } catch (EOFException e) {
            System.out.println("RDB: No checksum found (end of file)");
        }
    }

    private int readSizeEncoded(DataInputStream dis) throws IOException {
        int firstByte = dis.readUnsignedByte();
        int type = (firstByte & 0xC0) >> 6; // First two bits

        switch (type) {
            case 0: // 00: length is remaining 6 bits
                return firstByte & 0x3F;

            case 1: // 01: length is next 14 bits
                int secondByte = dis.readUnsignedByte();
                return ((firstByte & 0x3F) << 8) | secondByte;

            case 2: // 10: length is next 4 bytes
                return dis.readInt(); // Big-endian 4-byte integer

            case 3: // 11: special string encoding
                throw new IOException("Special string encoding in size field not supported here");

            default:
                throw new IOException("Invalid size encoding");
        }
    }

    private String readStringEncoded(DataInputStream dis) throws IOException {
        int firstByte = dis.readUnsignedByte();
        int type = (firstByte & 0xC0) >> 6; // First two bits

        if (type == 3) { // 11: special string encoding
            int encoding = firstByte & 0x3F; // Remaining 6 bits

            switch (encoding) {
                case 0: // 8-bit integer
                    byte int8 = dis.readByte();
                    return String.valueOf(int8);

                case 1: // 16-bit integer (little-endian)
                    int int16 = readLittleEndianShort(dis);
                    return String.valueOf(int16);

                case 2: // 32-bit integer (little-endian)
                    int int32 = readLittleEndianInt(dis);
                    return String.valueOf(int32);

                case 3: // LZF compressed string
                    throw new IOException("LZF compressed strings not supported");

                default:
                    throw new IOException("Unknown string encoding: " + encoding);
            }
        } else {
            // Normal string encoding: calculate size from first byte, then read string
            int size;

            switch (type) {
                case 0: // 00: length is remaining 6 bits
                    size = firstByte & 0x3F;
                    break;

                case 1: // 01: length is next 14 bits
                    int secondByte = dis.readUnsignedByte();
                    size = ((firstByte & 0x3F) << 8) | secondByte;
                    break;

                case 2: // 10: length is next 4 bytes
                    size = dis.readInt(); // Big-endian 4-byte integer
                    break;

                default:
                    throw new IOException("Invalid string size encoding");
            }

            // Read the string bytes
            byte[] stringBytes = new byte[size];
            dis.readFully(stringBytes);

            return new String(stringBytes);
        }
    }

    private int readLittleEndianShort(DataInputStream dis) throws IOException {
        int byte1 = dis.readUnsignedByte();
        int byte2 = dis.readUnsignedByte();
        return byte1 | (byte2 << 8);
    }

    private int readLittleEndianInt(DataInputStream dis) throws IOException {
        int byte1 = dis.readUnsignedByte();
        int byte2 = dis.readUnsignedByte();
        int byte3 = dis.readUnsignedByte();
        int byte4 = dis.readUnsignedByte();
        return byte1 | (byte2 << 8) | (byte3 << 16) | (byte4 << 24);
    }

    private long readLittleEndianLong(DataInputStream dis) throws IOException {
        long byte1 = dis.readUnsignedByte();
        long byte2 = dis.readUnsignedByte();
        long byte3 = dis.readUnsignedByte();
        long byte4 = dis.readUnsignedByte();
        long byte5 = dis.readUnsignedByte();
        long byte6 = dis.readUnsignedByte();
        long byte7 = dis.readUnsignedByte();
        long byte8 = dis.readUnsignedByte();

        return byte1 | (byte2 << 8) | (byte3 << 16) | (byte4 << 24) |
                (byte5 << 32) | (byte6 << 40) | (byte7 << 48) | (byte8 << 56);
    }
}
