package su.grinev.bson;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static jdk.incubator.vector.ByteVector.SPECIES_PREFERRED;

public class ObjectReader {
    private byte[] buf = new byte[10000];
    private static final byte[] temp = new byte[SPECIES_PREFERRED.length()];
    private static final VectorSpecies<Byte> SPECIES = SPECIES_PREFERRED;
    private final Pool<Context> contextPool;

    public ObjectReader(Pool<Context> pool) {
        contextPool = pool;
    }

    public Map.Entry<String, Object> readObject(ByteBuffer buffer, LinkedList<Context> stack, int type) {
        String key = readString(buffer, true);
        Object value;

        switch (type) {
            case 0x01 -> value = buffer.getDouble();         // Double
            case 0x02 -> value = readString(buffer, false);         // UTF-8 String
            case 0x03 -> {                                   // Embedded document
                value = new HashMap<>();
                int length = buffer.getInt();
                buffer.position(buffer.position() - 4);
                Context context = contextPool.get().setPos(buffer.position()).setValue(value);
                stack.add(context);
                buffer.position(buffer.position() + length);
            }
            case 0x04 -> {                                   // Array
                value = new ArrayList<>();
                int length = buffer.getInt();
                buffer.position(buffer.position() - 4);
                Context context = contextPool.get().setPos(buffer.position()).setValue(value);
                stack.add(context);
                buffer.position(buffer.position() + length);
            }
            case 0x05 -> value = readBinary(buffer);         // Binary data
            case 0x07 -> value = readObjectId(buffer);       // ObjectId
            case 0x08 -> value = buffer.get() != 0;          // Boolean
            case 0x09 -> value = readDateTime(buffer);       // LocalDateTime
            case 0x0A -> value = null;                       // Null
            case 0x10 -> value = buffer.getInt();            // Int32
            case 0x12 -> value = buffer.getLong();           // Int64
            case 0x13 -> value = readDecimal128(buffer);     // Decimal128
            default -> throw new IllegalArgumentException("Unsupported BSON type: 0x" + Integer.toHexString(type));
        }

        return Map.entry(key, value);
    }

    private String readString(ByteBuffer buffer, boolean isKey) {
        int start;
        int len;

        if (isKey) {
            start = buffer.position();
            while (buffer.get() != 0) {}
            int end = buffer.position();
            len = end - start;
        } else {
            len = buffer.getInt();
            start = buffer.position();
        }

        if (buf.length < len) {
            buf = new byte[len * 2];
        }

        buffer.position(start);
        buffer.get(buf, 0, len - 1);
        buffer.get();

        return new String(buf, 0, len - 1, StandardCharsets.UTF_8);
    }

    public static int findNullByteSIMD(ByteBuffer buffer) {
        int start = buffer.position();
        int limit = buffer.limit();
        int i = start;

        while (i + SPECIES.length() <= limit) {
            buffer.get(temp);
            ByteVector vector = ByteVector.fromArray(SPECIES, temp, 0);
            VectorMask<Byte> mask = vector.eq((byte) 0);

            if (mask.anyTrue()) {
                int indexInVector = mask.firstTrue();
                return i + indexInVector;
            }

            i += SPECIES.length();
        }

        return i;
    }

    public String readCStringSIMD(ByteBuffer buffer) {
        int start = buffer.position();
      //  int nullPos = findNullByteSIMD(buffer);
        int c = 0;
        while (buffer.get() != 0x00) { c++; }

        int len = c - start;
        if (buf.length < len) {
            throw new IllegalArgumentException("Temp buffer too small");
        }

        buffer.position(start);
        buffer.get(buf, 0, len);
        buffer.position(buffer.position() + 1);
        return new String(buf, 0, len, StandardCharsets.UTF_8);
    }

    private byte[] readBinary(ByteBuffer buffer) {
        int len = buffer.getInt();
        byte subtype = buffer.get();

        if (subtype == 0x02) {
            int innerLen = buffer.getInt();
            if (innerLen != len - 4) {
                throw new IllegalStateException("Invalid old binary format: length mismatch");
            }
            len = innerLen;
        }
        byte[] data = new byte[len];
        buffer.get(data);
        return data;
    }

    private String readObjectId(ByteBuffer buffer) {
        byte[] oid = new byte[12];
        buffer.get(oid);
        StringBuilder sb = new StringBuilder(24);
        for (byte b : oid) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private LocalDateTime readDateTime(ByteBuffer buffer) {
        long epochMillis = buffer.getLong();
        return Instant.ofEpochMilli(epochMillis)
                .atZone(ZoneOffset.UTC)
                .toLocalDateTime();
    }

    private BigDecimal readDecimal128(ByteBuffer buffer) {
        byte[] bytes = new byte[16];
        buffer.get(bytes);
        return new BigDecimal("0");
    }
}
