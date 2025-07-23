package su.grinev.bson;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static su.grinev.bson.Utility.decodeDecimal128;
import static su.grinev.bson.Utility.findNullByteSimdLong;

public final class ObjectReader {
    private final Pool<ReaderContext> contextPool;

    public ObjectReader(Pool<ReaderContext> pool) {
        contextPool = pool;
    }

    public boolean readElement(ByteBuffer buffer, Deque<ReaderContext> stack, Map<String, Object> map, List<Object> list) {
        Object value;

        int type = buffer.get();
        if (type == 0) {
            return false;
        }

        String key = readCStringSIMD(buffer);

        switch (type) {
            case 0x01 -> value = buffer.getDouble();
            case 0x02 -> value = readString(buffer); // UTF-8 String
            case 0x03 -> { // Embedded document
                int length = buffer.getInt();
                value = new HashMap<>();
                ReaderContext readerContext = contextPool.get().setPos(buffer.position() - 4).setValue(value);
                stack.add(readerContext);
                buffer.position(buffer.position() + length - 4);
            }
            case 0x04 -> { // Array
                int length = buffer.getInt();
                value = new ArrayList<>();
                ReaderContext readerContext = contextPool.get().setPos(buffer.position() - 4).setValue(value);
                stack.add(readerContext);
                buffer.position(buffer.position() + length - 4);
            }
            case 0x05 -> value = readBinary(buffer);
            case 0x07 -> value = readObjectId(buffer);
            case 0x08 -> value = buffer.get() != 0;
            case 0x09 -> value = readDateTime(buffer);
            case 0x0A -> value = null;
            case 0x10 -> value = buffer.getInt();
            case 0x12 -> value = buffer.getLong();
            case 0x13 -> value = readDecimal128(buffer);
            default -> throw new IllegalArgumentException("Unsupported BSON type: 0x" + Integer.toHexString(type));
        }

        if (map != null) {
            map.put(key, value);
        } else if (list != null) {
            list.add(value);
        }
        return true;
    }

    private String readString(ByteBuffer buffer) {
        int len = buffer.getInt();
        int start = buffer.position();

        buffer.position(start + len);
        return new String(buffer.array(), start, len - 1, StandardCharsets.UTF_8);
    }

    public String readCStringSIMD(ByteBuffer buffer) {
        int start = buffer.position();
        int nullPos = findNullByteSimdLong(buffer);
        int len = nullPos - start;

        buffer.position(buffer.position() + len + 1);
        return new String(buffer.array(), start, len, StandardCharsets.UTF_8);
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

        byte[] temp = new byte[len];
        buffer.get(temp);
        return temp;
    }

    private static String readObjectId(ByteBuffer buffer) {
        byte[] oid = new byte[12];
        buffer.get(oid);
        StringBuilder sb = new StringBuilder(24);
        for (byte b : oid) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static LocalDateTime readDateTime(ByteBuffer buffer) {
        long epochMillis = buffer.getLong();
        return Instant.ofEpochMilli(epochMillis)
                .atZone(ZoneOffset.UTC)
                .toLocalDateTime();
    }

    private static BigDecimal readDecimal128(ByteBuffer buffer) {
        long low = buffer.getLong();
        long high = buffer.getLong();
        return decodeDecimal128(low, high);
    }
}
