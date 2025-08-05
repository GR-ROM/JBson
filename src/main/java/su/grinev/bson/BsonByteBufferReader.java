package su.grinev.bson;

import su.grinev.exception.BsonException;
import su.grinev.pool.Pool;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;

import static su.grinev.bson.Utility.decodeDecimal128;
import static su.grinev.bson.Utility.findNullByteSimdLong;

public class BsonByteBufferReader implements BsonReader {
    private final ByteBuffer buffer;
    private final Pool<byte[]> bufferPool;

    public BsonByteBufferReader(ByteBuffer buffer, Pool<byte[]> bufferPool) {
        this.buffer = buffer;
        this.bufferPool = bufferPool;
    }

    @Override
    public String readString() {
        byte[] bytes = bufferPool.get();
        try {
            int len = buffer.getInt();

            if (len > bytes.length) {
                bytes = new byte[len - 1];
            }
            buffer.get(bytes, 0, len - 1);
            buffer.position(buffer.position() + 1);

            return new String(bytes, 0, len - 1, StandardCharsets.UTF_8);
        } finally {
            bufferPool.release(bytes);
        }
    }

    @Override
    public String readCString() {
        byte[] bytes = bufferPool.get();
        try {
            int start = buffer.position();
            int nullPos = findNullByteSimdLong(buffer);
            int len = nullPos - start;

            if (len > bytes.length) {
                bytes = new byte[len];
            }
            buffer.get(bytes, 0, len);

            buffer.position(buffer.position() + 1);
            return new String(bytes, 0, len, StandardCharsets.UTF_8);
        } finally {
            bufferPool.release(bytes);
        }
    }

    @Override
    public byte[] readBinary() {
        int len = buffer.getInt();
        byte subtype = buffer.get();

        if (subtype == 0x02) {
            int innerLen = buffer.getInt();
            if (innerLen != len - 4) {
                throw new BsonException("Invalid old binary format: length mismatch");
            }
            len = innerLen;
        }

        byte[] array = buffer.array();
        int offset = buffer.arrayOffset() + buffer.position();
        byte[] temp = Arrays.copyOfRange(array, offset, offset + len);
        buffer.position(buffer.position() + len);
        return temp;
    }

    @Override
    public String readObjectId() {
        byte[] oid = new byte[12];
        buffer.get(oid);
        StringBuilder sb = new StringBuilder(24);
        for (byte b : oid) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @Override
    public Instant readDateTime() {
        long epochMillis = buffer.getLong();
        return Instant.ofEpochMilli(epochMillis);
    }

    @Override
    public BigDecimal readDecimal128() {
        long low = buffer.getLong();
        long high = buffer.getLong();
        return decodeDecimal128(low, high);
    }

    @Override
    public double readDouble() {
        return buffer.getDouble();
    }

    @Override
    public float readFloat() {
        return buffer.getFloat();
    }

    @Override
    public int readInt() {
        return buffer.getInt();
    }

    @Override
    public int readInt(int position) {
        return buffer.getInt(position);
    }

    @Override
    public byte readByte() {
        return buffer.get();
    }

    @Override
    public int position() {
        return buffer.position();
    }

    @Override
    public void position(int position) {
        buffer.position(position);
    }

    @Override
    public long readLong() {
        return buffer.getLong();
    }

    @Override
    public boolean readBoolean() {
        return buffer.get() != 0;
    }
}
