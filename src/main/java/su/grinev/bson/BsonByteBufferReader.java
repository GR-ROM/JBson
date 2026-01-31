package su.grinev.bson;

import lombok.extern.slf4j.Slf4j;
import su.grinev.exception.BsonException;
import su.grinev.pool.Pool;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static su.grinev.bson.Utility.decodeDecimal128;

@Slf4j
public class BsonByteBufferReader implements BsonReader {
    private final ByteBuffer buffer;
    private final Pool<byte[]> bufferPool;
    private final Pool<ByteBuffer> byteBufferPool;

    public BsonByteBufferReader(ByteBuffer buffer, Pool<byte[]> bufferPool, Pool<ByteBuffer> binaryPacketPool) {
        this.buffer = buffer;
        this.bufferPool = bufferPool;
        this.byteBufferPool = binaryPacketPool;
    }

    @Override
    public String readString() {
        byte[] bytes = bufferPool.get();
        try {
            int len = buffer.getInt() - 1;

            bytes = ensureBufferCapacity(bytes, len);
            buffer.get(bytes, 0, len);
            buffer.position(buffer.position() + 1);

            return new String(bytes, 0, len, StandardCharsets.UTF_8);
        } finally {
            bufferPool.release(bytes);
        }
    }

    private static byte[] ensureBufferCapacity(byte[] bytes, int len) {
        if (len > bytes.length) {
            bytes = new byte[len];
        }
        return bytes;
    }

    @Override
    public String readCString() {
        byte[] bytes = bufferPool.get();

        try {
            int len = 0;
            for (int i = buffer.position(); buffer.get(i++) != 0; len++) {}

            bytes = ensureBufferCapacity(bytes, len);
            buffer.get(bytes, 0, len);
            buffer.position(buffer.position() + 1);
            return new String(bytes, 0, len, StandardCharsets.UTF_8);
        } finally {
            bufferPool.release(bytes);
        }
    }

    @Override
    public byte[] readBinaryAsArray() {
        int len = buffer.getInt();
        byte subtype = buffer.get();

        if (subtype == 0x02) {
            int innerLen = buffer.getInt();
            if (innerLen != len - 4) {
                throw new BsonException("Invalid old binary format: length mismatch");
            }
            len = innerLen;
        }

        byte[] temp = new byte[len];
        buffer.get(temp, 0, len);
        return temp;
    }

    @Override
    public ByteBuffer readBinary(boolean bufferProjection) {
        int len = buffer.getInt();
        byte subtype = buffer.get();

        if (subtype == 0x02) {
            int innerLen = buffer.getInt();
            if (innerLen != len - 4) {
                throw new BsonException("Invalid old binary format: length mismatch");
            }
            len = innerLen;
        }

        if (len < 0) {
            throw new BsonException("Negative binary length: " + len);
        }

        if (len > buffer.remaining()) {
            throw new BsonException("Binary data truncated: len=" + len + ", remaining=" + buffer.remaining());
        }

        ByteBuffer buffer1;
        if (bufferProjection) {
            buffer1 = buffer.slice(buffer.position(), len);
            buffer.position(buffer.position() + len);
        } else {
            buffer1 = byteBufferPool.get().clear();

            if (len > buffer1.capacity()) {
                buffer1 = ByteBuffer.allocateDirect(len);
                log.warn("Reallocated direct buffer for binary data: {} bytes", len);
            }

            int oldLimit = buffer.limit();
            buffer.limit(buffer.position() + len);
            buffer1.put(buffer);
            buffer.limit(oldLimit);
            buffer1.flip();
        }
        return buffer1;
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
