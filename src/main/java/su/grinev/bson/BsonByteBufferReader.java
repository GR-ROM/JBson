package su.grinev.bson;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static su.grinev.bson.Utility.decodeDecimal128;
import static su.grinev.bson.Utility.findNullByteSimdLong;

public class BsonByteBufferReader implements BsonReader {
    private final ByteBuffer buffer;

    public BsonByteBufferReader(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public String readString() {
        int len = buffer.getInt();

        byte[] bytes = new byte[len - 1];
        buffer.get(bytes);
        buffer.position(buffer.position() + 1);

        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public String readCString() {
        int start = buffer.position();
        int nullPos = findNullByteSimdLong(buffer);
        int len = nullPos - start;

        byte[] bytes = new byte[len];
        buffer.get(bytes);

        buffer.position(buffer.position() + 1);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] readBinary() {
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
