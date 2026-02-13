package su.grinev.messagepack;

import lombok.Getter;
import lombok.Setter;
import su.grinev.BinaryDocument;
import su.grinev.Serializer;
import su.grinev.pool.DynamicByteBuffer;
import su.grinev.pool.Pool;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MessagePackWriter implements Serializer {
    private final Pool<WriterContext> contextPool;
    private final Map<Integer, byte[]> keyCache = new HashMap<>();
    @Setter
    @Getter
    private boolean writeLengthHeader;


    public MessagePackWriter(Pool<WriterContext> contextPool) {
        this.contextPool = contextPool;
        writeLengthHeader = true;
    }

    @SuppressWarnings("unchecked")
    public void serialize(DynamicByteBuffer buffer, BinaryDocument document) {
        buffer.getBuffer().clear().order(ByteOrder.BIG_ENDIAN);
        if (writeLengthHeader) {
            buffer.putInt(0);
        }
        Map<Integer, Object> documentMap = document.getDocumentMap();
        LinkedList<WriterContext> stack = new LinkedList<>();
        stack.push(contextPool.get().init(documentMap.entrySet().iterator()));

        writeMapHeader(buffer, documentMap.size());

        while (!stack.isEmpty()) {
            WriterContext context = stack.getFirst();
            int stackSize = stack.size();

            while (context.objectMap.hasNext()) {
                Map.Entry<Integer, Object> entry = context.objectMap.next();
                //writeString(buffer, entry.getKey());
                writeInt(buffer, entry.getKey());

                Object value = entry.getValue();
                if (value instanceof Map map) {
                    writeMapHeader(buffer, map.size());
                    stack.push(contextPool.get().init(map.entrySet().iterator()));
                    break;
                } else {
                    writeValue(buffer, value);
                }
            }

            if (stack.size() == stackSize) {
                WriterContext ctx = stack.removeFirst();
                ctx.reset();
                contextPool.release(ctx);
            }
        }

        if (writeLengthHeader) {
            int bufferSize = buffer.getBuffer().position();
            buffer.position(0).putInt(bufferSize);
            buffer.position(bufferSize);
        }
        buffer.flip();
    }

    private void writeMapHeader(DynamicByteBuffer buffer, int size) {
        if (size < 16) {
            buffer.put((byte) (0x80 | size));
        } else if (size < 65536) {
            buffer.put((byte) 0xDE);
            buffer.putShort((short) size);
        } else {
            buffer.put((byte) 0xDF);
            buffer.putInt(size);
        }
    }

    private void writeArrayHeader(DynamicByteBuffer buffer, int size) {
        if (size < 16) {
            buffer.put((byte) (0x90 | size));
        } else if (size < 65536) {
            buffer.put((byte) 0xDC);
            buffer.putShort((short) size);
        } else {
            buffer.put((byte) 0xDD);
            buffer.putInt(size);
        }
    }

    @SuppressWarnings("unchecked")
    private void writeValue(DynamicByteBuffer buffer, Object value) {
        switch (value) {
            case null -> buffer.put((byte) 0xC0);
            case Boolean b -> buffer.put(b ? (byte) 0xC3 : (byte) 0xC2);
            case Integer i -> writeInt(buffer, i);
            case Long l -> writeLong(buffer, l);
            case Float f -> buffer.put((byte) 0xCA).putFloat(f);
            case Double d -> buffer.put((byte) 0xCB).putDouble(d);
            case String s -> writeString(buffer, s);
            case byte[] bytes -> writeBinary(buffer, bytes);
            case ByteBuffer bb -> writeBinary(buffer, bb);
            case List list -> {
                writeArrayHeader(buffer, list.size());
                for (Object item : list) {
                    writeValue(buffer, item);
                }
            }
            case Map map -> {
                writeMapHeader(buffer, map.size());
                for (Object e : map.entrySet()) {
                    Map.Entry<Integer, Object> entry = (Map.Entry<Integer, Object>) e;
                    writeInt(buffer, entry.getKey());
                    writeValue(buffer, entry.getValue());
                }
            }
            case MessagePackExtension ext -> writeExtension(buffer, ext);
            case Instant inst -> writeTimestamp(buffer, inst);
            case LocalDateTime ldt -> writeTimestamp(buffer, ldt.toInstant(ZoneOffset.UTC));
            default -> throw new MessagePackException("Unsupported type: " + value.getClass().getName());
        }
    }

    private void writeInt(DynamicByteBuffer buffer, int value) {
        if (value >= 0 && value <= 127) {
            buffer.put((byte) value);
        } else if (value >= -32 && value < 0) {
            buffer.put((byte) value);
        } else if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
            buffer.put((byte) 0xD0);
            buffer.put((byte) value);
        } else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
            buffer.put((byte) 0xD1);
            buffer.putShort((short) value);
        } else {
            buffer.put((byte) 0xD2);
            buffer.putInt(value);
        }
    }

    private void writeLong(DynamicByteBuffer buffer, long value) {
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            writeInt(buffer, (int) value);
        } else {
            buffer.put((byte) 0xD3).putLong(value);
        }
    }

    private void writeString(DynamicByteBuffer buffer, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        int len = bytes.length;
        if (len < 32) {
            buffer.put((byte) (0xA0 | len));
        } else if (len < 256) {
            buffer.put((byte) 0xD9).put((byte) len);
        } else if (len < 65536) {
            buffer.put((byte) 0xDA).putShort((short) len);
        } else {
            buffer.put((byte) 0xDB).putInt(len);
        }
        buffer.put(bytes);
    }

    private void writeBinary(DynamicByteBuffer buffer, byte[] bytes) {
        int len = bytes.length;
        if (len < 256) {
            buffer.put((byte) 0xC4).put((byte) len);
        } else if (len < 65536) {
            buffer.put((byte) 0xC5).putShort((short) len);
        } else {
            buffer.put((byte) 0xC6).putInt(len);
        }
        buffer.put(bytes);
    }

    private void writeBinary(DynamicByteBuffer buffer, ByteBuffer bb) {
        int len = bb.remaining();
        if (len < 256) {
            buffer.put((byte) 0xC4).put((byte) len);
        } else if (len < 65536) {
            buffer.put((byte) 0xC5).putShort((short) len);
        } else {
            buffer.put((byte) 0xC6).putInt(len);
        }
        buffer.getBuffer().put(bb);
    }

    private void writeExtension(DynamicByteBuffer buffer, MessagePackExtension ext) {
        int len = ext.data().length;
        switch (len) {
            case 1 -> buffer.put((byte) 0xD4);
            case 2 -> buffer.put((byte) 0xD5);
            case 4 -> buffer.put((byte) 0xD6);
            case 8 -> buffer.put((byte) 0xD7);
            case 16 -> buffer.put((byte) 0xD8);
            default -> {
                if (len < 256) {
                    buffer.put((byte) 0xC7).put((byte) len);
                } else if (len < 65536) {
                    buffer.put((byte) 0xC8).putShort((short) len);
                } else {
                    buffer.put((byte) 0xC9).putInt(len);
                }
            }
        }
        buffer.put(ext.type())
                .put(ext.data());
    }

    private void writeTimestamp(DynamicByteBuffer buffer, Instant instant) {
        long seconds = instant.getEpochSecond();
        int nanos = instant.getNano();

        if (nanos == 0 && seconds >= 0 && seconds <= 0xFFFFFFFFL) {
            // Timestamp 32: fixext 4 (0xD6), type=-1, 4 bytes uint32 seconds
            buffer.put((byte) 0xD6).put((byte) -1).putInt((int) seconds);
        } else if (seconds >= 0 && seconds <= 0x3FFFFFFFFL) {
            // Timestamp 64: fixext 8 (0xD7), type=-1, 8 bytes
            // Upper 30 bits = nanoseconds, lower 34 bits = seconds
            long val = ((long) nanos << 34) | seconds;
            buffer.put((byte) 0xD7).put((byte) -1).putLong(val);
        } else {
            // Timestamp 96: ext 8 format (0xC7), length=12, type=-1, 4 bytes nanos + 8 bytes seconds
            buffer.put((byte) 0xC7).put((byte) 12).put((byte) -1).putInt(nanos).putLong(seconds);
        }
    }
}
