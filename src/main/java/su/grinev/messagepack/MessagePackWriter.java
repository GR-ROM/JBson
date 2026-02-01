package su.grinev.messagepack;

import su.grinev.bson.Document;
import su.grinev.pool.DisposablePool;
import su.grinev.pool.DynamicByteBuffer;
import su.grinev.pool.Pool;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MessagePackWriter {

    private final DisposablePool<DynamicByteBuffer> bufferPool;
    private final Pool<WriterContext> contextPool;

    public MessagePackWriter(DisposablePool<DynamicByteBuffer> bufferPool, Pool<WriterContext> contextPool) {
        this.bufferPool = bufferPool;
        this.contextPool = contextPool;
    }

    @SuppressWarnings("unchecked")
    public DynamicByteBuffer serialize(Document document) {
        Map<String, Object> documentMap = document.getDocumentMap();
        DynamicByteBuffer buffer = bufferPool.get();
        buffer.getBuffer().clear().order(ByteOrder.BIG_ENDIAN);
        LinkedList<WriterContext> stack = new LinkedList<>();
        stack.push(contextPool.get().init(documentMap.entrySet().iterator()));

        writeMapHeader(buffer, documentMap.size());

        while (!stack.isEmpty()) {
            WriterContext context = stack.getFirst();
            int stackSize = stack.size();

            while (context.objectMap.hasNext()) {
                Map.Entry<String, Object> entry = context.objectMap.next();
                writeString(buffer, entry.getKey());

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

        return buffer;
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
            case Float f -> {
                buffer.put((byte) 0xCA);
                buffer.putFloat(f);
            }
            case Double d -> {
                buffer.put((byte) 0xCB);
                buffer.putDouble(d);
            }
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
                    Map.Entry<String, Object> entry = (Map.Entry<String, Object>) e;
                    writeString(buffer, entry.getKey());
                    writeValue(buffer, entry.getValue());
                }
            }
            case MessagePackExtension ext -> writeExtension(buffer, ext);
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
            buffer.put((byte) 0xD3);
            buffer.putLong(value);
        }
    }

    private void writeString(DynamicByteBuffer buffer, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        int len = bytes.length;
        if (len < 32) {
            buffer.put((byte) (0xA0 | len));
        } else if (len < 256) {
            buffer.put((byte) 0xD9);
            buffer.put((byte) len);
        } else if (len < 65536) {
            buffer.put((byte) 0xDA);
            buffer.putShort((short) len);
        } else {
            buffer.put((byte) 0xDB);
            buffer.putInt(len);
        }
        buffer.put(bytes);
    }

    private void writeBinary(DynamicByteBuffer buffer, byte[] bytes) {
        int len = bytes.length;
        if (len < 256) {
            buffer.put((byte) 0xC4);
            buffer.put((byte) len);
        } else if (len < 65536) {
            buffer.put((byte) 0xC5);
            buffer.putShort((short) len);
        } else {
            buffer.put((byte) 0xC6);
            buffer.putInt(len);
        }
        buffer.put(bytes);
    }

    private void writeBinary(DynamicByteBuffer buffer, ByteBuffer bb) {
        int len = bb.remaining();
        if (len < 256) {
            buffer.put((byte) 0xC4);
            buffer.put((byte) len);
        } else if (len < 65536) {
            buffer.put((byte) 0xC5);
            buffer.putShort((short) len);
        } else {
            buffer.put((byte) 0xC6);
            buffer.putInt(len);
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
                    buffer.put((byte) 0xC7);
                    buffer.put((byte) len);
                } else if (len < 65536) {
                    buffer.put((byte) 0xC8);
                    buffer.putShort((short) len);
                } else {
                    buffer.put((byte) 0xC9);
                    buffer.putInt(len);
                }
            }
        }
        buffer.put(ext.type());
        buffer.put(ext.data());
    }
}
