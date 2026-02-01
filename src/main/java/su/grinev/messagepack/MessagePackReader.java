package su.grinev.messagepack;

import su.grinev.bson.Document;
import su.grinev.pool.FastPool;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessagePackReader {

    private static final int STRING_BUFFER_SIZE = 256;

    private final FastPool<ReaderContext> contextPool;
    private final FastPool<ArrayDeque<ReaderContext>> stackPool;
    private final boolean useProjectionsForByteBuffer;
    private final boolean useByteBufferForBinary;

    // Thread-local string buffer - avoids pool overhead for every string read
    private final ThreadLocal<byte[]> stringBuffer = ThreadLocal.withInitial(() -> new byte[STRING_BUFFER_SIZE]);

    public MessagePackReader(FastPool<byte[]> stringBufferPool,
                             FastPool<ReaderContext> contextPool,
                             FastPool<ArrayDeque<ReaderContext>> stackPool,
                             boolean useProjectionsForByteBuffer,
                             boolean useByteBufferForBinary) {
        // stringBufferPool is kept for API compatibility but not used
        this.contextPool = contextPool;
        this.stackPool = stackPool;
        this.useProjectionsForByteBuffer = useProjectionsForByteBuffer;
        this.useByteBufferForBinary = useByteBufferForBinary;
    }

    public Document deserialize(ByteBuffer buffer) {
        Map<String, Object> root = new HashMap<>();
        ArrayDeque<ReaderContext> stack = stackPool.get();

        try {
            int rootSize = getMapSize(buffer);
            stack.addFirst(contextPool.get().init(root, rootSize));

            outer:
            while (!stack.isEmpty()) {
                ReaderContext current = stack.getFirst();
                int stackSize = stack.size();
                Map<String, Object> map = current.objectMap;

                while (current.index < current.size) {
                    current.index++;
                    String key = readKeyString(buffer);
                    Object value = readValue(buffer, stack);
                    map.put(key, value);

                    if (stack.size() > stackSize) {
                        continue outer;
                    }
                }

                stack.removeFirst();
                current.reset();
                contextPool.release(current);
            }
            return new Document(root);
        } finally {
            stack.clear();
            stackPool.release(stack);
        }
    }

    // Specialized key reader - keys are always strings, skip full type dispatch
    private String readKeyString(ByteBuffer buffer) {
        byte b = buffer.get();
        int unsigned = b & 0xFF;

        // Keys are almost always fixstr (short strings)
        if (unsigned >= 0xA0 && unsigned <= 0xBF) {
            int len = unsigned & 0x1F;
            byte[] strBuf = stringBuffer.get();
            if (strBuf.length < len) {
                strBuf = new byte[Math.max(len, STRING_BUFFER_SIZE * 2)];
                stringBuffer.set(strBuf);
            }
            buffer.get(strBuf, 0, len);
            return new String(strBuf, 0, len, StandardCharsets.UTF_8);
        }

        // Less common: longer strings
        int len = switch (unsigned) {
            case 0xD9 -> buffer.get() & 0xFF;    // STR8
            case 0xDA -> buffer.getShort() & 0xFFFF; // STR16
            case 0xDB -> buffer.getInt(); // STR32
            default -> throw new MessagePackException("Expected string key, got 0x" + Integer.toHexString(unsigned));
        };

        byte[] strBuf = stringBuffer.get();
        if (strBuf.length < len) {
            strBuf = new byte[len];
            stringBuffer.set(strBuf);
        }
        buffer.get(strBuf, 0, len);
        return new String(strBuf, 0, len, StandardCharsets.UTF_8);
    }

    private Object readValue(ByteBuffer buffer, ArrayDeque<ReaderContext> stack) {
        byte b = buffer.get();

        // Fast path for common types - inline type detection and handling
        if (b >= 0) {
            // Positive fixint: 0x00-0x7F (most common for small integers)
            return (int) b;
        }

        int unsigned = b & 0xFF;

        if (unsigned >= 0xA0 && unsigned <= 0xBF) {
            // Fixstr: 0xA0-0xBF - use thread-local buffer
            int len = unsigned & 0x1F;
            byte[] strBuf = stringBuffer.get();
            if (strBuf.length < len) {
                strBuf = new byte[Math.max(len, STRING_BUFFER_SIZE * 2)];
                stringBuffer.set(strBuf);
            }
            buffer.get(strBuf, 0, len);
            return new String(strBuf, 0, len, StandardCharsets.UTF_8);
        }

        if (unsigned >= 0x80 && unsigned <= 0x8F) {
            // Fixmap: 0x80-0x8F - inline map creation
            int size = unsigned & 0x0F;
            Map<String, Object> map = new HashMap<>(size + size / 3 + 1);
            stack.addFirst(contextPool.get().init(map, size));
            return map;
        }

        if (unsigned >= 0xE0) {
            // Negative fixint: 0xE0-0xFF
            return (int) b;
        }

        if (unsigned >= 0x90 && unsigned <= 0x9F) {
            // Fixarray: 0x90-0x9F
            int size = unsigned & 0x0F;
            List<Object> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readValue(buffer, stack));
            }
            return list;
        }

        // Less common types
        return switch (unsigned) {
            case 0xC0 -> null;  // NIL
            case 0xC2 -> false; // FALSE
            case 0xC3 -> true;  // TRUE
            case 0xCC -> buffer.get() & 0xFF;     // UINT8
            case 0xCD -> buffer.getShort() & 0xFFFF; // UINT16
            case 0xCE -> buffer.getInt() & 0xFFFFFFFFL; // UINT32
            case 0xCF -> buffer.getLong(); // UINT64
            case 0xD0 -> (int) buffer.get();   // INT8
            case 0xD1 -> (int) buffer.getShort(); // INT16
            case 0xD2 -> buffer.getInt();  // INT32
            case 0xD3 -> buffer.getLong(); // INT64
            case 0xCA -> buffer.getFloat();  // FLOAT32
            case 0xCB -> buffer.getDouble(); // FLOAT64
            case 0xD9 -> readString(buffer, buffer.get() & 0xFF);    // STR8
            case 0xDA -> readString(buffer, buffer.getShort() & 0xFFFF); // STR16
            case 0xDB -> readString(buffer, buffer.getInt()); // STR32
            case 0xC4 -> readBinary(buffer, buffer.get() & 0xFF);    // BIN8
            case 0xC5 -> readBinary(buffer, buffer.getShort() & 0xFFFF); // BIN16
            case 0xC6 -> readBinary(buffer, buffer.getInt()); // BIN32
            case 0xDC -> readArray(buffer, stack, buffer.getShort() & 0xFFFF); // ARRAY16
            case 0xDD -> readArray(buffer, stack, buffer.getInt()); // ARRAY32
            case 0xDE -> readMap(stack, buffer.getShort() & 0xFFFF); // MAP16
            case 0xDF -> readMap(stack, buffer.getInt()); // MAP32
            case 0xD4 -> readExtension(buffer, 1);
            case 0xD5 -> readExtension(buffer, 2);
            case 0xD6 -> readExtension(buffer, 4);
            case 0xD7 -> readExtension(buffer, 8);
            case 0xD8 -> readExtension(buffer, 16);
            case 0xC7 -> readExtension(buffer, buffer.get() & 0xFF);
            case 0xC8 -> readExtension(buffer, buffer.getShort() & 0xFFFF);
            case 0xC9 -> readExtension(buffer, buffer.getInt());
            case 0xC1 -> throw new MessagePackException("Invalid format byte 0xC1");
            default -> throw new MessagePackException("Unknown format byte 0x" + Integer.toHexString(unsigned));
        };
    }

    private Map<String, Object> readMap(ArrayDeque<ReaderContext> stack, int size) {
        Map<String, Object> map = new HashMap<>(size + size / 3 + 1);
        stack.addFirst(contextPool.get().init(map, size));
        return map;
    }

    private List<Object> readArray(ByteBuffer buffer, ArrayDeque<ReaderContext> stack, int size) {
        List<Object> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(readValue(buffer, stack));
        }
        return list;
    }

    private Object readBinary(ByteBuffer buffer, int length) {
        if (useByteBufferForBinary) {
            if (useProjectionsForByteBuffer) {
                ByteBuffer byteBuffer = buffer.slice(buffer.position(), length);
                buffer.position(buffer.position() + length);
                return byteBuffer;
            } else {
                ByteBuffer byteBuffer = ByteBuffer.allocateDirect(length);
                int oldLimit = buffer.limit();
                buffer.limit(buffer.position() + length);
                byteBuffer.put(buffer);
                buffer.limit(oldLimit);
                byteBuffer.flip();
                return byteBuffer;
            }
        } else {
            byte[] data = new byte[length];
            buffer.get(data, 0, length);
            return data;
        }
    }

    private MessagePackExtension readExtension(ByteBuffer buffer, int length) {
        byte extType = buffer.get();
        byte[] data = new byte[length];
        buffer.get(data);
        return new MessagePackExtension(extType, data);
    }

    private String readString(ByteBuffer buffer, int len) {
        byte[] strBuf = stringBuffer.get();
        if (strBuf.length < len) {
            strBuf = new byte[len];
            stringBuffer.set(strBuf);
        }
        buffer.get(strBuf, 0, len);
        return new String(strBuf, 0, len, StandardCharsets.UTF_8);
    }

    private int getMapSize(ByteBuffer buffer) {
        byte b = buffer.get();
        int unsigned = b & 0xFF;

        if (unsigned >= 0x80 && unsigned <= 0x8F) {
            return unsigned & 0x0F;
        } else if (unsigned == 0xDE) {
            return buffer.getShort() & 0xFFFF;
        } else if (unsigned == 0xDF) {
            return buffer.getInt();
        }
        throw new MessagePackException("Unexpected type 0x" + Integer.toHexString(unsigned));
    }
}
