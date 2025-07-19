package su.grinev.bson;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static su.grinev.bson.WriterContext.fillForArray;
import static su.grinev.bson.WriterContext.fillForDocument;

public class BsonWriter {
    public static final int INITIAL_POOL_SIZE = 1000;
    private final Deque<WriterContext> stack = new ArrayDeque<>(64);
    private ByteBuffer buffer = ByteBuffer.wrap(new byte[128 * 1024]);
    private final Pool<WriterContext> writerContextPool = new Pool<>(INITIAL_POOL_SIZE, WriterContext::new);
    private boolean needTraverseObject;

    public ByteBuffer serialize(Map<String, Object> document) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.rewind();

        WriterContext writerContext = writerContextPool.get();
        stack.addLast(writerContext
                .setParent(null)
                .setLength(0)
                .setLengthPos(buffer.position())
                .setIdx(0)
                .setMapEntries(document.entrySet().stream().toList())
                .setListEntries(null));

        while (!stack.isEmpty()) {
            WriterContext ctx = stack.getLast();

            if (ctx.idx == 0) {
                ensureCapacity(4);
                ctx.length += 4;
                buffer.position(buffer.position() + 4); // reserve space for length
            }

            needTraverseObject = false;

            if (ctx.mapEntries != null) {
                while (ctx.idx < ctx.mapEntries.size()) {
                    writeElement(ctx.mapEntries.get(ctx.idx).getKey(), ctx.mapEntries.get(ctx.idx).getValue(), ctx);
                    ctx.idx++;
                    if (needTraverseObject) {
                        break;
                    }
                }
            } else if (ctx.listEntries != null) {
                while (ctx.idx < ctx.listEntries.size()) {
                    writeElement(Integer.toString(ctx.idx), ctx.listEntries.get(ctx.idx), ctx);
                    ctx.idx++;
                    if (needTraverseObject) {
                        break;
                    }
                }
            }

            if (!needTraverseObject) {
                appendTerminator(ctx);
                buffer.putInt(ctx.lengthPos, ctx.length);
                if (ctx.parent != null) {
                    ctx.parent.length += ctx.length;
                }
                stack.removeLast();
            }
        }

        return buffer.flip();
    }

    private void appendTerminator(WriterContext ctx) {
        ensureCapacity(1);
        buffer.put((byte) 0x00);
        ctx.length += 1;
    }

    private void writeElement(String key, Object value, WriterContext ctx) {
        int start = buffer.position();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        if (value instanceof String s) {
            byte[] strBytes = s.getBytes(StandardCharsets.UTF_8);
            ensureCapacity(1 + keyBytes.length + 1 + 4 + strBytes.length + 1);
            buffer.put((byte) 0x02); // string
            appendCString(keyBytes);
            buffer.putInt(strBytes.length + 1);
            appendCString(strBytes);
        } else if (value instanceof Integer i) {
            ensureCapacity(1 + keyBytes.length + 1 + 4);
            buffer.put((byte) 0x10); // int32
            appendCString(keyBytes);
            buffer.putInt(i);
        } else if (value instanceof Long l) {
            ensureCapacity(1 + keyBytes.length + 1 + 8);
            buffer.put((byte) 0x12); // int64
            appendCString(keyBytes);
            buffer.putLong(l);
        } else if (value instanceof Double d) {
            ensureCapacity(1 + keyBytes.length + 1 + 8);
            buffer.put((byte) 0x01); // double
            appendCString(keyBytes);
            buffer.putDouble(d);
        } else if (value instanceof Boolean b) {
            ensureCapacity(1 + keyBytes.length + 1 + 1);
            buffer.put((byte) 0x08); // boolean
            appendCString(keyBytes);
            buffer.put((byte) (b ? 1 : 0));
        } else if (value == null) {
            ensureCapacity(1 + keyBytes.length + 1);
            buffer.put((byte) 0x0A);             // null
            appendCString(keyBytes);
        } else if (value instanceof byte[] bytes) { // binary data
            ensureCapacity(1 + keyBytes.length + 1 + 4 + 1 + bytes.length);
            buffer.put((byte) 0x05);            // type
            appendCString(keyBytes);
            buffer.putInt(bytes.length)      // block length
                    .put((byte) 0x00)           // generic subtype
                    .put(bytes);               // data
        } else if (value instanceof Map) {
            ensureCapacity(1 + keyBytes.length + 1);
            buffer.put((byte) 0x03);            // embedded document
            appendCString(keyBytes);

            needTraverseObject = true;

            WriterContext writerContext = writerContextPool.get();
            stack.addLast(fillForDocument(writerContext, ctx, buffer.position(), (Map<String, Object>) value));
        } else if (value instanceof List) {
            ensureCapacity(1 + keyBytes.length + 1);
            buffer.put((byte) 0x04); // array
            appendCString(keyBytes);

            needTraverseObject = true;

            WriterContext writerContext = writerContextPool.get();
            stack.addLast(fillForArray(writerContext, ctx, buffer.position(), (List<Object>) value));
        } else {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass());
        }

        int len = buffer.position() - start;
        ctx.length += len;
    }

    private void appendCString(byte[] keyBytes) {
        buffer.put(keyBytes).put((byte) 0x00);
    }

    private void ensureCapacity(int additional) {
        if (buffer.remaining() < additional) {
            int oldPosition = buffer.position();
            ByteBuffer oldBuffer = buffer;
            buffer = ByteBuffer.allocateDirect(Math.max(oldBuffer.capacity() * 2, oldBuffer.capacity() + additional));
            oldBuffer.flip();
            buffer.put(oldBuffer);
            buffer.position(oldPosition);
        }
    }
}
