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
    private ByteBuffer buffer = ByteBuffer.wrap(new byte[64 * 1024]);
    private final Pool<WriterContext> writerContextPool = new Pool<>(INITIAL_POOL_SIZE, WriterContext::new);
    private boolean needTraverseObject;

    public ByteBuffer serialize(Map<String, Object> document) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.rewind();

        WriterContext writerContext = writerContextPool.get();
        stack.addLast(fillForDocument(writerContext, null, 0, document));

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
                writeTerminator(ctx);
                buffer.putInt(ctx.lengthPos, ctx.length);
                if (ctx.parent != null) {
                    ctx.parent.length += ctx.length;
                }
                stack.removeLast();
            }
        }

        return buffer.flip();
    }

    private void writeTerminator(WriterContext ctx) {
        ensureCapacity(1);
        buffer.put((byte) 0x00);
        ctx.length += 1;
    }

    private void writeElement(String key, Object value, WriterContext ctx) {
        int start = buffer.position();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        switch (value) {
            case String s -> {
                byte[] strBytes = s.getBytes(StandardCharsets.UTF_8);
                ensureCapacity(1 + keyBytes.length + 1 + 4 + strBytes.length + 1);
                buffer.put((byte) 0x02); // string

                writeCString(keyBytes);
                buffer.putInt(strBytes.length + 1);
                writeCString(strBytes);
            }
            case Integer i -> {
                ensureCapacity(1 + keyBytes.length + 1 + 4);
                buffer.put((byte) 0x10); // int32

                writeCString(keyBytes);
                buffer.putInt(i);
            }
            case Long l -> {
                ensureCapacity(1 + keyBytes.length + 1 + 8);
                buffer.put((byte) 0x12); // int64

                writeCString(keyBytes);
                buffer.putLong(l);
            }
            case Double d -> {
                ensureCapacity(1 + keyBytes.length + 1 + 8);
                buffer.put((byte) 0x01); // double

                writeCString(keyBytes);
                buffer.putDouble(d);
            }
            case Boolean b -> {
                ensureCapacity(1 + keyBytes.length + 1 + 1);
                buffer.put((byte) 0x08); // boolean

                writeCString(keyBytes);
                buffer.put((byte) (b ? 1 : 0));
            }
            case null -> {
                ensureCapacity(1 + keyBytes.length + 1);
                buffer.put((byte) 0x0A); // null

                writeCString(keyBytes);
            }
            case byte[] bytes -> {
                ensureCapacity(1 + keyBytes.length + 1 + 4 + 1 + bytes.length);
                buffer.put((byte) 0x05); // type

                writeCString(keyBytes);
                buffer.putInt(bytes.length)      // block length
                        .put((byte) 0x00)           // generic subtype
                        .put(bytes);               // data
            }
            case Map map -> {
                ensureCapacity(1 + keyBytes.length + 1);
                buffer.put((byte) 0x03);            // embedded document

                writeCString(keyBytes);

                needTraverseObject = true;

                WriterContext writerContext = writerContextPool.get();
                stack.addLast(fillForDocument(writerContext, ctx, buffer.position(), map));
            }
            case List list -> {
                ensureCapacity(1 + keyBytes.length + 1);
                buffer.put((byte) 0x04); // array
                writeCString(keyBytes);

                needTraverseObject = true;

                WriterContext writerContext = writerContextPool.get();
                stack.addLast(fillForArray(writerContext, ctx, buffer.position(), list));
            }
            default -> throw new IllegalArgumentException("Unsupported type: " + value.getClass());
        }

        int len = buffer.position() - start;
        ctx.length += len;
    }

    private void writeCString(byte[] keyBytes) {
        buffer.put(keyBytes).put((byte) 0x00);
    }

    private void ensureCapacity(int additional) {
        if (buffer.remaining() < additional) {
            ByteBuffer oldBuffer = buffer;
            buffer = ByteBuffer.allocateDirect(Math.max(oldBuffer.capacity() * 2, oldBuffer.capacity() + additional)).order(ByteOrder.LITTLE_ENDIAN);
            oldBuffer.flip();
            buffer.put(oldBuffer);
        }
    }
}
