package su.grinev.bson;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static su.grinev.bson.WriterContext.fillForArray;
import static su.grinev.bson.WriterContext.fillForDocument;

public class BsonWriter {
    public static final int CONCURRENCY_LEVEL = 1000;
    public static final int INITIAL_POOL_SIZE = 1000;
    public static final int MAX_POOL_SIZE = 100000;
    private static final int INITIAL_BUFFER_SIZE = 64 * 1024;
    private final Pool<DynamicByteBuffer> bufferPool = new Pool<>(CONCURRENCY_LEVEL, CONCURRENCY_LEVEL, () -> new DynamicByteBuffer(INITIAL_BUFFER_SIZE));
    private final Pool<WriterContext> writerContextPool = new Pool<>(CONCURRENCY_LEVEL * INITIAL_POOL_SIZE, CONCURRENCY_LEVEL * MAX_POOL_SIZE, WriterContext::new);
    private boolean isNestedObjectPending;

    public ByteBuffer serialize(Map<String, Object> document) {
        DynamicByteBuffer buffer = null;

        try {
            Deque<WriterContext> stack = new ArrayDeque<>(64);

            buffer = bufferPool.get();
            buffer.initBuffer();

            WriterContext writerContext = writerContextPool.get();
            stack.addLast(fillForDocument(writerContext, null, 0, document));

            while (!stack.isEmpty()) {
                WriterContext ctx = stack.getLast();

                if (ctx.idx == 0) {
                    buffer.ensureCapacity(4);
                    ctx.length += 4;
                    buffer.position(buffer.position() + 4); // reserve space for length
                }

                isNestedObjectPending = false;

                if (ctx.mapEntries != null) {
                    while (ctx.idx < ctx.mapEntries.size()) {
                        writeElement(
                                buffer,
                                ctx.mapEntries.get(ctx.idx).getKey(),
                                ctx.mapEntries.get(ctx.idx).getValue(),
                                ctx,
                                stack
                        );
                        ctx.idx++;
                        if (isNestedObjectPending) {
                            break;
                        }
                    }
                } else if (ctx.listEntries != null) {
                    while (ctx.idx < ctx.listEntries.size()) {
                        writeElement(
                                buffer,
                                Integer.toString(ctx.idx),
                                ctx.listEntries.get(ctx.idx),
                                ctx,
                                stack
                        );
                        ctx.idx++;
                        if (isNestedObjectPending) {
                            break;
                        }
                    }
                }

                if (!isNestedObjectPending) {
                    writeTerminator(buffer, ctx);
                    buffer.putInt(ctx.lengthPos, ctx.length);
                    if (ctx.parent != null) {
                        ctx.parent.length += ctx.length;
                    }
                    stack.removeLast();
                    writerContextPool.release(ctx);
                }
            }

            return buffer.flip().getBuffer();
        } finally {
            bufferPool.release(buffer);
        }
    }

    private void writeTerminator(DynamicByteBuffer buffer, WriterContext ctx) {
        buffer.ensureCapacity(1);
        buffer.put((byte) 0x00);
        ctx.length += 1;
    }

    private void writeElement(
            DynamicByteBuffer buffer,
            String key,
            Object value,
            WriterContext ctx,
            Deque<WriterContext> stack
    ) {
        int start = buffer.position();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        switch (value) {
            case String s -> {
                byte[] strBytes = s.getBytes(StandardCharsets.UTF_8);
                buffer.ensureCapacity( 1 + keyBytes.length + 1 + 4 + strBytes.length + 1);
                buffer.put((byte) 0x02); // string

                writeCString(buffer, keyBytes);
                buffer.putInt(strBytes.length + 1);
                writeCString(buffer, strBytes);
            }
            case Integer i -> {
                buffer.ensureCapacity( 1 + keyBytes.length + 1 + 4);
                buffer.put((byte) 0x10); // int32

                writeCString(buffer, keyBytes);
                buffer.putInt(i);
            }
            case Long l -> {
                buffer.ensureCapacity( 1 + keyBytes.length + 1 + 8);
                buffer.put((byte) 0x12); // int64

                writeCString(buffer, keyBytes);
                buffer.putLong(l);
            }
            case Double d -> {
                buffer.ensureCapacity( 1 + keyBytes.length + 1 + 8);
                buffer.put((byte) 0x01); // double

                writeCString(buffer, keyBytes);
                buffer.putDouble(d);
            }
            case Boolean b -> {
                buffer.ensureCapacity( 1 + keyBytes.length + 1 + 1);
                buffer.put((byte) 0x08); // boolean

                writeCString(buffer, keyBytes);
                buffer.put((byte) (b ? 1 : 0));
            }
            case null -> {
                buffer.ensureCapacity( 1 + keyBytes.length + 1);
                buffer.put((byte) 0x0A); // null

                writeCString(buffer, keyBytes);
            }
            case byte[] bytes -> {
                buffer.ensureCapacity(1 + keyBytes.length + 1 + 4 + 1 + bytes.length);
                buffer.put((byte) 0x05); // type

                writeCString(buffer, keyBytes);
                buffer.putInt(bytes.length)      // block length
                        .put((byte) 0x00)           // generic subtype
                        .put(bytes);               // data
            }
            case Map map -> {
                buffer.ensureCapacity(1 + keyBytes.length + 1);
                buffer.put((byte) 0x03);            // embedded document

                writeCString(buffer, keyBytes);

                WriterContext writerContext = writerContextPool.get();
                stack.addLast(fillForDocument(writerContext, ctx, buffer.position(), map));
                isNestedObjectPending = true;
            }
            case List list -> {
                buffer.ensureCapacity(1 + keyBytes.length + 1);
                buffer.put((byte) 0x04); // array
                writeCString(buffer, keyBytes);

                WriterContext writerContext = writerContextPool.get();

                stack.addLast(fillForArray(writerContext, ctx, buffer.position(), list));
                isNestedObjectPending = true;
            }
            default -> throw new IllegalArgumentException("Unsupported type: " + value.getClass());
        }

        ctx.length += buffer.position() - start;
    }

    private static void writeCString(DynamicByteBuffer buffer, byte[] keyBytes) {
        buffer.put(keyBytes).put((byte) 0x00);
    }
}
