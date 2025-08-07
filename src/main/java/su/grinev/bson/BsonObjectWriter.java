package su.grinev.bson;

import su.grinev.pool.DisposablePool;
import su.grinev.pool.DynamicByteBuffer;
import su.grinev.pool.Pool;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static su.grinev.bson.Utility.encodeDecimal128;
import static su.grinev.bson.WriterContext.fillForArray;
import static su.grinev.bson.WriterContext.fillForDocument;

public class BsonObjectWriter {
    private final Pool<WriterContext> writerContextPool;
    private final DisposablePool<DynamicByteBuffer> dynamicByteBufferPool;
    private final Pool<byte[]> bufferPool;

    public BsonObjectWriter(
            int initialPoolSize,
            int maxPoolSize
    ) {
        writerContextPool = new Pool<>(
                initialPoolSize,
                maxPoolSize,
                WriterContext::new
        );
        dynamicByteBufferPool = new DisposablePool<>(
                initialPoolSize,
                maxPoolSize,
                () -> new DynamicByteBuffer(16 * 1024));
        bufferPool = new Pool<>(
                initialPoolSize,
                maxPoolSize,
                () -> new byte[16 * 1024]
        );
    }

    public DynamicByteBuffer serialize(Document document) {
        DynamicByteBuffer buffer = dynamicByteBufferPool.get();
        buffer.initBuffer();

        Deque<WriterContext> stack = new ArrayDeque<>(64);
        WriterContext writerContext = writerContextPool.get();
        stack.addLast(fillForDocument(writerContext, 0, document.getDocumentMap()));

        while (!stack.isEmpty()) {
            WriterContext ctx = stack.getLast();

            if (ctx.idx == 0) {
                ctx.startPos = buffer.position();
                buffer.ensureCapacity(4);
                buffer.position(buffer.position() + 4); // reserve space for length
            }

            if (ctx.mapEntries != null) {
                while (ctx.idx < ctx.mapEntries.size() && !ctx.isNestedObjectPending) {
                    writeElement(buffer, ctx, stack);
                    ctx.idx++;
                }
            } else if (ctx.listEntries != null) {
                while (ctx.idx < ctx.listEntries.size() && !ctx.isNestedObjectPending) {
                    writeElement(buffer, ctx, stack);
                    ctx.idx++;
                }
            }

            if (!ctx.isNestedObjectPending) {
                writeTerminator(buffer, ctx);
                buffer.putInt(ctx.lengthPos, buffer.position() - ctx.startPos);
                stack.removeLast();
                writerContextPool.release(ctx);
            }
            ctx.setNestedObjectPending(false);
        }
        return buffer;
    }

    public void serialize(Document document, OutputStream outputStream) throws IOException {
        try (DynamicByteBuffer dynamicByteBuffer = serialize(document)) {
            dynamicByteBuffer.flip();
            byte[] buf = bufferPool.get();
            try {
                while (dynamicByteBuffer.getBuffer().hasRemaining()) {
                    int chunkSize = Math.min(buf.length, dynamicByteBuffer.getBuffer().remaining());
                    dynamicByteBuffer.getBuffer().get(buf, 0, chunkSize);
                    outputStream.write(buf, 0, chunkSize);
                }
            } finally {
                bufferPool.release(buf);
            }
        }
    }

    private static void writeTerminator(DynamicByteBuffer buffer, WriterContext ctx) {
        buffer.ensureCapacity(1);
        buffer.put((byte) 0x00);
    }

    private void writeElement(DynamicByteBuffer buffer, WriterContext ctx, Deque<WriterContext> stack) {
        String key;
        Object value;

        if (ctx.mapEntries != null) {
            key = ctx.mapEntries.get(ctx.idx).getKey();
            value = ctx.mapEntries.get(ctx.idx).getValue();
        } else {
            key = Integer.toString(ctx.idx);
            value = ctx.listEntries.get(ctx.idx);
        }
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
            case Long l -> writeLong(buffer, l, keyBytes);
            case Double d -> {
                buffer.ensureCapacity( 1 + keyBytes.length + 1 + 8);

                buffer.put((byte) 0x01); // double
                writeCString(buffer, keyBytes);
                buffer.putDouble(d);
            }
            case BigDecimal bigDecimal -> {
                buffer.ensureCapacity(1 + keyBytes.length + 1);

                buffer.put((byte) 0x13);
                writeCString(buffer, keyBytes);
                long[] l = encodeDecimal128(bigDecimal);
                buffer.putLong(l[0]);
                buffer.putLong(l[1]);
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
            case Instant i -> {
                buffer.ensureCapacity(1 + keyBytes.length + 1 + 8);
                buffer.put((byte) 0x09);
                writeCString(buffer, keyBytes);
                buffer.putLong(i.toEpochMilli());
            }
            case Map map -> {
                buffer.ensureCapacity(1 + keyBytes.length + 1);
                buffer.put((byte) 0x03);            // embedded document
                writeCString(buffer, keyBytes);

                WriterContext writerContext = writerContextPool.get();
                stack.addLast(fillForDocument(writerContext, buffer.position(), map));
                ctx.setNestedObjectPending(true);
            }
            case List list -> {
                buffer.ensureCapacity(1 + keyBytes.length + 1);
                buffer.put((byte) 0x04); // array
                writeCString(buffer, keyBytes);

                WriterContext writerContext = writerContextPool.get();

                stack.addLast(fillForArray(writerContext, buffer.position(), list));
                ctx.setNestedObjectPending(true);
            }
            default -> throw new IllegalArgumentException("Unsupported type: " + value.getClass());
        }
    }

    private static void writeLong(DynamicByteBuffer buffer, Long l, byte[] keyBytes) {
        buffer.ensureCapacity( 1 + keyBytes.length + 1 + 8);

        buffer.put((byte) 0x12); // int64
        writeCString(buffer, keyBytes);
        buffer.putLong(l);
    }

    private static void writeCString(DynamicByteBuffer buffer, byte[] keyBytes) {
        buffer.put(keyBytes).put((byte) 0x00);
    }
}
