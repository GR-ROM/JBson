package su.grinev.bson;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class BsonDeserializer {
    private final Pool<ReaderContext> contextPool;

    public BsonDeserializer(
            int concurrenctyLevel,
            int initialContextStackPoolSize,
            int maxContextStackPoolSize
    ) {
        contextPool = new Pool<>(
                concurrenctyLevel * initialContextStackPoolSize,
                concurrenctyLevel * maxContextStackPoolSize,
                ReaderContext::new
        );
    }

    public Document deserialize(ByteBuffer buffer) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        Map<String, Object> rootDocument = new HashMap<>();
        BsonReader bsonReader = new BsonByteBufferReader(buffer);
        Deque<ReaderContext> stack = new ArrayDeque<>(64);

        ReaderContext ctx = contextPool.get()
                .setNestedObjectPending(false)
                .setPos(bsonReader.position())
                .setValue(rootDocument);
        stack.addLast(ctx);

        while (!stack.isEmpty()) {
            ctx = stack.getLast();

            if (ctx.getValue() instanceof Map m) {
                if (m.isEmpty()) {
                    bsonReader.readInt();
                }
                while (readElement(bsonReader, ctx, stack, m, null) && !ctx.isNestedObjectPending()) {
                }
            } else if (ctx.getValue() instanceof List l) {
                if (l.isEmpty()) {
                    bsonReader.readInt();
                }
                while (readElement(bsonReader, ctx, stack, null, l) && !ctx.isNestedObjectPending()) {
                }
            }

            if (!ctx.isNestedObjectPending()) {
                stack.removeLast();
                contextPool.release(ctx);
            } else {
                ctx.setNestedObjectPending(false);
            }
        }

        return new Document(rootDocument);
    }

    public Document deserialize(InputStream inputStream) {
        DynamicByteBuffer byteBuffer = new DynamicByteBuffer(64 * 1024);
        byte[] chunk = new byte[64 * 1024];
        int bytesRead;
        try {
            while ((bytesRead = inputStream.read(chunk)) != -1) {
                byteBuffer.ensureCapacity(bytesRead);
                byteBuffer.put(chunk, 0, bytesRead);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byteBuffer.flip();
        return deserialize(byteBuffer.getBuffer());
    }

    private boolean readElement(BsonReader objectReader, ReaderContext ctx, Deque<ReaderContext> stack, Map<String, Object> map, List<Object> list) {
        Object value;

        int type = objectReader.readByte();
        if (type == 0) {
            return false;
        }

        String key = objectReader.readCString();

        switch (type) {
            case 0x01 -> value = objectReader.readDouble();
            case 0x02 -> value = objectReader.readString(); // UTF-8 String
            case 0x03 -> { // Embedded document
                value = new HashMap<>();
                ReaderContext readerContext = contextPool.get()
                        .setNestedObjectPending(false)
                        .setPos(objectReader.position() - 4)
                        .setValue(value);
                stack.add(readerContext);
                ctx.setNestedObjectPending(true);
            }
            case 0x04 -> { // Array
                value = new ArrayList<>();
                ReaderContext readerContext = contextPool.get()
                        .setNestedObjectPending(false)
                        .setPos(objectReader.position() - 4)
                        .setValue(value);
                stack.add(readerContext);
                ctx.setNestedObjectPending(true);
            }
            case 0x05 -> value = objectReader.readBinary();
            case 0x07 -> value = objectReader.readObjectId();
            case 0x08 -> value = objectReader.readBoolean();
            case 0x09 -> value = objectReader.readDateTime();
            case 0x0A -> value = null;
            case 0x10 -> value = objectReader.readInt();
            case 0x12 -> value = objectReader.readLong();
            case 0x13 -> value = objectReader.readDecimal128();
            default -> throw new IllegalArgumentException("Unsupported BSON type: 0x" + Integer.toHexString(type));
        }

        if (map != null) {
            map.put(key, value);
        } else if (list != null) {
            list.add(value);
        }
        return true;
    }
}
