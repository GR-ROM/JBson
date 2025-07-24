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
        BsonReader bsonByteBufferReader = new BsonByteBufferReader(buffer);
        Deque<ReaderContext> stack = new ArrayDeque<>(64);

        ReaderContext readerContext = contextPool.get();
        stack.addLast(readerContext.setPos(bsonByteBufferReader.position()).setValue(rootDocument));

        int len = bsonByteBufferReader.readInt();
        bsonByteBufferReader.position(bsonByteBufferReader.position() - 4);

        while (!stack.isEmpty()) {
            readerContext = stack.removeLast();

            bsonByteBufferReader.position(readerContext.getPos());
            bsonByteBufferReader.readInt();

            if (readerContext.getValue() instanceof Map m) {
                while (readElement(bsonByteBufferReader, stack, m, null)) {
                }
            } else if (readerContext.getValue() instanceof List l) {
                while (readElement(bsonByteBufferReader, stack, null, l)) {
                }
            }
            contextPool.release(readerContext);
        }

        return new Document(rootDocument, len);
    }

    public Document deserialize(InputStream inputStream) {
        DynamicByteBuffer byteBuffer = new DynamicByteBuffer(64 * 1024);
        byte[] chunk = new byte[64 * 1024];
        int bytesRead;
        try {
            while ((bytesRead = inputStream.read(chunk)) != -1) {
                byteBuffer.ensureCapacity(bytesRead);
                byteBuffer.put(chunk);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byteBuffer.flip();
        return deserialize(byteBuffer.getBuffer());
    }

    private boolean readElement(BsonReader objectReader, Deque<ReaderContext> stack, Map<String, Object> map, List<Object> list) {
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
                int length = objectReader.readInt();
                value = new HashMap<>();
                ReaderContext readerContext = contextPool.get().setPos(objectReader.position() - 4).setValue(value);
                stack.add(readerContext);
                objectReader.position(objectReader.position() + length - 4);
            }
            case 0x04 -> { // Array
                int length = objectReader.readInt();
                value = new ArrayList<>();
                ReaderContext readerContext = contextPool.get().setPos(objectReader.position() - 4).setValue(value);
                stack.add(readerContext);
                objectReader.position(objectReader.position() + length - 4);
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
