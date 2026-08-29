package su.grinev;

import lombok.Getter;
import su.grinev.bson.BsonObjectReader;
import su.grinev.bson.BsonObjectWriter;
import su.grinev.messagepack.MessagePackReader;
import su.grinev.messagepack.MessagePackWriter;
import su.grinev.pool.DisposablePool;
import su.grinev.pool.DynamicByteBuffer;
import su.grinev.pool.PoolFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

@Getter
public class Codec {
    private final Binder binder;
    private final Serializer serializer;
    private final Deserializer deserializer;
    private final DisposablePool<DynamicByteBuffer> bufferPool;
    private final ThreadLocal<byte[]> serializeChunk = ThreadLocal.withInitial(() -> new byte[8192]);

    public Codec(Serializer serializer, Deserializer deserializer, DisposablePool<DynamicByteBuffer> bufferPool, Binder.ClassNameMode classNameMode) {
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.bufferPool = bufferPool;
        this.binder = new Binder(classNameMode);
    }

    public static Codec bson(PoolFactory poolFactory, int documentSize, Supplier<ByteBuffer> byteBufferAllocator) {
        return bson(poolFactory, documentSize, byteBufferAllocator, true, Binder.ClassNameMode.FULL_NAME);
    }

    public static Codec bson(PoolFactory poolFactory, int documentSize, Supplier<ByteBuffer> byteBufferAllocator, boolean readBinaryAsByteArray) {
        return bson(poolFactory, documentSize, byteBufferAllocator, readBinaryAsByteArray, Binder.ClassNameMode.FULL_NAME);
    }

    public static Codec bson(PoolFactory poolFactory, int documentSize, Supplier<ByteBuffer> byteBufferAllocator, boolean readBinaryAsByteArray, Binder.ClassNameMode classNameMode) {
        BsonObjectWriter writer = new BsonObjectWriter(poolFactory, documentSize, true);
        BsonObjectReader reader = new BsonObjectReader(poolFactory, documentSize, true, byteBufferAllocator);
        reader.setReadBinaryAsByteArray(readBinaryAsByteArray);
        DisposablePool<DynamicByteBuffer> pool = poolFactory.getDisposablePool("codec-buffer-pool", () -> new DynamicByteBuffer(documentSize, true));
        return new Codec(writer, reader, pool, classNameMode);
    }

    public static Codec messagePack(PoolFactory poolFactory, int documentSize) {
        return messagePack(poolFactory, documentSize, Binder.ClassNameMode.FULL_NAME);
    }

    public static Codec messagePack(PoolFactory poolFactory, int documentSize, Binder.ClassNameMode classNameMode) {
        return messagePack(poolFactory, documentSize, classNameMode, -1);
    }

    /**
     * @param maxCollectionSize hard cap on any decoded array/map element count; the reader rejects
     *                          larger collections (MessagePackException) before allocating. Pass a
     *                          value &lt;= 0 to use the reader default. Tighten it to the largest
     *                          collection your protocol legitimately uses to bound decode allocation.
     */
    public static Codec messagePack(PoolFactory poolFactory, int documentSize, Binder.ClassNameMode classNameMode, int maxCollectionSize) {
        MessagePackWriter writer = new MessagePackWriter();
        MessagePackReader reader = maxCollectionSize > 0
                ? new MessagePackReader(true, true, maxCollectionSize)
                : new MessagePackReader(true, true);
        DisposablePool<DynamicByteBuffer> pool = poolFactory.getDisposablePool("codec-buffer-pool", () -> new DynamicByteBuffer(documentSize, true));
        return new Codec(writer, reader, pool, classNameMode);
    }

    public DynamicByteBuffer serialize(Object o) {
        BinaryDocument document = binder.unbind(o);
        DynamicByteBuffer buffer = bufferPool.get();
        serializer.serialize(buffer, document);
        return buffer;
    }

    public <T> T deserialize(ByteBuffer buffer, Class<T> tClass) {
        return binder.bind(tClass, deserializer.deserialize(buffer));
    }

    public void serialize(Object o, OutputStream outputStream) throws IOException {
        try (DynamicByteBuffer buffer = bufferPool.get()) {
            BinaryDocument document = binder.unbind(o);
            serializer.serialize(buffer, document);
            ByteBuffer raw = buffer.getBuffer();
            byte[] chunk = serializeChunk.get();
            while (raw.hasRemaining()) {
                int len = Math.min(chunk.length, raw.remaining());
                raw.get(chunk, 0, len);
                outputStream.write(chunk, 0, len);
            }
        }
    }

    public <T> T deserialize(InputStream inputStream, Class<T> tClass) throws IOException {
        byte[] data = inputStream.readAllBytes();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return deserialize(buffer, tClass);
    }
}
