package su.grinev;

import su.grinev.bson.BsonObjectReader;
import su.grinev.bson.BsonObjectWriter;
import su.grinev.messagepack.MessagePackReader;
import su.grinev.messagepack.MessagePackWriter;
import su.grinev.messagepack.ReaderContext;
import su.grinev.messagepack.WriterContext;
import su.grinev.pool.DisposablePool;
import su.grinev.pool.DynamicByteBuffer;
import su.grinev.pool.Pool;
import su.grinev.pool.PoolFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.function.Supplier;

public class Codec {
    private final Binder writerBinder = new Binder();
    private final Binder readerBinder = new Binder();
    private final Serializer serializer;
    private final Deserializer deserializer;
    private final DisposablePool<DynamicByteBuffer> bufferPool;

    public Codec(Serializer serializer, Deserializer deserializer, DisposablePool<DynamicByteBuffer> bufferPool) {
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.bufferPool = bufferPool;
    }

    public static Codec bson(PoolFactory poolFactory, int documentSize, Supplier<ByteBuffer> byteBufferAllocator) {
        return bson(poolFactory, documentSize, byteBufferAllocator, true);
    }

    public static Codec bson(PoolFactory poolFactory, int documentSize, Supplier<ByteBuffer> byteBufferAllocator, boolean readBinaryAsByteArray) {
        BsonObjectWriter writer = new BsonObjectWriter(poolFactory, documentSize, true);
        BsonObjectReader reader = new BsonObjectReader(poolFactory, documentSize, true, byteBufferAllocator);
        reader.setReadBinaryAsByteArray(readBinaryAsByteArray);
        DisposablePool<DynamicByteBuffer> pool = poolFactory.getDisposablePool(
                "codec-buffer-pool", () -> new DynamicByteBuffer(documentSize, true));
        return new Codec(writer, reader, pool);
    }

    public static Codec messagePack(PoolFactory poolFactory, int documentSize) {
        Pool<WriterContext> writerContextPool = poolFactory.getPool("msgpack-writer-context-pool", WriterContext::new);
        Pool<ReaderContext> readerContextPool = poolFactory.getPool("msgpack-reader-context-pool", ReaderContext::new);
        Pool<ArrayDeque<ReaderContext>> stackPool = poolFactory.getPool("msgpack-reader-stack-pool", () -> new ArrayDeque<>(64));
        MessagePackWriter writer = new MessagePackWriter(writerContextPool);
        MessagePackReader reader = new MessagePackReader(readerContextPool, stackPool, true, true);
        DisposablePool<DynamicByteBuffer> pool = poolFactory.getDisposablePool(
                "codec-buffer-pool", () -> new DynamicByteBuffer(documentSize, true));
        return new Codec(writer, reader, pool);
    }

    public DynamicByteBuffer serialize(Object o) {
        BinaryDocument document = writerBinder.unbind(o);
        DynamicByteBuffer buffer = bufferPool.get();
        serializer.serialize(buffer, document);
        return buffer;
    }

    public <T> T deserialize(ByteBuffer buffer, Class<T> tClass) {
        BinaryDocument document = new BinaryDocument(new java.util.HashMap<>());
        deserializer.deserialize(buffer, document);
        return readerBinder.bind(tClass, document);
    }

    public void serialize(Object o, OutputStream outputStream) throws IOException {
        try (DynamicByteBuffer buffer = bufferPool.get()) {
            BinaryDocument document = writerBinder.unbind(o);
            serializer.serialize(buffer, document);
            ByteBuffer raw = buffer.getBuffer();
            byte[] chunk = new byte[8192];
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
