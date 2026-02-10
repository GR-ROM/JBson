package su.grinev;

import lombok.Getter;
import su.grinev.bson.BsonObjectReader;
import su.grinev.bson.BsonObjectWriter;
import su.grinev.pool.DisposablePool;
import su.grinev.pool.DynamicByteBuffer;
import su.grinev.pool.PoolFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

@Getter
public class BsonMapper {
    public final PoolFactory poolFactory;
    private final Binder writerBinder = new Binder();
    private final Binder readerBinder = new Binder();
    private final BsonObjectWriter bsonObjectWriter;
    private final BsonObjectReader bsonObjectReader;
    private final DisposablePool<DynamicByteBuffer> bufferPool;

    public BsonMapper(PoolFactory poolFactory, int documentSize, int initialCStringSize, Supplier<ByteBuffer> byteBufferAllocator) {
        this.poolFactory = poolFactory;
        this.bsonObjectWriter = new BsonObjectWriter(poolFactory, documentSize, true);
        this.bsonObjectReader = new BsonObjectReader(poolFactory, documentSize, true, byteBufferAllocator);
        this.bufferPool = poolFactory.getDisposablePool("bson-mapper-buffer-pool", () -> new DynamicByteBuffer(documentSize, true));
    }

    public DynamicByteBuffer serialize(Object o) {
        BinaryDocument document = writerBinder.unbind(o);
        DynamicByteBuffer buffer = bufferPool.get();
        bsonObjectWriter.serialize(buffer, document);
        return buffer;
    }

    public void serialize(Object o, OutputStream outputStream) throws IOException {
        BinaryDocument document = writerBinder.unbind(o);
        bsonObjectWriter.serialize(document, outputStream);
    }

    public <T> T deserialize(ByteBuffer buffer, Class<T> tClass) {
        BinaryDocument document = new BinaryDocument(new java.util.HashMap<>());
        bsonObjectReader.deserialize(buffer, document);
        return readerBinder.bind(tClass, document);
    }

    public <T> T deserialize(InputStream inputStream, Class<T> tClass) throws IOException {
        BinaryDocument document = new BinaryDocument(new java.util.HashMap<>());
        bsonObjectReader.deserialize(inputStream, document);
        return readerBinder.bind(tClass, document);
    }
}
