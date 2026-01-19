package su.grinev;

import lombok.Getter;
import su.grinev.bson.BsonObjectReader;
import su.grinev.bson.BsonObjectWriter;
import su.grinev.bson.Document;
import su.grinev.pool.DynamicByteBuffer;
import su.grinev.pool.Pool;
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

    public BsonMapper(PoolFactory poolFactory, int documentSize, int initialCStringSize, Supplier<ByteBuffer> byteBufferAllocator) {
        this.poolFactory = poolFactory;
        this.bsonObjectWriter = new BsonObjectWriter(poolFactory, documentSize, true);
        this.bsonObjectReader = new BsonObjectReader(poolFactory, documentSize, initialCStringSize, byteBufferAllocator);
    }

    public DynamicByteBuffer serialize(Object o) {
        Document document = writerBinder.unbind(o);
        return bsonObjectWriter.serialize(document);
    }

    public void serialize(Object o, OutputStream outputStream) throws IOException {
        Document document = writerBinder.unbind(o);
        bsonObjectWriter.serialize(document, outputStream);
    }

    public <T> T deserialize(ByteBuffer buffer, Class<T> tClass) {
        Document document = bsonObjectReader.deserialize(buffer);
        return readerBinder.bind(tClass, document);
    }

    public <T> T deserialize(InputStream inputStream, Class<T> tClass) throws IOException {
        Document document = bsonObjectReader.deserialize(inputStream);
        return readerBinder.bind(tClass, document);
    }
}
