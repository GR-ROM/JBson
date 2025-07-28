package su.grinev;

import su.grinev.bson.*;
import su.grinev.pool.DisposablePool;
import su.grinev.pool.DynamicByteBuffer;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ObjectMapper {
    public static final int INITIAL_POOL_SIZE = 100;
    public static final int MAX_POOL_SIZE = 100000;
    private final Binder writerBinder = new Binder();
    private final Binder readerBinder = new Binder();
    private final BsonObjectWriter bsonObjectWriter = new BsonObjectWriter(INITIAL_POOL_SIZE, MAX_POOL_SIZE);
    private final BsonObjectReader bsonObjectReader = new BsonObjectReader(INITIAL_POOL_SIZE, MAX_POOL_SIZE);

    public DynamicByteBuffer serialize(Object o) {
        Document document = writerBinder.unbind(o);
        return bsonObjectWriter.serialize(document);
    }

    public void serialize(Object o, OutputStream outputStream) {
        Document document = writerBinder.unbind(o);
        bsonObjectWriter.serialize(document, outputStream);
    }

    public <T> T deserialize(ByteBuffer buffer, Class<T> tClass) {
        Document document = bsonObjectReader.deserialize(buffer);
        return readerBinder.bind(tClass, document);
    }

    public <T> T deserialize(InputStream inputStream, Class<T> tClass) {
        Document document = bsonObjectReader.deserialize(inputStream);
        return readerBinder.bind(tClass, document);
    }
}
