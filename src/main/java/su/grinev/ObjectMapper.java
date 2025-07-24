package su.grinev;

import su.grinev.bson.BsonDeserializer;
import su.grinev.bson.BsonWriter;
import su.grinev.bson.Document;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ObjectMapper {
    public static final int CONCURRENCY_LEVEL = 1000;
    public static final int INITIAL_POOL_SIZE = 1000;
    public static final int MAX_POOL_SIZE = 100000;
    private final Binder binder = new Binder();
    private final BsonWriter bsonWriter = new BsonWriter(CONCURRENCY_LEVEL, INITIAL_POOL_SIZE, MAX_POOL_SIZE);
    private final BsonDeserializer bsonDeserializer = new BsonDeserializer(CONCURRENCY_LEVEL, INITIAL_POOL_SIZE, MAX_POOL_SIZE);

    public ByteBuffer serialize(Object o) {
        Document document = binder.unbind(o);
        return bsonWriter.serialize(document);
    }

    public void serialize(Object o, OutputStream outputStream) {
        Document document = binder.unbind(o);
        bsonWriter.serialize(document, outputStream);
    }

    public <T> T deserialize(ByteBuffer buffer, Class<T> tClass) {
        Document document = bsonDeserializer.deserialize(buffer);
        return binder.bind(tClass, document);
    }

    public <T> T deserialize(InputStream inputStream, Class<T> tClass) {
        Document document = bsonDeserializer.deserialize(inputStream);
        return binder.bind(tClass, document);
    }
}
