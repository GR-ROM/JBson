package su.grinev;

import su.grinev.bson.BsonObjectReader;
import su.grinev.bson.BsonObjectWriter;
import su.grinev.bson.Document;
import su.grinev.pool.DynamicByteBuffer;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ObjectMapper {
    public final int initialPoolSize;
    public final int maxPoolSize;
    private final Binder writerBinder = new Binder();
    private final Binder readerBinder = new Binder();
    private final BsonObjectWriter bsonObjectWriter;
    private final BsonObjectReader bsonObjectReader;

    public ObjectMapper(int initialPoolSize, int maxPoolSize) {
        this.initialPoolSize = initialPoolSize;
        this.maxPoolSize = maxPoolSize;
        this.bsonObjectWriter = new BsonObjectWriter(initialPoolSize, maxPoolSize);
        this.bsonObjectReader = new BsonObjectReader(initialPoolSize, maxPoolSize);
    }

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
