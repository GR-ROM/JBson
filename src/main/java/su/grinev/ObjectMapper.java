package su.grinev;

import su.grinev.bson.BsonDeserializer;
import su.grinev.bson.BsonWriter;
import su.grinev.bson.Document;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class ObjectMapper {
    private final Binder binder = new Binder();
    private final BsonWriter bsonWriter = new BsonWriter();
    private final BsonDeserializer bsonDeserializer = new BsonDeserializer();

    public ByteBuffer serialize(Object o) {
        Document document = binder.unbind(o);
        return bsonWriter.serialize(document);
    }

    public ByteArrayOutputStream serialize(ByteArrayOutputStream outputStream, Object o) {
        return outputStream;
    }

    public <T> T deserialize(ByteBuffer buffer, Class<T> tClass) {
        Document document = bsonDeserializer.deserialize(buffer);
        return binder.bind(tClass, document);
    }

}
