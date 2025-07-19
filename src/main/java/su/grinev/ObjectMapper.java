package su.grinev;

import su.grinev.bson.BsonReader;
import su.grinev.bson.BsonWriter;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class ObjectMapper {
    private final Binder binder = new Binder();

    public ByteBuffer serialize(Object o) {
        BsonWriter bsonWriter = new BsonWriter();
        return bsonWriter.serialize(binder.unbind(o));
    }

    public ByteArrayOutputStream serialize(ByteArrayOutputStream outputStream, Object o) {
        BsonWriter bsonWriter = new BsonWriter();
        return outputStream;
    }

    public <T> T deserialize(ByteBuffer buffer, Class<T> tClass) {
        BsonReader bsonReader = new BsonReader();
        return binder.bind(tClass, bsonReader.deserialize(buffer));
    }

}
