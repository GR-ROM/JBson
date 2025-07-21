package su.grinev;

import su.grinev.bson.BsonReader;
import su.grinev.bson.BsonWriter;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

public class ObjectMapper {
    private final Binder binder = new Binder();
    private final BsonWriter bsonWriter = new BsonWriter();
    private final BsonReader bsonReader = new BsonReader();

    public ByteBuffer serialize(Object o) {
        Map<String, Object> map = binder.unbind(o);

        return bsonWriter.serialize(map);
    }

    public ByteArrayOutputStream serialize(ByteArrayOutputStream outputStream, Object o) {
        return outputStream;
    }

    public <T> T deserialize(ByteBuffer buffer, Class<T> tClass) {
        Map<String, Object> map = bsonReader.deserialize(buffer);

        return binder.bind(tClass, map);
    }

}
