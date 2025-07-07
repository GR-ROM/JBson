package su.grinev;

import su.grinev.bson.BsonReader;

public interface Binder {

    void bind(KeyTypeValue keyTypeValue);


    record KeyTypeValue(String key, String type, Object value) {};
}
