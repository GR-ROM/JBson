package su.grinev.test;

import annotation.BsonType;

import java.util.Map;

public class VpnRequest<T> {
    String ver;
    String type;
    Map<String, String> opts;
    @BsonType(discriminator = "type")
    T data;
}


