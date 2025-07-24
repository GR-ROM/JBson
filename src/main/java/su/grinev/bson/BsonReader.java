package su.grinev.bson;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public interface BsonReader extends Position {
    String readString();
    float readFloat();
    double readDouble();
    int readInt();
    long readLong();
    boolean readBoolean();
    byte[] readBinary();
    byte readByte();
    String readObjectId();
    LocalDateTime readDateTime();
    BigDecimal readDecimal128();
    String readCString();
}
