package su.grinev;

import su.grinev.bson.BsonDeserializer;
import su.grinev.bson.BsonWriter;
import su.grinev.bson.Document;
import su.grinev.test.VpnPacket;
import su.grinev.test.VpnRequest;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Main {

    public static byte[] loadJsonFile(String filePath) throws IOException {
        return Files.readAllBytes(Path.of(filePath));
    }

    public static ByteBuffer loadBsonFile(String filePath) throws IOException {
        byte[] data = Files.readAllBytes(Path.of(filePath));

        ByteBuffer buffer = ByteBuffer.allocateDirect(data.length);

        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.put(data);
        return buffer;
    }

    public static void main(String[] args) {
        Binder binder = new Binder();
        BsonWriter bsonWriter = new BsonWriter();
        BsonDeserializer bsonDeserializer = new BsonDeserializer();

        byte[] packet = new byte[128 * 1024];

        List<String> test = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < 1000; i++) {
            test.add(Integer.toString(random.nextInt()));
        }

        VpnRequest<VpnPacket> request = VpnRequest.<VpnPacket>builder()
                .ver("0.1")
                .type(VpnPacket.class.getTypeName())
                .opts(Map.of(
                        "sourceCountry", "kz",
                        "destinationCounty", "ru",
                        "longStringKeyAaBbCcDdEeFfGgHhIiKkLlMmNnOoPpRrSsTtQqXxYyZz", "value"
                        ))
                .reserved(test)
                .data(VpnPacket.builder()
                        .bigDecimal(BigDecimal.ONE)
                        .encoding("RAW")
                        .packet(packet)
                        .build())
                .build();

        List<Long> serializationTime = new ArrayList<>();
        List<Long> deserializationTime = new ArrayList<>();
        Document deserialized = new Document(Map.of(), 0);

        for (int i = 0; i < 100000; i++) {

            Map<String, Object> documentMap = binder.unbind(request);
            long delta = System.nanoTime();
            ByteBuffer b = bsonWriter.serialize(documentMap);
            serializationTime.add((System.nanoTime() - delta) / 1000);

            delta = System.nanoTime();
            deserialized = bsonDeserializer.deserialize(b);
            deserializationTime.add((System.nanoTime() - delta) / 1000);
            Object request1 = binder.bind(VpnRequest.class, deserialized);
        }
        //System.out.println(Arrays.toString(b.array()));
        System.out.println(deserialized);

        List<Long> sortedSerialization = serializationTime.stream().sorted().toList();
        List<Long> sortedDeserialization = deserializationTime.stream().sorted().toList();

        System.out.println("Serialization median time: %sus".formatted(Long.toString(sortedSerialization.get(sortedSerialization.size() / 2))));
        System.out.println("Deserialization median time: %sus".formatted(Long.toString(sortedDeserialization.get(sortedDeserialization.size() / 2))));
    }
}