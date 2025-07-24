package su.grinev;

import org.junit.jupiter.api.Test;
import su.grinev.bson.BsonDeserializer;
import su.grinev.bson.BsonWriter;
import su.grinev.bson.Document;
import su.grinev.test.TestTest;
import su.grinev.test.VpnPacket;
import su.grinev.test.VpnRequest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ObjectMapperTests {

    @Test
    public void serializeAndDeserializeObjectTest() {
        ObjectMapper objectMapper = new ObjectMapper();

        byte[] b1 = new byte[100];
        byte[] b2 = new byte[100];

        for (int i = 0; i < 100; i++) {
            b1[i] = (byte) i;
            b2[i] = (byte) i;
        }

        TestTest nested = new TestTest();
        nested.setB(b2);
        nested.setS("TestString");
        nested.setD(0.111111111);
        nested.setI(1111111111);

        TestTest testTest = new TestTest();
        testTest.setB(b1);
        testTest.setS("TestString");
        testTest.setD(0.111111111);
        testTest.setI(1111111111);
        testTest.setNestedTestTest(nested);

        ByteBuffer b = objectMapper.serialize(testTest);

        TestTest deserialized = objectMapper.deserialize(b, TestTest.class);

        assertEquals(testTest, deserialized);
    }

    @Test
    public void performanceTest() {
        Binder binder = new Binder();
        BsonWriter bsonWriter = new BsonWriter(10, 1000, 10000);
        BsonDeserializer bsonDeserializer = new BsonDeserializer(10, 1000, 10000);

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


        for (int i = 0; i < 10000; i++) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(100000);
            Document documentMap = binder.unbind(request);
            long delta = System.nanoTime();
            bsonWriter.serialize(documentMap, outputStream);
            serializationTime.add((System.nanoTime() - delta) / 1000);
            byte[] data = outputStream.toByteArray();
            InputStream inputStream = new ByteArrayInputStream(data);

            delta = System.nanoTime();
            deserialized = bsonDeserializer.deserialize(inputStream);
            deserializationTime.add((System.nanoTime() - delta) / 1000);
            Object request1 = binder.bind(VpnRequest.class, deserialized);
        }
        System.out.println(deserialized);

        List<Long> sortedSerialization = serializationTime.stream().sorted().toList();
        List<Long> sortedDeserialization = deserializationTime.stream().sorted().toList();

        System.out.println("Serialization median time: %sus".formatted(Long.toString(sortedSerialization.get(sortedSerialization.size() / 2))));
        System.out.println("Deserialization median time: %sus".formatted(Long.toString(sortedDeserialization.get(sortedDeserialization.size() / 2))));
    }

}
