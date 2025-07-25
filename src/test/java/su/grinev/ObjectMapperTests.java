package su.grinev;

import org.junit.jupiter.api.Test;
import su.grinev.test.Command;
import su.grinev.test.VpnForwardPacketDto;
import su.grinev.test.VpnRequestDto;

import java.nio.ByteBuffer;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ObjectMapperTests {

    @Test
    public void serializeAndDeserializeObjectTest() {
        ObjectMapper objectMapper = new ObjectMapper();

        VpnRequestDto vpnRequestDto = VpnRequestDto.wrap(Command.FOO, VpnForwardPacketDto.builder()
                        .packet(new byte[1024])
                .build());

        vpnRequestDto.setTimestamp(Instant.ofEpochMilli(Instant.now().toEpochMilli()));

        ByteBuffer b = objectMapper.serialize(vpnRequestDto);

        VpnRequestDto<?> deserialized = objectMapper.deserialize(b, VpnRequestDto.class);

        assertEquals(vpnRequestDto, deserialized);
    }

//    @Test
//    public void performanceTest() {
//        Binder binder = new Binder();
//        BsonWriter bsonWriter = new BsonWriter(10, 1000, 10000);
//        BsonDeserializer bsonDeserializer = new BsonDeserializer(10, 1000, 10000);
//
//        byte[] packet = new byte[128 * 1024];
//
//        List<String> test = new ArrayList<>();
//        Random random = new Random();
//
//        for (int i = 0; i < 1000; i++) {
//            test.add(Integer.toString(random.nextInt()));
//        }
//
//        List<Long> serializationTime = new ArrayList<>();
//        List<Long> deserializationTime = new ArrayList<>();
//        Document deserialized = new Document(Map.of(), 0);
//
//
//        for (int i = 0; i < 10000; i++) {
//            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(100000);
//            Document documentMap = binder.unbind(request);
//            long delta = System.nanoTime();
//            bsonWriter.serialize(documentMap, outputStream);
//            serializationTime.add((System.nanoTime() - delta) / 1000);
//            byte[] data = outputStream.toByteArray();
//            InputStream inputStream = new ByteArrayInputStream(data);
//
//            delta = System.nanoTime();
//            deserialized = bsonDeserializer.deserialize(inputStream);
//            deserializationTime.add((System.nanoTime() - delta) / 1000);
//            Object request1 = binder.bind(VpnRequest.class, deserialized);
//        }
//        System.out.println(deserialized);
//
//        List<Long> sortedSerialization = serializationTime.stream().sorted().toList();
//        List<Long> sortedDeserialization = deserializationTime.stream().sorted().toList();
//
//        System.out.println("Serialization median time: %sus".formatted(Long.toString(sortedSerialization.get(sortedSerialization.size() / 2))));
//        System.out.println("Deserialization median time: %sus".formatted(Long.toString(sortedDeserialization.get(sortedDeserialization.size() / 2))));
//    }

}
