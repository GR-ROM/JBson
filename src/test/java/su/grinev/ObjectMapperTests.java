package su.grinev;

import org.junit.jupiter.api.Test;
import su.grinev.bson.BsonObjectReader;
import su.grinev.bson.BsonObjectWriter;
import su.grinev.bson.Document;
import su.grinev.pool.DynamicByteBuffer;
import su.grinev.test.VpnForwardPacketDto;
import su.grinev.test.VpnRequestDto;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static su.grinev.test.Command.FOO;

public class ObjectMapperTests {

    @Test
    public void serializeAndDeserializeObjectTest() {
        ObjectMapper objectMapper = new ObjectMapper(10, 100);

        VpnRequestDto vpnRequestDto = VpnRequestDto.wrap(FOO, VpnForwardPacketDto.builder()
                        .packet(new byte[1024])
                .build());

        vpnRequestDto.setTimestamp(Instant.ofEpochMilli(Instant.now().toEpochMilli()));

        DynamicByteBuffer b = objectMapper.serialize(vpnRequestDto);
        b.flip();
        VpnRequestDto<?> deserialized = objectMapper.deserialize(b.getBuffer(), VpnRequestDto.class);

        b.dispose();
        assertEquals(vpnRequestDto, deserialized);
    }

    @Test
    public void performanceTest() {
        Binder binder = new Binder();
        BsonObjectWriter bsonObjectWriter = new BsonObjectWriter(10, 1000);
        BsonObjectReader bsonObjectReader = new BsonObjectReader( 10, 1000);

        byte[] packet = new byte[128 * 1024];

        List<String> test = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < 1000; i++) {
            test.add(Integer.toString(random.nextInt()));
        }

        VpnRequestDto<VpnForwardPacketDto> requestDto = VpnRequestDto.wrap(FOO, VpnForwardPacketDto.builder()
                .packet(packet)
                .build());

        List<Long> serializationTime = new ArrayList<>();
        List<Long> deserializationTime = new ArrayList<>();
        Document deserialized = new Document(Map.of(), 0);
        Object request1 = null;

        for (int i = 0; i < 10000; i++) {
            Document documentMap = binder.unbind(requestDto);
            long delta = System.nanoTime();
            DynamicByteBuffer b = bsonObjectWriter.serialize(documentMap);
            serializationTime.add((System.nanoTime() - delta) / 1000);
            b.flip();
            delta = System.nanoTime();
            deserialized = bsonObjectReader.deserialize(b.getBuffer());
            b.dispose();
            deserializationTime.add((System.nanoTime() - delta) / 1000);
            request1 = binder.bind(VpnRequestDto.class, deserialized);
        }
        System.out.println(deserialized);

        List<Long> sortedSerialization = serializationTime.stream().sorted().toList();
        List<Long> sortedDeserialization = deserializationTime.stream().sorted().toList();

        System.out.println("Serialization median time: %sus".formatted(Long.toString(sortedSerialization.get(sortedSerialization.size() / 2))));
        System.out.println("Deserialization median time: %sus".formatted(Long.toString(sortedDeserialization.get(sortedDeserialization.size() / 2))));
    }

}
