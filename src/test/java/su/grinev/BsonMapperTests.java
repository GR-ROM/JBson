package su.grinev;

import org.junit.jupiter.api.Test;
import su.grinev.bson.BsonObjectReader;
import su.grinev.bson.BsonObjectWriter;
import su.grinev.bson.Document;
import su.grinev.pool.DynamicByteBuffer;
import su.grinev.test.VpnForwardPacketDto;
import su.grinev.test.VpnRequestDto;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static su.grinev.test.Command.FOO;

public class BsonMapperTests {

    @Test
    public void serializeAndDeserializeObjectTest() {
        BsonMapper bsonMapper = new BsonMapper(10, 100, 2048, 512, null);
        bsonMapper.getBsonObjectReader().setReadBinaryAsByteArray(false);
        bsonMapper.getBsonObjectReader().setEnableBufferProjection(true);
        VpnRequestDto<VpnForwardPacketDto> vpnRequestDto = VpnRequestDto.wrap(FOO, VpnForwardPacketDto.builder()
                .packet(ByteBuffer.allocateDirect(1024))
                .build());

        for (int i = 0; i < vpnRequestDto.getData().getPacket().limit(); i++) {
            vpnRequestDto.getData().getPacket().put(i, (byte) i);
        }

        vpnRequestDto.setTimestamp(Instant.ofEpochMilli(Instant.now().toEpochMilli()));

        DynamicByteBuffer b = bsonMapper.serialize(vpnRequestDto);
        b.flip();
        VpnRequestDto<?> deserialized = bsonMapper.deserialize(b.getBuffer(), VpnRequestDto.class);

        b.dispose();
        assertEquals(vpnRequestDto, deserialized);
    }

    @Test
    public void performanceTest() {
        Binder binder = new Binder();

        BsonObjectWriter bsonObjectWriter = new BsonObjectWriter(10, 100, 129 * 1024, true);
        BsonObjectReader bsonObjectReader = new BsonObjectReader( 10, 1000, 1000, 129  * 1024, 128, null);
        bsonObjectReader.setReadBinaryAsByteArray(false);
        bsonObjectReader.setEnableBufferProjection(true);

        VpnRequestDto<VpnForwardPacketDto> requestDto = VpnRequestDto.wrap(FOO, VpnForwardPacketDto.builder()
                .packet(ByteBuffer.allocateDirect(128 * 1024))
                .build());

        List<Long> serializationTime = new ArrayList<>();
        List<Long> deserializationTime = new ArrayList<>();
        Document deserialized = new Document(Map.of(), 0);
        Object request1;

        for (int i = 0; i < 10000; i++) {
            requestDto.getData().getPacket().clear();
            for (int b = 0; b < requestDto.getData().getPacket().limit(); b++) {
                requestDto.getData().getPacket().put(b, (byte) ((byte) b % 128));
            }

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

            for (int j = 0; j < requestDto.getData().getPacket().limit(); j++) {
                assertEquals(requestDto.getData().getPacket().get(j), ((VpnRequestDto<VpnForwardPacketDto>)request1).getData().getPacket().get(j));
            }
        }
        System.out.println(deserialized);

        List<Long> sortedSerialization = serializationTime.stream().sorted().toList();
        List<Long> sortedDeserialization = deserializationTime.stream().sorted().toList();

        System.out.println("Serialization median time: %sus".formatted(Long.toString(sortedSerialization.get(sortedSerialization.size() / 2))));
        System.out.println("Deserialization median time: %sus".formatted(Long.toString(sortedDeserialization.get(sortedDeserialization.size() / 2))));
    }

}
