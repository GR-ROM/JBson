package su.grinev;

import org.junit.jupiter.api.Test;
import su.grinev.bson.BsonObjectReader;
import su.grinev.bson.BsonObjectWriter;
import su.grinev.bson.Document;
import su.grinev.pool.DynamicByteBuffer;
import su.grinev.pool.PoolFactory;
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
        PoolFactory poolFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(100)
                .setMaxPoolSize(1000)
                .setOutOfPoolTimeout(1000)
                .setBlocking(true)
                .build();

        BsonMapper bsonMapper = new BsonMapper(poolFactory, 4096, 64, () -> ByteBuffer.allocateDirect(4096));
        bsonMapper.getBsonObjectReader().setReadBinaryAsByteArray(false);
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
        final int WARMUP_ITERATIONS = 5000;
        final int BENCHMARK_ITERATIONS = 10000;

        Binder binder = new Binder();
        PoolFactory poolFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(10)
                .setMaxPoolSize(1000)
                .setBlocking(true)
                .setOutOfPoolTimeout(1000)
                .build();

        BsonObjectWriter bsonObjectWriter = new BsonObjectWriter(poolFactory, 129 * 1024, true);
        BsonObjectReader bsonObjectReader = new BsonObjectReader(poolFactory, 129  * 1024, 128, true, () -> ByteBuffer.allocateDirect(4096));
        bsonObjectReader.setReadBinaryAsByteArray(false);

        VpnRequestDto<VpnForwardPacketDto> requestDto = VpnRequestDto.wrap(FOO, VpnForwardPacketDto.builder()
                .packet(ByteBuffer.allocateDirect(128 * 1024))
                .build());

        // Warm-up phase: allow JIT to optimize hot paths
        System.out.println("Warming up for " + WARMUP_ITERATIONS + " iterations...");
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            requestDto.getData().getPacket().clear();
            for (int b = 0; b < requestDto.getData().getPacket().limit(); b++) {
                requestDto.getData().getPacket().put(b, (byte) ((byte) b % 128));
            }

            Document documentMap = binder.unbind(requestDto);
            DynamicByteBuffer b = bsonObjectWriter.serialize(documentMap);
            b.flip();
            Document deserialized = bsonObjectReader.deserialize(b.getBuffer());
            b.dispose();
            binder.bind(VpnRequestDto.class, deserialized);
        }
        System.out.println("Warm-up complete. Running benchmark...");

        // Benchmark phase
        List<Long> serializationTime = new ArrayList<>();
        List<Long> deserializationTime = new ArrayList<>();
        Document deserialized = new Document(Map.of(), 0);
        Object request1;

        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            requestDto.getData().getPacket().clear();
            for (int b = 0; b < requestDto.getData().getPacket().limit(); b++) {
                requestDto.getData().getPacket().put(b, (byte) ((byte) b % 128));
            }

            Document documentMap = binder.unbind(requestDto);
            long delta = System.nanoTime();
            DynamicByteBuffer b = bsonObjectWriter.serialize(documentMap);
            serializationTime.add(System.nanoTime() - delta);
            b.flip();
            delta = System.nanoTime();
            deserialized = bsonObjectReader.deserialize(b.getBuffer());
            b.dispose();
            deserializationTime.add(System.nanoTime() - delta);
            request1 = binder.bind(VpnRequestDto.class, deserialized);

            for (int j = 0; j < requestDto.getData().getPacket().limit(); j++) {
                assertEquals(requestDto.getData().getPacket().get(j), ((VpnRequestDto<VpnForwardPacketDto>)request1).getData().getPacket().get(j));
            }
        }
        System.out.println(deserialized);

        List<Long> sortedSerialization = serializationTime.stream().sorted().toList();
        List<Long> sortedDeserialization = deserializationTime.stream().sorted().toList();

        long serMedian = sortedSerialization.get(sortedSerialization.size() / 2);
        long deserMedian = sortedDeserialization.get(sortedDeserialization.size() / 2);

        System.out.println("Serialization median time: %.3fus".formatted(serMedian / 1000.0));
        System.out.println("Deserialization median time: %.3fus".formatted(deserMedian / 1000.0));
    }

}
