package su.grinev;

import org.junit.jupiter.api.Test;
import su.grinev.bson.BsonObjectReader;
import su.grinev.bson.BsonObjectWriter;
import su.grinev.dto.BlockingsInfoCacheableDto;
import su.grinev.dto.GetBlockingsInfoResultCacheableDto;
import su.grinev.json.JsonReader;
import su.grinev.json.JsonWriter;
import su.grinev.messagepack.MessagePackReader;
import su.grinev.messagepack.MessagePackWriter;
import su.grinev.messagepack.ReaderContext;
import su.grinev.messagepack.WriterContext;
import su.grinev.pool.DisposablePool;
import su.grinev.pool.DynamicByteBuffer;
import su.grinev.pool.Pool;
import su.grinev.pool.PoolFactory;
import su.grinev.proto.BlockingsProto;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.*;

public class DtoBenchmarkTest {

    @Test
    public void benchmarkBlockingsInfoDto() throws Exception {
        runBenchmark(10, "10 items");
    }

    @Test
    public void benchmarkBlockingsInfoDtoLargeList() throws Exception {
        runBenchmark(100, "100 items");
    }

    private void runBenchmark(int itemCount, String description) throws Exception {
        final int WARMUP = 5000;
        final int ITERATIONS = 20000;

        PoolFactory poolFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(100)
                .setMaxPoolSize(2000)
                .setBlocking(true)
                .setOutOfPoolTimeout(1000)
                .build();

        // Setup JSON (string keys, traditional approach)
        DisposablePool<DynamicByteBuffer> jsonBufferPool =
                poolFactory.getDisposablePool(() -> new DynamicByteBuffer(512 * 1024, true));
        JsonWriter jsonWriter = new JsonWriter(jsonBufferPool);
        JsonReader jsonReader = new JsonReader();

        // Setup BSON (integer keys)
        BsonObjectWriter bsonWriter = new BsonObjectWriter(poolFactory, 512 * 1024, true);
        BsonObjectReader bsonReader = new BsonObjectReader(
                poolFactory, 512 * 1024, true, () -> ByteBuffer.allocateDirect(4096));

        // Setup MessagePack (integer keys)
        Pool<ReaderContext> msgpackReaderCtxPool = poolFactory.getPool(ReaderContext::new);
        Pool<ArrayDeque<ReaderContext>> msgpackStackPool =
                poolFactory.getPool(() -> new ArrayDeque<>(64));
        Pool<WriterContext> msgpackWriterCtxPool = poolFactory.getPool(WriterContext::new);
        DisposablePool<DynamicByteBuffer> msgpackBufferPool =
                poolFactory.getDisposablePool(() -> new DynamicByteBuffer(512 * 1024, true));
        MessagePackWriter msgpackWriter = new MessagePackWriter(msgpackBufferPool, msgpackWriterCtxPool);
        MessagePackReader msgpackReader = new MessagePackReader(
                msgpackReaderCtxPool, msgpackStackPool, true, true);

        // Setup Binder (for BSON/MessagePack int-key POJO mapping)
        Binder binder = new Binder();

        // Create test DTO
        GetBlockingsInfoResultCacheableDto dto = createTestDto(itemCount);

        // JSON: traditional string-keyed Document
        Document jsonDocument = dtoToDocument(dto);

        // BSON / MessagePack: integer-keyed BinaryDocument
        BinaryDocument binaryDocument = dtoToBinaryDocument(dto);

        // Protobuf
        BlockingsProto.GetBlockingsInfoResultCacheableDto protoDto = dtoToProto(dto);

        // Pre-serialize to measure sizes
        DynamicByteBuffer jsonPreBuf = jsonWriter.serialize(jsonDocument);
        jsonPreBuf.flip();
        byte[] jsonBytes = new byte[jsonPreBuf.getBuffer().remaining()];
        jsonPreBuf.getBuffer().get(jsonBytes);
        jsonPreBuf.dispose();

        DynamicByteBuffer bsonPreBuf = bsonWriter.serialize(binaryDocument);
        bsonPreBuf.flip();
        byte[] bsonBytes = new byte[bsonPreBuf.getBuffer().remaining()];
        bsonPreBuf.getBuffer().get(bsonBytes);
        bsonPreBuf.dispose();

        DynamicByteBuffer msgpackPreBuf = msgpackWriter.serialize(binaryDocument);
        msgpackPreBuf.flip();
        byte[] msgpackBytes = new byte[msgpackPreBuf.getBuffer().remaining()];
        msgpackPreBuf.getBuffer().get(msgpackBytes);
        msgpackPreBuf.dispose();

        byte[] protoBytes = protoDto.toByteArray();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(dto);
        oos.close();
        byte[] javaBytes = baos.toByteArray();

        System.out.println("=== GetBlockingsInfoResultCacheableDto Benchmark (" + description + ") ===");
        System.out.println();

        // Print sizes
        System.out.println("=== Serialized Sizes ===");
        System.out.printf("%-15s %10s%n", "Format", "Size");
        System.out.printf("%-15s %10d bytes%n", "JSON", jsonBytes.length);
        System.out.printf("%-15s %10d bytes%n", "BSON", bsonBytes.length);
        System.out.printf("%-15s %10d bytes%n", "MessagePack", msgpackBytes.length);
        System.out.printf("%-15s %10d bytes%n", "Protobuf", protoBytes.length);
        System.out.printf("%-15s %10d bytes%n", "Java native", javaBytes.length);
        System.out.println();

        // Warmup all formats
        for (int i = 0; i < WARMUP; i++) {
            DynamicByteBuffer buf = jsonWriter.serialize(jsonDocument);
            buf.flip();
            byte[] b = new byte[buf.getBuffer().remaining()];
            buf.getBuffer().get(b);
            buf.dispose();
            jsonReader.deserialize(b);

            buf = bsonWriter.serialize(binaryDocument);
            buf.flip();
            bsonReader.deserialize(buf.getBuffer());
            buf.dispose();

            buf = msgpackWriter.serialize(binaryDocument);
            buf.flip();
            msgpackReader.deserialize(buf.getBuffer());
            buf.dispose();

            protoDto.toByteArray();
            BlockingsProto.GetBlockingsInfoResultCacheableDto.parseFrom(protoBytes);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(dto);
            out.close();
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(javaBytes));
            ois.readObject();
            ois.close();
        }

        // ---- Benchmark serialization ----

        List<Long> jsonSerTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            DynamicByteBuffer buf = jsonWriter.serialize(jsonDocument);
            jsonSerTimes.add(System.nanoTime() - start);
            buf.dispose();
        }

        List<Long> bsonSerTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            DynamicByteBuffer buf = bsonWriter.serialize(binaryDocument);
            bsonSerTimes.add(System.nanoTime() - start);
            buf.dispose();
        }

        List<Long> msgpackSerTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            DynamicByteBuffer buf = msgpackWriter.serialize(binaryDocument);
            msgpackSerTimes.add(System.nanoTime() - start);
            buf.dispose();
        }

        List<Long> protoSerTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            protoDto.toByteArray();
            protoSerTimes.add(System.nanoTime() - start);
        }

        List<Long> javaSerTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            long start = System.nanoTime();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(dto);
            out.close();
            javaSerTimes.add(System.nanoTime() - start);
        }

        // ---- Benchmark deserialization ----

        List<Long> jsonDeserTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            jsonReader.deserialize(jsonBytes);
            jsonDeserTimes.add(System.nanoTime() - start);
        }

        List<Long> bsonDeserTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            ByteBuffer buf = ByteBuffer.wrap(bsonBytes);
            long start = System.nanoTime();
            bsonReader.deserialize(buf);
            bsonDeserTimes.add(System.nanoTime() - start);
        }

        List<Long> msgpackDeserTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            ByteBuffer buf = ByteBuffer.wrap(msgpackBytes);
            long start = System.nanoTime();
            msgpackReader.deserialize(buf);
            msgpackDeserTimes.add(System.nanoTime() - start);
        }

        List<Long> protoDeserTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            BlockingsProto.GetBlockingsInfoResultCacheableDto.parseFrom(protoBytes);
            protoDeserTimes.add(System.nanoTime() - start);
        }

        List<Long> javaDeserTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            ByteArrayInputStream bais = new ByteArrayInputStream(javaBytes);
            long start = System.nanoTime();
            ObjectInputStream ois = new ObjectInputStream(bais);
            ois.readObject();
            ois.close();
            javaDeserTimes.add(System.nanoTime() - start);
        }

        // ---- Benchmark full POJO round-trip (Binder + serialization) ----

        // Warmup POJO round-trips
        for (int i = 0; i < WARMUP; i++) {
            BinaryDocument doc = binder.unbind(dto);

            DynamicByteBuffer buf = bsonWriter.serialize(doc);
            buf.flip();
            BinaryDocument deser = bsonReader.deserialize(buf.getBuffer());
            binder.bind(GetBlockingsInfoResultCacheableDto.class, deser);
            buf.dispose();

            buf = msgpackWriter.serialize(doc);
            buf.flip();
            deser = msgpackReader.deserialize(buf.getBuffer());
            binder.bind(GetBlockingsInfoResultCacheableDto.class, deser);
            buf.dispose();
        }

        List<Long> bsonRoundtripTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            BinaryDocument doc = binder.unbind(dto);
            DynamicByteBuffer buf = bsonWriter.serialize(doc);
            buf.flip();
            BinaryDocument deser = bsonReader.deserialize(buf.getBuffer());
            binder.bind(GetBlockingsInfoResultCacheableDto.class, deser);
            bsonRoundtripTimes.add(System.nanoTime() - start);
            buf.dispose();
        }

        List<Long> msgpackRoundtripTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            BinaryDocument doc = binder.unbind(dto);
            DynamicByteBuffer buf = msgpackWriter.serialize(doc);
            buf.flip();
            BinaryDocument deser = msgpackReader.deserialize(buf.getBuffer());
            binder.bind(GetBlockingsInfoResultCacheableDto.class, deser);
            msgpackRoundtripTimes.add(System.nanoTime() - start);
            buf.dispose();
        }

        List<Long> protoRoundtripTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            byte[] pb = dtoToProto(dto).toByteArray();
            BlockingsProto.GetBlockingsInfoResultCacheableDto.parseFrom(pb);
            protoRoundtripTimes.add(System.nanoTime() - start);
        }

        List<Long> javaRoundtripTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            long start = System.nanoTime();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(dto);
            out.close();
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
            ois.readObject();
            ois.close();
            javaRoundtripTimes.add(System.nanoTime() - start);
        }

        // Sort all times for median
        jsonSerTimes.sort(Long::compareTo);
        bsonSerTimes.sort(Long::compareTo);
        msgpackSerTimes.sort(Long::compareTo);
        protoSerTimes.sort(Long::compareTo);
        javaSerTimes.sort(Long::compareTo);
        jsonDeserTimes.sort(Long::compareTo);
        bsonDeserTimes.sort(Long::compareTo);
        msgpackDeserTimes.sort(Long::compareTo);
        protoDeserTimes.sort(Long::compareTo);
        javaDeserTimes.sort(Long::compareTo);
        bsonRoundtripTimes.sort(Long::compareTo);
        msgpackRoundtripTimes.sort(Long::compareTo);
        protoRoundtripTimes.sort(Long::compareTo);
        javaRoundtripTimes.sort(Long::compareTo);

        // Extract medians
        double jsonSer = jsonSerTimes.get(ITERATIONS / 2) / 1000.0;
        double bsonSer = bsonSerTimes.get(ITERATIONS / 2) / 1000.0;
        double msgpackSer = msgpackSerTimes.get(ITERATIONS / 2) / 1000.0;
        double protoSer = protoSerTimes.get(ITERATIONS / 2) / 1000.0;
        double javaSer = javaSerTimes.get(ITERATIONS / 2) / 1000.0;
        double jsonDeser = jsonDeserTimes.get(ITERATIONS / 2) / 1000.0;
        double bsonDeser = bsonDeserTimes.get(ITERATIONS / 2) / 1000.0;
        double msgpackDeser = msgpackDeserTimes.get(ITERATIONS / 2) / 1000.0;
        double protoDeser = protoDeserTimes.get(ITERATIONS / 2) / 1000.0;
        double javaDeser = javaDeserTimes.get(ITERATIONS / 2) / 1000.0;
        double bsonRt = bsonRoundtripTimes.get(ITERATIONS / 2) / 1000.0;
        double msgpackRt = msgpackRoundtripTimes.get(ITERATIONS / 2) / 1000.0;
        double protoRt = protoRoundtripTimes.get(ITERATIONS / 2) / 1000.0;
        double javaRt = javaRoundtripTimes.get(ITERATIONS / 2) / 1000.0;

        // Print raw serialization/deserialization results
        System.out.println("=== Raw Serialization Performance (median, microseconds) ===");
        System.out.printf("%-20s %15s %15s %15s%n", "Format", "Serialize", "Deserialize", "Total");
        System.out.printf("%-20s %12.1f us %12.1f us %12.1f us%n", "JSON (str keys)", jsonSer, jsonDeser, jsonSer + jsonDeser);
        System.out.printf("%-20s %12.1f us %12.1f us %12.1f us%n", "BSON (int keys)", bsonSer, bsonDeser, bsonSer + bsonDeser);
        System.out.printf("%-20s %12.1f us %12.1f us %12.1f us%n", "MsgPack (int keys)", msgpackSer, msgpackDeser, msgpackSer + msgpackDeser);
        System.out.printf("%-20s %12.1f us %12.1f us %12.1f us%n", "Protobuf", protoSer, protoDeser, protoSer + protoDeser);
        System.out.printf("%-20s %12.1f us %12.1f us %12.1f us%n", "Java native", javaSer, javaDeser, javaSer + javaDeser);
        System.out.println();

        // Print full POJO round-trip results
        System.out.println("=== Full POJO Round-Trip: DTO -> Binder.unbind -> serialize -> deserialize -> Binder.bind -> DTO (median, microseconds) ===");
        System.out.printf("%-20s %15s%n", "Format", "Round-trip");
        System.out.printf("%-20s %12.1f us%n", "BSON (int keys)", bsonRt);
        System.out.printf("%-20s %12.1f us%n", "MsgPack (int keys)", msgpackRt);
        System.out.printf("%-20s %12.1f us%n", "Protobuf", protoRt);
        System.out.printf("%-20s %12.1f us%n", "Java native", javaRt);
        System.out.println();

        // Print speedup vs Java native
        System.out.println("=== Speedup vs Java Native Serialization ===");
        System.out.printf("%-20s %15s %15s %15s%n", "Format", "Serialize", "Deserialize", "Total");
        System.out.printf("%-20s %14.1fx %14.1fx %14.1fx%n", "JSON (str keys)",
                javaSer / jsonSer, javaDeser / jsonDeser, (javaSer + javaDeser) / (jsonSer + jsonDeser));
        System.out.printf("%-20s %14.1fx %14.1fx %14.1fx%n", "BSON (int keys)",
                javaSer / bsonSer, javaDeser / bsonDeser, (javaSer + javaDeser) / (bsonSer + bsonDeser));
        System.out.printf("%-20s %14.1fx %14.1fx %14.1fx%n", "MsgPack (int keys)",
                javaSer / msgpackSer, javaDeser / msgpackDeser, (javaSer + javaDeser) / (msgpackSer + msgpackDeser));
        System.out.printf("%-20s %14.1fx %14.1fx %14.1fx%n", "Protobuf",
                javaSer / protoSer, javaDeser / protoDeser, (javaSer + javaDeser) / (protoSer + protoDeser));
    }

    private GetBlockingsInfoResultCacheableDto createTestDto(int blockingsCount) {
        List<BlockingsInfoCacheableDto> blockings = new ArrayList<>();
        for (int i = 0; i < blockingsCount; i++) {
            blockings.add(new BlockingsInfoCacheableDto(
                    i + 1,
                    "2024-01-" + String.format("%02d", (i % 28) + 1),
                    "Federal Tax Service Department #" + (i % 10),
                    "Tax debt collection order #" + (10000 + i),
                    100000L + i * 1000,
                    i % 2 == 0 ? "FULL" : "PARTIAL"
            ));
        }

        return new GetBlockingsInfoResultCacheableDto(
                "CUST-12345678901234",
                "40817810099910004567",
                blockings
        );
    }

    /**
     * JSON traditional approach: string field names as keys.
     */
    private Document dtoToDocument(GetBlockingsInfoResultCacheableDto dto) {
        Map<String, Object> root = new HashMap<>();
        root.put("customerId", dto.getCustomerId());
        root.put("accountNumber", dto.getAccountNumber());

        List<Map<String, Object>> blockingsList = new ArrayList<>();
        for (BlockingsInfoCacheableDto blocking : dto.getBlockingsInfo()) {
            Map<String, Object> map = new HashMap<>();
            map.put("number", blocking.getNumber());
            map.put("date", blocking.getDate());
            map.put("authorityName", blocking.getAuthorityName());
            map.put("blockReason", blocking.getBlockReason());
            map.put("blockAmount", blocking.getBlockAmount());
            map.put("blockType", blocking.getBlockType());
            blockingsList.add(map);
        }
        root.put("blockingsInfo", blockingsList);

        return new Document(root);
    }

    /**
     * BSON / MessagePack approach: integer @Tag values as keys.
     * Tag assignments match the @Tag annotations on the DTOs:
     *   GetBlockingsInfoResultCacheableDto: accountNumber=0, blockingsInfo=1, customerId=2
     *   BlockingsInfoCacheableDto: authorityName=0, blockAmount=1, blockReason=2, blockType=3, date=4, number=5
     */
    private BinaryDocument dtoToBinaryDocument(GetBlockingsInfoResultCacheableDto dto) {
        Map<Integer, Object> root = new HashMap<>();
        root.put(2, dto.getCustomerId());
        root.put(0, dto.getAccountNumber());

        List<Map<Integer, Object>> blockingsList = new ArrayList<>();
        for (BlockingsInfoCacheableDto blocking : dto.getBlockingsInfo()) {
            Map<Integer, Object> map = new HashMap<>();
            map.put(5, blocking.getNumber());
            map.put(4, blocking.getDate());
            map.put(0, blocking.getAuthorityName());
            map.put(2, blocking.getBlockReason());
            map.put(1, blocking.getBlockAmount());
            map.put(3, blocking.getBlockType());
            blockingsList.add(map);
        }
        root.put(1, blockingsList);

        return new BinaryDocument(root);
    }

    private BlockingsProto.GetBlockingsInfoResultCacheableDto dtoToProto(
            GetBlockingsInfoResultCacheableDto dto) {
        BlockingsProto.GetBlockingsInfoResultCacheableDto.Builder builder =
                BlockingsProto.GetBlockingsInfoResultCacheableDto.newBuilder()
                        .setCustomerId(dto.getCustomerId())
                        .setAccountNumber(dto.getAccountNumber());

        for (BlockingsInfoCacheableDto blocking : dto.getBlockingsInfo()) {
            builder.addBlockingsInfo(
                    BlockingsProto.BlockingsInfoCacheableDto.newBuilder()
                            .setNumber(blocking.getNumber())
                            .setDate(blocking.getDate())
                            .setAuthorityName(blocking.getAuthorityName())
                            .setBlockReason(blocking.getBlockReason())
                            .setBlockAmount(blocking.getBlockAmount())
                            .setBlockType(blocking.getBlockType())
                            .build()
            );
        }

        return builder.build();
    }
}
