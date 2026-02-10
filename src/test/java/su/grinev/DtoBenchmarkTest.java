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
import su.grinev.proto.PayloadProto;
import su.grinev.test.VpnForwardPacketDto;
import su.grinev.test.VpnRequestDto;

import static su.grinev.test.Command.FOO;

import com.google.protobuf.ByteString;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class DtoBenchmarkTest {

    @Test
    public void benchmarkBlockingsInfoDto() throws Exception {
        runBenchmark(10, "10 items");
    }

    @Test
    public void benchmarkBlockingsInfoDtoLargeList() throws Exception {
        runBenchmark(100, "100 items");
    }

    @Test
    public void benchmarkIpPacket1500b() throws Exception {
        final int WARMUP = 5000;
        final int ITERATIONS = 20000;
        final int PACKET_SIZE = 1500;

        PoolFactory poolFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(100)
                .setMaxPoolSize(2000)
                .setBlocking(true)
                .setOutOfPoolTimeout(1000)
                .build();

        // Setup JSON
        DisposablePool<DynamicByteBuffer> jsonBufferPool =
                poolFactory.getDisposablePool(() -> new DynamicByteBuffer(4096, true));
        JsonWriter jsonWriter = new JsonWriter(jsonBufferPool);
        JsonReader jsonReader = new JsonReader();

        // Setup BSON
        BsonObjectWriter bsonWriter = new BsonObjectWriter(poolFactory, 4096, true);
        BsonObjectReader bsonReader = new BsonObjectReader(
                poolFactory, 4096, true, () -> ByteBuffer.allocateDirect(4096));
        bsonReader.setReadBinaryAsByteArray(false);

        // Setup MessagePack
        Pool<ReaderContext> mpReaderCtxPool = poolFactory.getPool(ReaderContext::new);
        Pool<ArrayDeque<ReaderContext>> mpStackPool =
                poolFactory.getPool(() -> new ArrayDeque<>(64));
        Pool<WriterContext> mpWriterCtxPool = poolFactory.getPool(WriterContext::new);
        DisposablePool<DynamicByteBuffer> mpBufferPool =
                poolFactory.getDisposablePool(() -> new DynamicByteBuffer(4096, true));
        MessagePackWriter msgpackWriter = new MessagePackWriter(mpBufferPool, mpWriterCtxPool);
        MessagePackReader msgpackReader = new MessagePackReader(
                mpReaderCtxPool, mpStackPool, true, true);

        Binder binder = new Binder();

        // Create 1500b IP packet with pseudo-random payload
        ByteBuffer packetBuf = ByteBuffer.allocateDirect(PACKET_SIZE);
        byte[] randomPayload = new byte[PACKET_SIZE];
        ThreadLocalRandom.current().nextBytes(randomPayload);
        packetBuf.put(randomPayload);
        packetBuf.flip();

        VpnRequestDto<VpnForwardPacketDto> dto = VpnRequestDto.wrap(FOO,
                VpnForwardPacketDto.builder().packet(packetBuf).build());
        dto.setTimestamp(null);

        // JSON document (string keys, base64 for binary)
        String base64Payload = Base64.getEncoder().encodeToString(randomPayload);
        Map<String, Object> jsonPacketMap = new HashMap<>();
        jsonPacketMap.put("packet", base64Payload);
        Map<String, Object> jsonRootMap = new HashMap<>();
        jsonRootMap.put("command", "FOO");
        jsonRootMap.put("protocolVersion", "0.1");
        jsonRootMap.put("data", jsonPacketMap);
        Document jsonDocument = new Document(jsonRootMap);

        // Protobuf
        PayloadProto.PayloadRequest protoDto = PayloadProto.PayloadRequest.newBuilder()
                .setCommand("FOO")
                .setData(PayloadProto.PayloadData.newBuilder()
                        .setPacket(ByteString.copyFrom(randomPayload))
                        .build())
                .build();

        // Pre-serialize to measure sizes
        BinaryDocument binaryDoc = binder.unbind(dto);

        DynamicByteBuffer jsonPreBuf = jsonWriter.serialize(jsonDocument);
        jsonPreBuf.flip();
        byte[] jsonBytes = new byte[jsonPreBuf.getBuffer().remaining()];
        jsonPreBuf.getBuffer().get(jsonBytes);
        jsonPreBuf.dispose();

        DynamicByteBuffer bsonPreBuf = bsonWriter.serialize(binaryDoc);
        bsonPreBuf.flip();
        byte[] bsonBytes = new byte[bsonPreBuf.getBuffer().remaining()];
        bsonPreBuf.getBuffer().get(bsonBytes);
        bsonPreBuf.dispose();

        DynamicByteBuffer mpPreBuf = msgpackWriter.serialize(binaryDoc);
        mpPreBuf.flip();
        byte[] msgpackBytes = new byte[mpPreBuf.getBuffer().remaining()];
        mpPreBuf.getBuffer().get(msgpackBytes);
        mpPreBuf.dispose();

        byte[] protoBytes = protoDto.toByteArray();

        System.out.println("=== IP Packet Benchmark (1500b pseudo-random payload) ===");
        System.out.println();

        System.out.println("=== Serialized Sizes ===");
        System.out.printf("%-20s %10s%n", "Format", "Size");
        System.out.printf("%-20s %10d bytes%n", "JSON (base64)", jsonBytes.length);
        System.out.printf("%-20s %10d bytes%n", "BSON", bsonBytes.length);
        System.out.printf("%-20s %10d bytes%n", "MessagePack", msgpackBytes.length);
        System.out.printf("%-20s %10d bytes%n", "Protobuf", protoBytes.length);
        System.out.println();

        // Warmup
        for (int i = 0; i < WARMUP; i++) {
            DynamicByteBuffer buf = jsonWriter.serialize(jsonDocument);
            buf.flip();
            byte[] b = new byte[buf.getBuffer().remaining()];
            buf.getBuffer().get(b);
            buf.dispose();
            jsonReader.deserialize(b);

            bsonReader.deserialize(ByteBuffer.wrap(bsonBytes), new BinaryDocument(new HashMap<>()));
            msgpackReader.deserialize(ByteBuffer.wrap(msgpackBytes), new BinaryDocument(new HashMap<>()));

            protoDto.toByteArray();
            PayloadProto.PayloadRequest.parseFrom(protoBytes);

            packetBuf.rewind();
            BinaryDocument doc = binder.unbind(dto);
            buf = bsonWriter.serialize(doc);
            buf.flip();
            bsonReader.deserialize(buf.getBuffer(), new BinaryDocument(new HashMap<>()));
            buf.dispose();

            packetBuf.rewind();
            doc = binder.unbind(dto);
            buf = msgpackWriter.serialize(doc);
            buf.flip();
            msgpackReader.deserialize(buf.getBuffer(), new BinaryDocument(new HashMap<>()));
            buf.dispose();
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
            packetBuf.rewind();
            BinaryDocument doc = binder.unbind(dto);
            long start = System.nanoTime();
            DynamicByteBuffer buf = bsonWriter.serialize(doc);
            bsonSerTimes.add(System.nanoTime() - start);
            buf.dispose();
        }

        List<Long> msgpackSerTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            packetBuf.rewind();
            BinaryDocument doc = binder.unbind(dto);
            long start = System.nanoTime();
            DynamicByteBuffer buf = msgpackWriter.serialize(doc);
            msgpackSerTimes.add(System.nanoTime() - start);
            buf.dispose();
        }

        List<Long> protoSerTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            protoDto.toByteArray();
            protoSerTimes.add(System.nanoTime() - start);
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
            long start = System.nanoTime();
            bsonReader.deserialize(ByteBuffer.wrap(bsonBytes), new BinaryDocument(new HashMap<>()));
            bsonDeserTimes.add(System.nanoTime() - start);
        }

        List<Long> msgpackDeserTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            msgpackReader.deserialize(ByteBuffer.wrap(msgpackBytes), new BinaryDocument(new HashMap<>()));
            msgpackDeserTimes.add(System.nanoTime() - start);
        }

        List<Long> protoDeserTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            PayloadProto.PayloadRequest.parseFrom(protoBytes);
            protoDeserTimes.add(System.nanoTime() - start);
        }

        // Sort for median
        jsonSerTimes.sort(Long::compareTo);
        bsonSerTimes.sort(Long::compareTo);
        msgpackSerTimes.sort(Long::compareTo);
        protoSerTimes.sort(Long::compareTo);
        jsonDeserTimes.sort(Long::compareTo);
        bsonDeserTimes.sort(Long::compareTo);
        msgpackDeserTimes.sort(Long::compareTo);
        protoDeserTimes.sort(Long::compareTo);

        double jsonSer = jsonSerTimes.get(ITERATIONS / 2) / 1000.0;
        double bsonSer = bsonSerTimes.get(ITERATIONS / 2) / 1000.0;
        double mpSer = msgpackSerTimes.get(ITERATIONS / 2) / 1000.0;
        double protoSer = protoSerTimes.get(ITERATIONS / 2) / 1000.0;
        double jsonDeser = jsonDeserTimes.get(ITERATIONS / 2) / 1000.0;
        double bsonDeser = bsonDeserTimes.get(ITERATIONS / 2) / 1000.0;
        double mpDeser = msgpackDeserTimes.get(ITERATIONS / 2) / 1000.0;
        double protoDeser = protoDeserTimes.get(ITERATIONS / 2) / 1000.0;

        // Throughput from raw serialize+deserialize total
        double jsonTotal = jsonSer + jsonDeser;
        double bsonTotal = bsonSer + bsonDeser;
        double mpTotal = mpSer + mpDeser;
        double protoTotal = protoSer + protoDeser;
        double jsonPps = 1_000_000.0 / jsonTotal;
        double bsonPps = 1_000_000.0 / bsonTotal;
        double mpPps = 1_000_000.0 / mpTotal;
        double protoPps = 1_000_000.0 / protoTotal;
        double jsonGbps = jsonPps * PACKET_SIZE * 8 / 1_000_000_000.0;
        double bsonGbps = bsonPps * PACKET_SIZE * 8 / 1_000_000_000.0;
        double mpGbps = mpPps * PACKET_SIZE * 8 / 1_000_000_000.0;
        double protoGbps = protoPps * PACKET_SIZE * 8 / 1_000_000_000.0;

        System.out.println("=== Raw Serialization Performance (median, microseconds) ===");
        System.out.printf("%-20s %15s %15s %15s %15s %15s%n", "Format", "Serialize", "Deserialize", "Total", "Packets/sec", "Throughput");
        System.out.printf("%-20s %12.1f us %12.1f us %12.1f us %,13.0f %11.1f Gbps%n", "JSON (base64)", jsonSer, jsonDeser, jsonTotal, jsonPps, jsonGbps);
        System.out.printf("%-20s %12.1f us %12.1f us %12.1f us %,13.0f %11.1f Gbps%n", "BSON (int keys)", bsonSer, bsonDeser, bsonTotal, bsonPps, bsonGbps);
        System.out.printf("%-20s %12.1f us %12.1f us %12.1f us %,13.0f %11.1f Gbps%n", "MsgPack (int keys)", mpSer, mpDeser, mpTotal, mpPps, mpGbps);
        System.out.printf("%-20s %12.1f us %12.1f us %12.1f us %,13.0f %11.1f Gbps%n", "Protobuf", protoSer, protoDeser, protoTotal, protoPps, protoGbps);
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

        // Setup LZ4
        LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
        LZ4Compressor lz4Compressor = lz4Factory.fastCompressor();
        LZ4FastDecompressor lz4Decompressor = lz4Factory.fastDecompressor();

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

        // LZ4-compressed variants
        int lz4BsonMaxLen = lz4Compressor.maxCompressedLength(bsonBytes.length);
        byte[] lz4BsonBytes = new byte[lz4BsonMaxLen];
        int lz4BsonLen = lz4Compressor.compress(bsonBytes, 0, bsonBytes.length, lz4BsonBytes, 0, lz4BsonMaxLen);

        int lz4MsgpackMaxLen = lz4Compressor.maxCompressedLength(msgpackBytes.length);
        byte[] lz4MsgpackBytes = new byte[lz4MsgpackMaxLen];
        int lz4MsgpackLen = lz4Compressor.compress(msgpackBytes, 0, msgpackBytes.length, lz4MsgpackBytes, 0, lz4MsgpackMaxLen);

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
        System.out.printf("%-15s %10d bytes%n", "LZ4(BSON)", lz4BsonLen);
        System.out.printf("%-15s %10d bytes%n", "MessagePack", msgpackBytes.length);
        System.out.printf("%-15s %10d bytes%n", "LZ4(MsgPack)", lz4MsgpackLen);
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
            bsonReader.deserialize(buf.getBuffer(), new BinaryDocument(new HashMap<>()));
            buf.dispose();

            buf = msgpackWriter.serialize(binaryDocument);
            buf.flip();
            msgpackReader.deserialize(buf.getBuffer(), new BinaryDocument(new HashMap<>()));
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

            // LZ4(BSON) warmup
            buf = bsonWriter.serialize(binaryDocument);
            buf.flip();
            byte[] rawBson = new byte[buf.getBuffer().remaining()];
            buf.getBuffer().get(rawBson);
            buf.dispose();
            byte[] compressed = new byte[lz4Compressor.maxCompressedLength(rawBson.length)];
            int cLen = lz4Compressor.compress(rawBson, 0, rawBson.length, compressed, 0, compressed.length);
            byte[] decompressed = new byte[rawBson.length];
            lz4Decompressor.decompress(compressed, 0, decompressed, 0, rawBson.length);
            bsonReader.deserialize(ByteBuffer.wrap(decompressed), new BinaryDocument(new HashMap<>()));

            // LZ4(MsgPack) warmup
            buf = msgpackWriter.serialize(binaryDocument);
            buf.flip();
            byte[] rawMsgpack = new byte[buf.getBuffer().remaining()];
            buf.getBuffer().get(rawMsgpack);
            buf.dispose();
            compressed = new byte[lz4Compressor.maxCompressedLength(rawMsgpack.length)];
            cLen = lz4Compressor.compress(rawMsgpack, 0, rawMsgpack.length, compressed, 0, compressed.length);
            decompressed = new byte[rawMsgpack.length];
            lz4Decompressor.decompress(compressed, 0, decompressed, 0, rawMsgpack.length);
            msgpackReader.deserialize(ByteBuffer.wrap(decompressed), new BinaryDocument(new HashMap<>()));
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

        List<Long> lz4BsonSerTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            DynamicByteBuffer buf = bsonWriter.serialize(binaryDocument);
            buf.flip();
            int len = buf.getBuffer().remaining();
            byte[] raw = new byte[len];
            buf.getBuffer().get(raw);
            byte[] dest = new byte[lz4Compressor.maxCompressedLength(len)];
            lz4Compressor.compress(raw, 0, len, dest, 0, dest.length);
            lz4BsonSerTimes.add(System.nanoTime() - start);
            buf.dispose();
        }

        List<Long> lz4MsgpackSerTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            DynamicByteBuffer buf = msgpackWriter.serialize(binaryDocument);
            buf.flip();
            int len = buf.getBuffer().remaining();
            byte[] raw = new byte[len];
            buf.getBuffer().get(raw);
            byte[] dest = new byte[lz4Compressor.maxCompressedLength(len)];
            lz4Compressor.compress(raw, 0, len, dest, 0, dest.length);
            lz4MsgpackSerTimes.add(System.nanoTime() - start);
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
            bsonReader.deserialize(buf, new BinaryDocument(new HashMap<>()));
            bsonDeserTimes.add(System.nanoTime() - start);
        }

        List<Long> msgpackDeserTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            ByteBuffer buf = ByteBuffer.wrap(msgpackBytes);
            long start = System.nanoTime();
            msgpackReader.deserialize(buf, new BinaryDocument(new HashMap<>()));
            msgpackDeserTimes.add(System.nanoTime() - start);
        }

        // For LZ4 deser, use the pre-compressed bytes
        byte[] lz4BsonCompressed = Arrays.copyOf(lz4BsonBytes, lz4BsonLen);
        int lz4BsonOrigLen = bsonBytes.length;

        List<Long> lz4BsonDeserTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            byte[] decompressed = new byte[lz4BsonOrigLen];
            lz4Decompressor.decompress(lz4BsonCompressed, 0, decompressed, 0, lz4BsonOrigLen);
            bsonReader.deserialize(ByteBuffer.wrap(decompressed), new BinaryDocument(new HashMap<>()));
            lz4BsonDeserTimes.add(System.nanoTime() - start);
        }

        byte[] lz4MsgpackCompressed = Arrays.copyOf(lz4MsgpackBytes, lz4MsgpackLen);
        int lz4MsgpackOrigLen = msgpackBytes.length;

        List<Long> lz4MsgpackDeserTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            byte[] decompressed = new byte[lz4MsgpackOrigLen];
            lz4Decompressor.decompress(lz4MsgpackCompressed, 0, decompressed, 0, lz4MsgpackOrigLen);
            msgpackReader.deserialize(ByteBuffer.wrap(decompressed), new BinaryDocument(new HashMap<>()));
            lz4MsgpackDeserTimes.add(System.nanoTime() - start);
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
            BinaryDocument deser = new BinaryDocument(new HashMap<>());
            bsonReader.deserialize(buf.getBuffer(), deser);
            binder.bind(GetBlockingsInfoResultCacheableDto.class, deser);
            buf.dispose();

            buf = msgpackWriter.serialize(doc);
            buf.flip();
            deser = new BinaryDocument(new HashMap<>());
            msgpackReader.deserialize(buf.getBuffer(), deser);
            binder.bind(GetBlockingsInfoResultCacheableDto.class, deser);
            buf.dispose();
        }

        List<Long> bsonRoundtripTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            BinaryDocument doc = binder.unbind(dto);
            DynamicByteBuffer buf = bsonWriter.serialize(doc);
            buf.flip();
            BinaryDocument deser = new BinaryDocument(new HashMap<>());
            bsonReader.deserialize(buf.getBuffer(), deser);
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
            BinaryDocument deser = new BinaryDocument(new HashMap<>());
            msgpackReader.deserialize(buf.getBuffer(), deser);
            binder.bind(GetBlockingsInfoResultCacheableDto.class, deser);
            msgpackRoundtripTimes.add(System.nanoTime() - start);
            buf.dispose();
        }

        List<Long> lz4BsonRoundtripTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            BinaryDocument doc = binder.unbind(dto);
            DynamicByteBuffer buf = bsonWriter.serialize(doc);
            buf.flip();
            int len = buf.getBuffer().remaining();
            byte[] raw = new byte[len];
            buf.getBuffer().get(raw);
            byte[] cdest = new byte[lz4Compressor.maxCompressedLength(len)];
            int clen = lz4Compressor.compress(raw, 0, len, cdest, 0, cdest.length);
            byte[] decompressed = new byte[len];
            lz4Decompressor.decompress(cdest, 0, decompressed, 0, len);
            BinaryDocument deser = new BinaryDocument(new HashMap<>());
            bsonReader.deserialize(ByteBuffer.wrap(decompressed), deser);
            binder.bind(GetBlockingsInfoResultCacheableDto.class, deser);
            lz4BsonRoundtripTimes.add(System.nanoTime() - start);
            buf.dispose();
        }

        List<Long> lz4MsgpackRoundtripTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            BinaryDocument doc = binder.unbind(dto);
            DynamicByteBuffer buf = msgpackWriter.serialize(doc);
            buf.flip();
            int len = buf.getBuffer().remaining();
            byte[] raw = new byte[len];
            buf.getBuffer().get(raw);
            byte[] cdest = new byte[lz4Compressor.maxCompressedLength(len)];
            int clen = lz4Compressor.compress(raw, 0, len, cdest, 0, cdest.length);
            byte[] decompressed = new byte[len];
            lz4Decompressor.decompress(cdest, 0, decompressed, 0, len);
            BinaryDocument deser = new BinaryDocument(new HashMap<>());
            msgpackReader.deserialize(ByteBuffer.wrap(decompressed), deser);
            binder.bind(GetBlockingsInfoResultCacheableDto.class, deser);
            lz4MsgpackRoundtripTimes.add(System.nanoTime() - start);
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
        lz4BsonSerTimes.sort(Long::compareTo);
        msgpackSerTimes.sort(Long::compareTo);
        lz4MsgpackSerTimes.sort(Long::compareTo);
        protoSerTimes.sort(Long::compareTo);
        javaSerTimes.sort(Long::compareTo);
        jsonDeserTimes.sort(Long::compareTo);
        bsonDeserTimes.sort(Long::compareTo);
        lz4BsonDeserTimes.sort(Long::compareTo);
        msgpackDeserTimes.sort(Long::compareTo);
        lz4MsgpackDeserTimes.sort(Long::compareTo);
        protoDeserTimes.sort(Long::compareTo);
        javaDeserTimes.sort(Long::compareTo);
        bsonRoundtripTimes.sort(Long::compareTo);
        lz4BsonRoundtripTimes.sort(Long::compareTo);
        msgpackRoundtripTimes.sort(Long::compareTo);
        lz4MsgpackRoundtripTimes.sort(Long::compareTo);
        protoRoundtripTimes.sort(Long::compareTo);
        javaRoundtripTimes.sort(Long::compareTo);

        // Extract medians
        double jsonSer = jsonSerTimes.get(ITERATIONS / 2) / 1000.0;
        double bsonSer = bsonSerTimes.get(ITERATIONS / 2) / 1000.0;
        double lz4BsonSer = lz4BsonSerTimes.get(ITERATIONS / 2) / 1000.0;
        double msgpackSer = msgpackSerTimes.get(ITERATIONS / 2) / 1000.0;
        double lz4MsgpackSer = lz4MsgpackSerTimes.get(ITERATIONS / 2) / 1000.0;
        double protoSer = protoSerTimes.get(ITERATIONS / 2) / 1000.0;
        double javaSer = javaSerTimes.get(ITERATIONS / 2) / 1000.0;
        double jsonDeser = jsonDeserTimes.get(ITERATIONS / 2) / 1000.0;
        double bsonDeser = bsonDeserTimes.get(ITERATIONS / 2) / 1000.0;
        double lz4BsonDeser = lz4BsonDeserTimes.get(ITERATIONS / 2) / 1000.0;
        double msgpackDeser = msgpackDeserTimes.get(ITERATIONS / 2) / 1000.0;
        double lz4MsgpackDeser = lz4MsgpackDeserTimes.get(ITERATIONS / 2) / 1000.0;
        double protoDeser = protoDeserTimes.get(ITERATIONS / 2) / 1000.0;
        double javaDeser = javaDeserTimes.get(ITERATIONS / 2) / 1000.0;
        double bsonRt = bsonRoundtripTimes.get(ITERATIONS / 2) / 1000.0;
        double lz4BsonRt = lz4BsonRoundtripTimes.get(ITERATIONS / 2) / 1000.0;
        double msgpackRt = msgpackRoundtripTimes.get(ITERATIONS / 2) / 1000.0;
        double lz4MsgpackRt = lz4MsgpackRoundtripTimes.get(ITERATIONS / 2) / 1000.0;
        double protoRt = protoRoundtripTimes.get(ITERATIONS / 2) / 1000.0;
        double javaRt = javaRoundtripTimes.get(ITERATIONS / 2) / 1000.0;

        // Print raw serialization/deserialization results
        System.out.println("=== Raw Serialization Performance (median, microseconds) ===");
        System.out.printf("%-20s %15s %15s %15s%n", "Format", "Serialize", "Deserialize", "Total");
        System.out.printf("%-20s %12.1f us %12.1f us %12.1f us%n", "JSON (str keys)", jsonSer, jsonDeser, jsonSer + jsonDeser);
        System.out.printf("%-20s %12.1f us %12.1f us %12.1f us%n", "BSON (int keys)", bsonSer, bsonDeser, bsonSer + bsonDeser);
        System.out.printf("%-20s %12.1f us %12.1f us %12.1f us%n", "LZ4(BSON)", lz4BsonSer, lz4BsonDeser, lz4BsonSer + lz4BsonDeser);
        System.out.printf("%-20s %12.1f us %12.1f us %12.1f us%n", "MsgPack (int keys)", msgpackSer, msgpackDeser, msgpackSer + msgpackDeser);
        System.out.printf("%-20s %12.1f us %12.1f us %12.1f us%n", "LZ4(MsgPack)", lz4MsgpackSer, lz4MsgpackDeser, lz4MsgpackSer + lz4MsgpackDeser);
        System.out.printf("%-20s %12.1f us %12.1f us %12.1f us%n", "Protobuf", protoSer, protoDeser, protoSer + protoDeser);
        System.out.printf("%-20s %12.1f us %12.1f us %12.1f us%n", "Java native", javaSer, javaDeser, javaSer + javaDeser);
        System.out.println();

        // Print full POJO round-trip results
        System.out.println("=== Full POJO Round-Trip: DTO -> Binder.unbind -> serialize -> deserialize -> Binder.bind -> DTO (median, microseconds) ===");
        System.out.printf("%-20s %15s%n", "Format", "Round-trip");
        System.out.printf("%-20s %12.1f us%n", "BSON (int keys)", bsonRt);
        System.out.printf("%-20s %12.1f us%n", "LZ4(BSON)", lz4BsonRt);
        System.out.printf("%-20s %12.1f us%n", "MsgPack (int keys)", msgpackRt);
        System.out.printf("%-20s %12.1f us%n", "LZ4(MsgPack)", lz4MsgpackRt);
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
        System.out.printf("%-20s %14.1fx %14.1fx %14.1fx%n", "LZ4(BSON)",
                javaSer / lz4BsonSer, javaDeser / lz4BsonDeser, (javaSer + javaDeser) / (lz4BsonSer + lz4BsonDeser));
        System.out.printf("%-20s %14.1fx %14.1fx %14.1fx%n", "MsgPack (int keys)",
                javaSer / msgpackSer, javaDeser / msgpackDeser, (javaSer + javaDeser) / (msgpackSer + msgpackDeser));
        System.out.printf("%-20s %14.1fx %14.1fx %14.1fx%n", "LZ4(MsgPack)",
                javaSer / lz4MsgpackSer, javaDeser / lz4MsgpackDeser, (javaSer + javaDeser) / (lz4MsgpackSer + lz4MsgpackDeser));
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
