package su.grinev;

import org.junit.jupiter.api.Test;
import su.grinev.bson.BsonObjectReader;
import su.grinev.bson.BsonObjectWriter;
import su.grinev.bson.Document;
import su.grinev.json.JsonParser;
import su.grinev.json.JsonWriter;
import su.grinev.json.Tokenizer;
import su.grinev.json.token.Token;
import su.grinev.messagepack.MessagePackReader;
import su.grinev.messagepack.MessagePackWriter;
import su.grinev.messagepack.ReaderContext;
import su.grinev.messagepack.WriterContext;
import su.grinev.pool.DisposablePool;
import su.grinev.pool.DynamicByteBuffer;
import su.grinev.pool.FastPool;
import su.grinev.pool.Pool;
import su.grinev.pool.PoolFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PerformanceInvestigation {

    @Test
    public void investigateJsonPerformance() {
        final int ITERATIONS = 10000;

        PoolFactory poolFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(100)
                .setMaxPoolSize(2000)
                .setBlocking(true)
                .setOutOfPoolTimeout(1000)
                .build();

        DisposablePool<DynamicByteBuffer> bufferPool = poolFactory.getDisposablePool(() -> new DynamicByteBuffer(512 * 1024, true));
        JsonWriter writer = new JsonWriter(bufferPool);
        JsonParser parser = new JsonParser();

        // Create test data
        Map<String, Object> fields = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            Map<String, Object> nested = new HashMap<>();
            nested.put("id", i);
            nested.put("name", "item_" + i);
            nested.put("active", i % 2 == 0);
            nested.put("score", i * 1.5);
            fields.put("field_" + i, nested);
        }
        Document document = new Document(fields);

        // Pre-serialize for deserialization tests
        DynamicByteBuffer preBuffer = writer.serialize(document);
        preBuffer.flip();
        byte[] jsonBytes = new byte[preBuffer.getBuffer().remaining()];
        preBuffer.getBuffer().get(jsonBytes);
        preBuffer.dispose();

        // Warmup
        for (int i = 0; i < 5000; i++) {
            List<Token> tokens = new Tokenizer(jsonBytes).tokenize();
            parser.parse(tokens);
        }

        // Measure tokenization vs parsing separately
        List<Long> tokenizeTimes = new ArrayList<>();
        List<Long> parseTimes = new ArrayList<>();
        List<Long> totalTimes = new ArrayList<>();

        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            List<Token> tokens = new Tokenizer(jsonBytes).tokenize();
            long afterTokenize = System.nanoTime();
            parser.parse(tokens);
            long afterParse = System.nanoTime();

            tokenizeTimes.add(afterTokenize - start);
            parseTimes.add(afterParse - afterTokenize);
            totalTimes.add(afterParse - start);
        }

        tokenizeTimes.sort(Long::compareTo);
        parseTimes.sort(Long::compareTo);
        totalTimes.sort(Long::compareTo);

        System.out.println("=== JSON Deserialization Breakdown ===");
        System.out.println("Tokenization median: %.3fus".formatted(tokenizeTimes.get(ITERATIONS / 2) / 1000.0));
        System.out.println("Parsing median:      %.3fus".formatted(parseTimes.get(ITERATIONS / 2) / 1000.0));
        System.out.println("Total median:        %.3fus".formatted(totalTimes.get(ITERATIONS / 2) / 1000.0));
    }

    @Test
    public void investigateBsonPerformance() {
        final int ITERATIONS = 10000;

        PoolFactory poolFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(100)
                .setMaxPoolSize(2000)
                .setBlocking(true)
                .setOutOfPoolTimeout(1000)
                .build();

        BsonObjectWriter bsonWriter = new BsonObjectWriter(poolFactory, 512 * 1024, true);
        BsonObjectReader bsonReader = new BsonObjectReader(poolFactory, 512 * 1024, 128, true, () -> ByteBuffer.allocateDirect(4096));

        // Create test data
        Map<String, Object> fields = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            Map<String, Object> nested = new HashMap<>();
            nested.put("id", i);
            nested.put("name", "item_" + i);
            nested.put("active", i % 2 == 0);
            nested.put("score", i * 1.5);
            fields.put("field_" + i, nested);
        }
        Document document = new Document(fields);

        // Pre-serialize
        DynamicByteBuffer preBuffer = bsonWriter.serialize(document);
        preBuffer.flip();
        byte[] bsonBytes = new byte[preBuffer.getBuffer().remaining()];
        preBuffer.getBuffer().get(bsonBytes);
        preBuffer.dispose();

        // Warmup
        for (int i = 0; i < 5000; i++) {
            ByteBuffer buf = ByteBuffer.wrap(bsonBytes);
            bsonReader.deserialize(buf);
        }

        // Measure
        List<Long> deserTimes = new ArrayList<>();
        List<Long> wrapTimes = new ArrayList<>();

        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            ByteBuffer buf = ByteBuffer.wrap(bsonBytes);
            long afterWrap = System.nanoTime();
            bsonReader.deserialize(buf);
            long afterDeser = System.nanoTime();

            wrapTimes.add(afterWrap - start);
            deserTimes.add(afterDeser - afterWrap);
        }

        wrapTimes.sort(Long::compareTo);
        deserTimes.sort(Long::compareTo);

        System.out.println("=== BSON Deserialization Breakdown ===");
        System.out.println("ByteBuffer.wrap median: %.3fus".formatted(wrapTimes.get(ITERATIONS / 2) / 1000.0));
        System.out.println("Deserialize median:     %.3fus".formatted(deserTimes.get(ITERATIONS / 2) / 1000.0));
        System.out.println("Total median:           %.3fus".formatted((wrapTimes.get(ITERATIONS / 2) + deserTimes.get(ITERATIONS / 2)) / 1000.0));
    }

    @Test
    public void investigateMessagePackPerformance() {
        final int ITERATIONS = 10000;

        PoolFactory poolFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(100)
                .setMaxPoolSize(2000)
                .setBlocking(true)
                .setOutOfPoolTimeout(1000)
                .build();

        FastPool<byte[]> stringPool = poolFactory.getFastPool(() -> new byte[256]);
        FastPool<ReaderContext> readerContextPool = poolFactory.getFastPool(ReaderContext::new);
        FastPool<ArrayDeque<ReaderContext>> stackPool = poolFactory.getFastPool(() -> new ArrayDeque<>(64));
        Pool<WriterContext> writerContextPool = poolFactory.getPool(WriterContext::new);
        DisposablePool<DynamicByteBuffer> bufferPool = poolFactory.getDisposablePool(() -> new DynamicByteBuffer(512 * 1024, true));

        MessagePackWriter writer = new MessagePackWriter(bufferPool, writerContextPool);
        MessagePackReader reader = new MessagePackReader(stringPool, readerContextPool, stackPool, true, true);

        // Create test data
        Map<String, Object> fields = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            Map<String, Object> nested = new HashMap<>();
            nested.put("id", i);
            nested.put("name", "item_" + i);
            nested.put("active", i % 2 == 0);
            nested.put("score", i * 1.5);
            fields.put("field_" + i, nested);
        }
        Document document = new Document(fields);

        // Pre-serialize
        DynamicByteBuffer preBuffer = writer.serialize(document);
        preBuffer.flip();
        byte[] msgpackBytes = new byte[preBuffer.getBuffer().remaining()];
        preBuffer.getBuffer().get(msgpackBytes);
        preBuffer.dispose();
        bufferPool.release(preBuffer);

        // Warmup
        for (int i = 0; i < 5000; i++) {
            ByteBuffer buf = ByteBuffer.wrap(msgpackBytes);
            reader.deserialize(buf);
        }

        // Measure
        List<Long> deserTimes = new ArrayList<>();

        for (int i = 0; i < ITERATIONS; i++) {
            ByteBuffer buf = ByteBuffer.wrap(msgpackBytes);
            long start = System.nanoTime();
            reader.deserialize(buf);
            deserTimes.add(System.nanoTime() - start);
        }

        deserTimes.sort(Long::compareTo);

        System.out.println("=== MessagePack Deserialization ===");
        System.out.println("Deserialize median: %.3fus".formatted(deserTimes.get(ITERATIONS / 2) / 1000.0));
    }

    @Test
    public void measurePoolOverhead() {
        final int ITERATIONS = 10000;

        PoolFactory poolFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(100)
                .setMaxPoolSize(2000)
                .setBlocking(true)
                .setOutOfPoolTimeout(1000)
                .build();

        Pool<byte[]> pool = poolFactory.getPool(() -> new byte[128]);

        // Warmup
        for (int i = 0; i < 5000; i++) {
            for (int j = 0; j < 2000; j++) {
                byte[] buf = pool.get();
                pool.release(buf);
            }
        }

        // Measure 2000 pool get/release operations (simulating string reads)
        List<Long> times = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            for (int j = 0; j < 2000; j++) {
                byte[] buf = pool.get();
                pool.release(buf);
            }
            times.add(System.nanoTime() - start);
        }

        times.sort(Long::compareTo);
        System.out.println("=== Pool Overhead ===");
        System.out.println("2000 pool get/release pairs median: %.3fus".formatted(times.get(ITERATIONS / 2) / 1000.0));
        System.out.println("This is the overhead for reading ~2000 strings in BSON/MsgPack");
    }

    @Test
    public void measureStringCreationOverhead() {
        final int ITERATIONS = 10000;
        byte[] testData = "item_500".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        // Warmup
        for (int i = 0; i < 5000; i++) {
            for (int j = 0; j < 2000; j++) {
                new String(testData, 0, testData.length, java.nio.charset.StandardCharsets.UTF_8);
            }
        }

        // Measure 2000 string creations
        List<Long> times = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            for (int j = 0; j < 2000; j++) {
                new String(testData, 0, testData.length, java.nio.charset.StandardCharsets.UTF_8);
            }
            times.add(System.nanoTime() - start);
        }

        times.sort(Long::compareTo);
        System.out.println("=== String Creation Overhead ===");
        System.out.println("2000 String creations from bytes median: %.3fus".formatted(times.get(ITERATIONS / 2) / 1000.0));
    }

    @Test
    public void measureByteBufferOverhead() {
        final int ITERATIONS = 10000;
        byte[] data = new byte[70000]; // Similar to BSON size
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // Warmup
        for (int i = 0; i < 5000; i++) {
            buffer.position(0);
            for (int j = 0; j < 5000; j++) {
                buffer.get();
                buffer.getInt();
            }
        }

        // Measure ByteBuffer operations (simulating BSON reads)
        List<Long> times = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.position(0);
            long start = System.nanoTime();
            for (int j = 0; j < 5000; j++) {
                buffer.get();  // type byte
                buffer.getInt(); // some int
            }
            times.add(System.nanoTime() - start);
        }

        times.sort(Long::compareTo);
        System.out.println("=== ByteBuffer Overhead ===");
        System.out.println("5000 get+getInt operations median: %.3fus".formatted(times.get(ITERATIONS / 2) / 1000.0));
    }

    @Test
    public void compareObjectCreationOverhead() {
        final int ITERATIONS = 10000;

        // Warmup
        for (int i = 0; i < 5000; i++) {
            Map<String, Object> map = new HashMap<>();
            for (int j = 0; j < 1000; j++) {
                Map<String, Object> nested = new HashMap<>();
                nested.put("id", j);
                nested.put("name", "item_" + j);
                nested.put("active", j % 2 == 0);
                nested.put("score", j * 1.5);
                map.put("field_" + j, nested);
            }
        }

        // Measure just HashMap creation
        List<Long> times = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            Map<String, Object> map = new HashMap<>();
            for (int j = 0; j < 1000; j++) {
                Map<String, Object> nested = new HashMap<>();
                nested.put("id", j);
                nested.put("name", "item_" + j);
                nested.put("active", j % 2 == 0);
                nested.put("score", j * 1.5);
                map.put("field_" + j, nested);
            }
            times.add(System.nanoTime() - start);
        }

        times.sort(Long::compareTo);
        System.out.println("=== Object Creation Baseline ===");
        System.out.println("Creating 1000 nested HashMaps median: %.3fus".formatted(times.get(ITERATIONS / 2) / 1000.0));
    }
}
