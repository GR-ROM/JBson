package su.grinev;

import org.junit.jupiter.api.Test;
import su.grinev.bson.BsonObjectReader;
import su.grinev.bson.BsonObjectWriter;
import su.grinev.bson.Document;
import su.grinev.json.JsonParser;
import su.grinev.json.JsonReader;
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

import java.util.List;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FormatComparisonTest {

    @Test
    public void compareFormats() {
        PoolFactory poolFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(10)
                .setMaxPoolSize(1000)
                .setBlocking(true)
                .setOutOfPoolTimeout(1000)
                .build();

        // Setup BSON
        BsonObjectWriter bsonWriter = new BsonObjectWriter(poolFactory, 512 * 1024, true);
        BsonObjectReader bsonReader = new BsonObjectReader(poolFactory, 512 * 1024, 128, true, () -> ByteBuffer.allocateDirect(4096));

        // Setup MessagePack with fast pools
        FastPool<byte[]> stringPool = poolFactory.getFastPool(() -> new byte[256]);
        FastPool<ReaderContext> readerContextPool = poolFactory.getFastPool(ReaderContext::new);
        FastPool<ArrayDeque<ReaderContext>> msgpackStackPool = poolFactory.getFastPool(() -> new ArrayDeque<>(64));
        Pool<WriterContext> writerContextPool = poolFactory.getPool(WriterContext::new);
        DisposablePool<DynamicByteBuffer> msgpackBufferPool = poolFactory.getDisposablePool(() -> new DynamicByteBuffer(512 * 1024, true));
        MessagePackWriter msgpackWriter = new MessagePackWriter(msgpackBufferPool, writerContextPool);
        MessagePackReader msgpackReader = new MessagePackReader(stringPool, readerContextPool, msgpackStackPool, true, true);

        // Setup JSON
        DisposablePool<DynamicByteBuffer> jsonBufferPool = poolFactory.getDisposablePool(() -> new DynamicByteBuffer(512 * 1024, true));
        JsonWriter jsonWriter = new JsonWriter(jsonBufferPool);
        JsonParser jsonParser = new JsonParser();

        // Create test data - 1000 nested objects
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

        // Serialize with each format
        DynamicByteBuffer bsonBuffer = bsonWriter.serialize(document);
        bsonBuffer.flip();
        int bsonSize = bsonBuffer.getBuffer().remaining();

        DynamicByteBuffer msgpackBuffer = msgpackWriter.serialize(document);
        msgpackBuffer.flip();
        int msgpackSize = msgpackBuffer.getBuffer().remaining();

        DynamicByteBuffer jsonBuffer = jsonWriter.serialize(document);
        jsonBuffer.flip();
        int jsonSize = jsonBuffer.getBuffer().remaining();

        System.out.println("=== Payload Size Comparison (1000 nested objects) ===");
        System.out.println("BSON:        " + bsonSize + " bytes");
        System.out.println("MessagePack: " + msgpackSize + " bytes");
        System.out.println("JSON:        " + jsonSize + " bytes");
        System.out.println();
        System.out.println("MessagePack is " + String.format("%.1f%%", (1.0 - (double) msgpackSize / bsonSize) * 100) + " smaller than BSON");
        System.out.println("JSON is " + String.format("%.1f%%", ((double) jsonSize / bsonSize - 1.0) * 100) + " larger than BSON");

        // Deserialize and verify each format
        Document bsonDoc = bsonReader.deserialize(bsonBuffer.getBuffer());
        assertEquals(1000, bsonDoc.getDocumentMap().size());

        Document msgpackDoc = msgpackReader.deserialize(msgpackBuffer.getBuffer());
        assertEquals(1000, msgpackDoc.getDocumentMap().size());

        byte[] jsonBytes = new byte[jsonSize];
        jsonBuffer.getBuffer().get(jsonBytes);
        List<Token> tokens = new Tokenizer(jsonBytes).tokenize();
        Document jsonDoc = jsonParser.parse(tokens);
        assertEquals(1000, jsonDoc.getDocumentMap().size());

        // Verify a sample field from each
        @SuppressWarnings("unchecked")
        Map<String, Object> bsonField = (Map<String, Object>) bsonDoc.getDocumentMap().get("field_500");
        @SuppressWarnings("unchecked")
        Map<String, Object> msgpackField = (Map<String, Object>) msgpackDoc.getDocumentMap().get("field_500");
        @SuppressWarnings("unchecked")
        Map<String, Object> jsonField = (Map<String, Object>) jsonDoc.getDocumentMap().get("field_500");

        assertEquals(500, ((Number) bsonField.get("id")).intValue());
        assertEquals(500, ((Number) msgpackField.get("id")).intValue());
        assertEquals(500, ((Number) jsonField.get("id")).intValue());

        assertEquals("item_500", bsonField.get("name"));
        assertEquals("item_500", msgpackField.get("name"));
        assertEquals("item_500", jsonField.get("name"));

        System.out.println("\nAll formats verified correctly!");

        bsonBuffer.dispose();
        msgpackBuffer.dispose();
        jsonBuffer.dispose();
    }

    @Test
    public void comparePerformanceWithPooling() {
        final int WARMUP_ITERATIONS = 5000;
        final int ITERATIONS = 10000;

        PoolFactory poolFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(100)
                .setMaxPoolSize(2000)
                .setBlocking(true)
                .setOutOfPoolTimeout(1000)
                .build();

        // Setup BSON with pools
        BsonObjectWriter bsonWriter = new BsonObjectWriter(poolFactory, 512 * 1024, true);
        BsonObjectReader bsonReader = new BsonObjectReader(poolFactory, 512 * 1024, 128, true, () -> ByteBuffer.allocateDirect(4096));

        // Setup MessagePack with fast pools
        FastPool<byte[]> msgpackStringPool = poolFactory.getFastPool(() -> new byte[256]);
        FastPool<ReaderContext> msgpackReaderContextPool = poolFactory.getFastPool(ReaderContext::new);
        FastPool<ArrayDeque<ReaderContext>> msgpackStackPool = poolFactory.getFastPool(() -> new ArrayDeque<>(64));
        Pool<WriterContext> msgpackWriterContextPool = poolFactory.getPool(WriterContext::new);
        DisposablePool<DynamicByteBuffer> msgpackBufferPool = poolFactory.getDisposablePool(() -> new DynamicByteBuffer(512 * 1024, true));
        MessagePackWriter msgpackWriter = new MessagePackWriter(msgpackBufferPool, msgpackWriterContextPool);
        MessagePackReader msgpackReader = new MessagePackReader(msgpackStringPool, msgpackReaderContextPool, msgpackStackPool, true, true);

        // Setup JSON with pooled buffer
        DisposablePool<DynamicByteBuffer> jsonBufferPool = poolFactory.getDisposablePool(() -> new DynamicByteBuffer(512 * 1024, true));
        JsonWriter jsonWriter = new JsonWriter(jsonBufferPool);
        JsonReader jsonReader = new JsonReader();

        // Create test data - 1000 nested objects
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
        DynamicByteBuffer bsonPreBuffer = bsonWriter.serialize(document);
        bsonPreBuffer.flip();
        byte[] bsonBytes = new byte[bsonPreBuffer.getBuffer().remaining()];
        bsonPreBuffer.getBuffer().get(bsonBytes);
        bsonPreBuffer.dispose();

        DynamicByteBuffer msgpackPreBuffer = msgpackWriter.serialize(document);
        msgpackPreBuffer.flip();
        byte[] msgpackBytes = new byte[msgpackPreBuffer.getBuffer().remaining()];
        msgpackPreBuffer.getBuffer().get(msgpackBytes);
        msgpackPreBuffer.dispose();

        DynamicByteBuffer jsonPreBuffer = jsonWriter.serialize(document);
        jsonPreBuffer.flip();
        byte[] jsonBytes = new byte[jsonPreBuffer.getBuffer().remaining()];
        jsonPreBuffer.getBuffer().get(jsonBytes);
        jsonPreBuffer.dispose();

        System.out.println("=== Performance Comparison (1000 nested objects) ===");
        System.out.println("Payload sizes: BSON=" + bsonBytes.length + ", MsgPack=" + msgpackBytes.length + ", JSON=" + jsonBytes.length);
        System.out.println();

        // Warmup all paths
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            // BSON
            DynamicByteBuffer buf = bsonWriter.serialize(document);
            buf.flip();
            bsonReader.deserialize(buf.getBuffer());
            buf.dispose();

            // MessagePack
            buf = msgpackWriter.serialize(document);
            buf.flip();
            msgpackReader.deserialize(buf.getBuffer());
            buf.dispose();

            // JSON
            buf = jsonWriter.serialize(document);
            buf.flip();
            byte[] jb = new byte[buf.getBuffer().remaining()];
            buf.getBuffer().get(jb);
            jsonReader.deserialize(jb);
            buf.dispose();
        }

        // === SERIALIZATION BENCHMARKS ===
        List<Long> bsonSerTimes = new ArrayList<>();
        List<Long> msgpackSerTimes = new ArrayList<>();
        List<Long> jsonSerTimes = new ArrayList<>();

        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            DynamicByteBuffer buf = bsonWriter.serialize(document);
            buf.flip();
            bsonSerTimes.add(System.nanoTime() - start);
            buf.dispose();

            start = System.nanoTime();
            buf = msgpackWriter.serialize(document);
            buf.flip();
            msgpackSerTimes.add(System.nanoTime() - start);
            buf.dispose();

            start = System.nanoTime();
            buf = jsonWriter.serialize(document);
            buf.flip();
            jsonSerTimes.add(System.nanoTime() - start);
            buf.dispose();
        }

        bsonSerTimes.sort(Long::compareTo);
        msgpackSerTimes.sort(Long::compareTo);
        jsonSerTimes.sort(Long::compareTo);

        System.out.println("=== SERIALIZATION (median, microseconds) ===");
        System.out.printf("BSON:        %.1f us%n", bsonSerTimes.get(ITERATIONS / 2) / 1000.0);
        System.out.printf("MessagePack: %.1f us%n", msgpackSerTimes.get(ITERATIONS / 2) / 1000.0);
        System.out.printf("JSON:        %.1f us%n", jsonSerTimes.get(ITERATIONS / 2) / 1000.0);
        System.out.println();

        // === DESERIALIZATION BENCHMARKS ===
        List<Long> bsonDeserTimes = new ArrayList<>();
        List<Long> msgpackDeserTimes = new ArrayList<>();
        List<Long> jsonDeserTimes = new ArrayList<>();

        for (int i = 0; i < ITERATIONS; i++) {
            ByteBuffer bsonBuf = ByteBuffer.wrap(bsonBytes);
            long start = System.nanoTime();
            bsonReader.deserialize(bsonBuf);
            bsonDeserTimes.add(System.nanoTime() - start);

            ByteBuffer msgpackBuf = ByteBuffer.wrap(msgpackBytes);
            start = System.nanoTime();
            msgpackReader.deserialize(msgpackBuf);
            msgpackDeserTimes.add(System.nanoTime() - start);

            start = System.nanoTime();
            jsonReader.deserialize(jsonBytes);
            jsonDeserTimes.add(System.nanoTime() - start);
        }

        bsonDeserTimes.sort(Long::compareTo);
        msgpackDeserTimes.sort(Long::compareTo);
        jsonDeserTimes.sort(Long::compareTo);

        System.out.println("=== DESERIALIZATION (median, microseconds) ===");
        System.out.printf("BSON:        %.1f us%n", bsonDeserTimes.get(ITERATIONS / 2) / 1000.0);
        System.out.printf("MessagePack: %.1f us%n", msgpackDeserTimes.get(ITERATIONS / 2) / 1000.0);
        System.out.printf("JSON:        %.1f us%n", jsonDeserTimes.get(ITERATIONS / 2) / 1000.0);
        System.out.println();

        // === ROUNDTRIP BENCHMARKS ===
        List<Long> bsonRoundtripTimes = new ArrayList<>();
        List<Long> msgpackRoundtripTimes = new ArrayList<>();
        List<Long> jsonRoundtripTimes = new ArrayList<>();

        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            DynamicByteBuffer buf = bsonWriter.serialize(document);
            buf.flip();
            bsonReader.deserialize(buf.getBuffer());
            bsonRoundtripTimes.add(System.nanoTime() - start);
            buf.dispose();

            start = System.nanoTime();
            buf = msgpackWriter.serialize(document);
            buf.flip();
            msgpackReader.deserialize(buf.getBuffer());
            msgpackRoundtripTimes.add(System.nanoTime() - start);
            buf.dispose();

            start = System.nanoTime();
            buf = jsonWriter.serialize(document);
            buf.flip();
            byte[] jb = new byte[buf.getBuffer().remaining()];
            buf.getBuffer().get(jb);
            jsonReader.deserialize(jb);
            jsonRoundtripTimes.add(System.nanoTime() - start);
            buf.dispose();
        }

        bsonRoundtripTimes.sort(Long::compareTo);
        msgpackRoundtripTimes.sort(Long::compareTo);
        jsonRoundtripTimes.sort(Long::compareTo);

        System.out.println("=== ROUNDTRIP (median, microseconds) ===");
        System.out.printf("BSON:        %.1f us%n", bsonRoundtripTimes.get(ITERATIONS / 2) / 1000.0);
        System.out.printf("MessagePack: %.1f us%n", msgpackRoundtripTimes.get(ITERATIONS / 2) / 1000.0);
        System.out.printf("JSON:        %.1f us%n", jsonRoundtripTimes.get(ITERATIONS / 2) / 1000.0);
    }
}
