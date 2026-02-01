package su.grinev;

import org.junit.jupiter.api.Test;
import su.grinev.bson.Document;
import su.grinev.messagepack.*;
import su.grinev.pool.DisposablePool;
import su.grinev.pool.DynamicByteBuffer;
import su.grinev.pool.FastPool;
import su.grinev.pool.Pool;
import su.grinev.pool.PoolFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class MessagePackTest {

    private final PoolFactory poolFactory = PoolFactory.Builder.builder()
            .setMinPoolSize(1)
            .setMaxPoolSize(10)
            .setOutOfPoolTimeout(1000)
            .setBlocking(false)
            .build();

    private final FastPool<byte[]> stringPool = poolFactory.getFastPool(() -> new byte[128]);
    private final FastPool<ReaderContext> readerContextPool = poolFactory.getFastPool(ReaderContext::new);
    private final FastPool<ArrayDeque<ReaderContext>> stackPool = poolFactory.getFastPool(() -> new ArrayDeque<>(64));
    private final Pool<WriterContext> writerContextPool = poolFactory.getPool(WriterContext::new);
    private final DisposablePool<DynamicByteBuffer> bufferPool = poolFactory.getDisposablePool(() -> new DynamicByteBuffer(129 * 1024, true));

    private byte[] msgpack = new byte[] {
            (byte) 0x82,             // fixmap(2)

            (byte) 0xA2, 'i', 'd',   // "id"
            (byte) 0x01,             // 1

            (byte) 0xA4, 'm', 'e', 't', 'a', // "meta"
            (byte) 0x82,             // fixmap(2)

            (byte) 0xA6, 'a', 'c', 't', 'i', 'v', 'e', // "active"
            (byte) 0xC3,             // true

            (byte) 0xA5, 's', 'c', 'o', 'r', 'e', // "score"
            (byte) 0x64              // 100
    };

    @Test
    public void test() {
        MessagePackReader messagePackReader = new MessagePackReader(stringPool, readerContextPool, stackPool, false, false);
        messagePackReader.deserialize(ByteBuffer.wrap(msgpack));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void deserializeComplexNestedObject() {
        // {
        //   "name": "test",
        //   "id": 42,
        //   "enabled": true,
        //   "ratio": 3.14 (float32),
        //   "tags": ["alpha", "beta", "gamma"],
        //   "metadata": {
        //     "created": -1,
        //     "count": 1000,
        //     "nested": {
        //       "level": 3,
        //       "empty": null
        //     }
        //   },
        //   "data": <binary 4 bytes>
        // }

        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.order(ByteOrder.BIG_ENDIAN);

        // Root map with 7 entries
        buf.put((byte) 0x87);

        // "name": "test"
        buf.put((byte) 0xA4).put("name".getBytes());
        buf.put((byte) 0xA4).put("test".getBytes());

        // "id": 42
        buf.put((byte) 0xA2).put("id".getBytes());
        buf.put((byte) 0x2A); // positive fixint 42

        // "enabled": true
        buf.put((byte) 0xA7).put("enabled".getBytes());
        buf.put((byte) 0xC3); // true

        // "ratio": 3.14f
        buf.put((byte) 0xA5).put("ratio".getBytes());
        buf.put((byte) 0xCA); // float32
        buf.putFloat(3.14f);

        // "tags": ["alpha", "beta", "gamma"]
        buf.put((byte) 0xA4).put("tags".getBytes());
        buf.put((byte) 0x93); // fixarray(3)
        buf.put((byte) 0xA5).put("alpha".getBytes());
        buf.put((byte) 0xA4).put("beta".getBytes());
        buf.put((byte) 0xA5).put("gamma".getBytes());

        // "metadata": { ... }
        buf.put((byte) 0xA8).put("metadata".getBytes());
        buf.put((byte) 0x83); // fixmap(3)

        // "created": -1
        buf.put((byte) 0xA7).put("created".getBytes());
        buf.put((byte) 0xFF); // negative fixint -1

        // "count": 1000 (uint16)
        buf.put((byte) 0xA5).put("count".getBytes());
        buf.put((byte) 0xCD); // uint16
        buf.putShort((short) 1000);

        // "nested": { "level": 3, "empty": null }
        buf.put((byte) 0xA6).put("nested".getBytes());
        buf.put((byte) 0x82); // fixmap(2)
        buf.put((byte) 0xA5).put("level".getBytes());
        buf.put((byte) 0x03); // positive fixint 3
        buf.put((byte) 0xA5).put("empty".getBytes());
        buf.put((byte) 0xC0); // nil

        // "data": binary 4 bytes
        buf.put((byte) 0xA4).put("data".getBytes());
        buf.put((byte) 0xC4); // bin8
        buf.put((byte) 0x04); // length 4
        buf.put(new byte[] {0x01, 0x02, 0x03, 0x04});

        buf.flip();

        MessagePackReader reader = new MessagePackReader(stringPool, readerContextPool, stackPool, false, false);
        Document result = reader.deserialize(buf);

        // Verify root fields
        assertEquals("test", result.get("name"));
        assertEquals(42, result.get("id"));
        assertEquals(true, result.get("enabled"));
        assertEquals(3.14f, (Float) result.get("ratio"), 0.001f);

        // Verify tags array
        List<Object> tags = (List<Object>) result.get("tags");
        assertEquals(3, tags.size());
        assertEquals("alpha", tags.get(0));
        assertEquals("beta", tags.get(1));
        assertEquals("gamma", tags.get(2));

        // Verify metadata nested object using dot notation
        assertEquals(-1, result.get("metadata.created"));
        assertEquals(1000, result.get("metadata.count"));

        // Verify deeply nested object using dot notation
        assertEquals(3, result.get("metadata.nested.level"));
        assertNull(result.get("metadata.nested.empty"));

        // Verify binary data
        byte[] data = (byte[]) result.get("data");
        assertArrayEquals(new byte[] {0x01, 0x02, 0x03, 0x04}, data);
    }

    private ByteBuffer createBinaryTestMessage() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        buf.order(ByteOrder.BIG_ENDIAN);

        // { "bin": <binary 8 bytes>, "after": 42 }
        buf.put((byte) 0x82); // fixmap(2)

        buf.put((byte) 0xA3).put("bin".getBytes());
        buf.put((byte) 0xC4); // bin8
        buf.put((byte) 0x08); // length 8
        buf.put(new byte[] {0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, (byte) 0x80});

        buf.put((byte) 0xA5).put("after".getBytes());
        buf.put((byte) 0x2A); // fixint 42

        buf.flip();
        return buf;
    }

    @Test
    public void deserializeBinaryAsByteArray() {
        MessagePackReader reader = new MessagePackReader(stringPool, readerContextPool, stackPool, false, false);
        Document result = reader.deserialize(createBinaryTestMessage());

        Object bin = result.get("bin");
        assertInstanceOf(byte[].class, bin);

        byte[] data = (byte[]) bin;
        assertEquals(8, data.length);
        assertEquals(0x10, data[0]);
        assertEquals((byte) 0x80, data[7]);

        assertEquals(42, result.get("after"));
    }

    @Test
    public void deserializeBinaryAsByteBufferCopy() {
        MessagePackReader reader = new MessagePackReader(stringPool, readerContextPool, stackPool, false, true);
        Document result = reader.deserialize(createBinaryTestMessage());

        Object bin = result.get("bin");
        assertInstanceOf(ByteBuffer.class, bin);

        ByteBuffer buffer = (ByteBuffer) bin;
        assertTrue(buffer.isDirect());
        assertEquals(8, buffer.remaining());

        byte[] data = new byte[8];
        buffer.get(data);
        assertEquals(0x10, data[0]);
        assertEquals((byte) 0x80, data[7]);

        assertEquals(42, result.get("after"));
    }

    @Test
    public void deserializeBinaryAsByteBufferProjection() {
        ByteBuffer source = createBinaryTestMessage();

        MessagePackReader reader = new MessagePackReader(stringPool, readerContextPool, stackPool, true, true);
        Document result = reader.deserialize(source);

        Object bin = result.get("bin");
        assertInstanceOf(ByteBuffer.class, bin);

        ByteBuffer projected = (ByteBuffer) bin;
        assertFalse(projected.isDirect());
        assertEquals(8, projected.remaining());

        byte[] data = new byte[8];
        projected.get(data);
        assertEquals(0x10, data[0]);
        assertEquals((byte) 0x80, data[7]);

        assertEquals(42, result.get("after"));
    }

    // Writer tests

    @Test
    public void serializeSimpleMap() {
        MessagePackWriter writer = new MessagePackWriter(bufferPool, writerContextPool);

        Map<String, Object> map = new HashMap<>();
        map.put("id", 42);
        map.put("name", "test");

        DynamicByteBuffer result = writer.serialize(new Document(map));
        ByteBuffer buf = result.getBuffer();
        buf.flip();

        MessagePackReader reader = new MessagePackReader(stringPool, readerContextPool, stackPool, false, false);
        Document deserialized = reader.deserialize(buf);

        assertEquals(42, deserialized.get("id"));
        assertEquals("test", deserialized.get("name"));
    }

    @Test
    public void serializeNestedMap() {
        MessagePackWriter writer = new MessagePackWriter(bufferPool, writerContextPool);

        Map<String, Object> nested = new HashMap<>();
        nested.put("level", 2);
        nested.put("active", true);

        Map<String, Object> root = new HashMap<>();
        root.put("id", 1);
        root.put("nested", nested);

        DynamicByteBuffer result = writer.serialize(new Document(root));
        ByteBuffer buf = result.getBuffer();
        buf.flip();

        MessagePackReader reader = new MessagePackReader(stringPool, readerContextPool, stackPool, false, false);
        Document deserialized = reader.deserialize(buf);

        assertEquals(1, deserialized.get("id"));
        assertEquals(2, deserialized.get("nested.level"));
        assertEquals(true, deserialized.get("nested.active"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void serializeComplexObject() {
        MessagePackWriter writer = new MessagePackWriter(bufferPool, writerContextPool);

        Map<String, Object> map = new HashMap<>();
        map.put("nil", null);
        map.put("bool", true);
        map.put("int", 42);
        map.put("negInt", -10);
        map.put("long", 1000000L);
        map.put("float", 3.14f);
        map.put("double", 2.718281828);
        map.put("string", "hello");
        map.put("binary", new byte[]{0x01, 0x02, 0x03});
        map.put("array", List.of(1, 2, 3));

        DynamicByteBuffer result = writer.serialize(new Document(map));
        ByteBuffer buf = result.getBuffer();
        buf.flip();

        MessagePackReader reader = new MessagePackReader(stringPool, readerContextPool, stackPool, false, false);
        Document deserialized = reader.deserialize(buf);

        assertNull(deserialized.get("nil"));
        assertEquals(true, deserialized.get("bool"));
        assertEquals(42, deserialized.get("int"));
        assertEquals(-10, deserialized.get("negInt"));
        assertEquals(1000000, ((Number) deserialized.get("long")).intValue());
        assertEquals(3.14f, ((Number) deserialized.get("float")).floatValue(), 0.001f);
        assertEquals(2.718281828, ((Number) deserialized.get("double")).doubleValue(), 0.0000001);
        assertEquals("hello", deserialized.get("string"));

        byte[] binary = (byte[]) deserialized.get("binary");
        assertArrayEquals(new byte[]{0x01, 0x02, 0x03}, binary);

        List<Object> array = (List<Object>) deserialized.get("array");
        assertEquals(3, array.size());
        assertEquals(1, array.get(0));
        assertEquals(2, array.get(1));
        assertEquals(3, array.get(2));
    }

    @Test
    public void serializeDeeplyNestedMaps() {
        MessagePackWriter writer = new MessagePackWriter(bufferPool, writerContextPool);

        Map<String, Object> level3 = new HashMap<>();
        level3.put("value", "deep");

        Map<String, Object> level2 = new HashMap<>();
        level2.put("level3", level3);

        Map<String, Object> level1 = new HashMap<>();
        level1.put("level2", level2);

        Map<String, Object> root = new HashMap<>();
        root.put("level1", level1);

        DynamicByteBuffer result = writer.serialize(new Document(root));
        ByteBuffer buf = result.getBuffer();
        buf.flip();

        MessagePackReader reader = new MessagePackReader(stringPool, readerContextPool, stackPool, false, false);
        Document deserialized = reader.deserialize(buf);

        assertEquals("deep", deserialized.get("level1.level2.level3.value"));
    }

    @Test
    public void contextPoolReusesObjects() {
        PoolFactory localFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(1)
                .setMaxPoolSize(5)
                .setOutOfPoolTimeout(1000)
                .setBlocking(false)
                .build();

        FastPool<ReaderContext> localReaderPool = localFactory.getFastPool(ReaderContext::new);
        FastPool<ArrayDeque<ReaderContext>> localStackPool = localFactory.getFastPool(() -> new ArrayDeque<>(64));
        Pool<WriterContext> localWriterPool = localFactory.getPool(WriterContext::new);
        DisposablePool<DynamicByteBuffer> localBufferPool = localFactory.getDisposablePool(() -> new DynamicByteBuffer(129 * 1024, true));

        MessagePackWriter writer = new MessagePackWriter(localBufferPool, localWriterPool);
        MessagePackReader reader = new MessagePackReader(stringPool, localReaderPool, localStackPool, false, false);

        // Run multiple serialization/deserialization cycles
        for (int i = 0; i < 10; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("iteration", i);
            map.put("nested", Map.of("value", i * 2));

            DynamicByteBuffer result = writer.serialize(new Document(map));
            ByteBuffer buf = result.getBuffer();
            buf.flip();

            Document deserialized = reader.deserialize(buf);
            assertEquals(i, deserialized.get("iteration"));

            result.dispose();
            localBufferPool.release(result);
        }

        // Pool should have reused contexts - max pool size is 5, but we did 10 iterations
        // This verifies no memory leak and proper pool recycling
    }

    @Test
    public void serializeExtension() {
        MessagePackWriter writer = new MessagePackWriter(bufferPool, writerContextPool);

        Map<String, Object> map = new HashMap<>();
        map.put("ext", new MessagePackExtension((byte) 1, new byte[]{0x01, 0x02, 0x03, 0x04}));

        DynamicByteBuffer result = writer.serialize(new Document(map));
        ByteBuffer buf = result.getBuffer();
        buf.flip();

        MessagePackReader reader = new MessagePackReader(stringPool, readerContextPool, stackPool, false, false);
        Document deserialized = reader.deserialize(buf);

        MessagePackExtension ext = (MessagePackExtension) deserialized.get("ext");
        assertEquals(1, ext.type());
        assertArrayEquals(new byte[]{0x01, 0x02, 0x03, 0x04}, ext.data());
    }

    // --- Delta timing tests ---

    private static final int WARMUP_ITERATIONS = 1000;
    private static final int MEASURE_ITERATIONS = 10000;

    @Test
    public void timingSerialize128kb() {
        PoolFactory perfPoolFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(10)
                .setMaxPoolSize(100)
                .setOutOfPoolTimeout(1000)
                .setBlocking(true)
                .build();

        DisposablePool<DynamicByteBuffer> perfBufferPool = perfPoolFactory.getDisposablePool(() -> new DynamicByteBuffer(256 * 1024, true));
        Pool<WriterContext> perfWriterPool = perfPoolFactory.getPool(WriterContext::new);
        MessagePackWriter writer = new MessagePackWriter(perfBufferPool, perfWriterPool);

        byte[] payload = new byte[128 * 1024];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i % 128);

        Map<String, Object> map = new HashMap<>();
        map.put("command", "FORWARD");
        map.put("sessionId", 12345L);
        map.put("payload", payload);
        Document doc = new Document(map);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            DynamicByteBuffer buf = writer.serialize(doc);
            buf.dispose();
            perfBufferPool.release(buf);
        }

        // Measure
        long start = System.nanoTime();
        for (int i = 0; i < MEASURE_ITERATIONS; i++) {
            DynamicByteBuffer buf = writer.serialize(doc);
            buf.dispose();
            perfBufferPool.release(buf);
        }
        long elapsed = System.nanoTime() - start;

        double avgMicros = (elapsed / 1000.0) / MEASURE_ITERATIONS;
        double throughputMBps = (128.0 * 1024 * MEASURE_ITERATIONS) / (elapsed / 1_000_000_000.0) / (1024 * 1024);

        System.out.printf("Serialize 128KB: %.2f µs/op, %.2f MB/s%n", avgMicros, throughputMBps);
    }

    @Test
    public void timingDeserialize128kb() {
        PoolFactory perfPoolFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(10)
                .setMaxPoolSize(100)
                .setOutOfPoolTimeout(1000)
                .setBlocking(true)
                .build();

        DisposablePool<DynamicByteBuffer> perfBufferPool = perfPoolFactory.getDisposablePool(() -> new DynamicByteBuffer(256 * 1024, true));
        Pool<WriterContext> perfWriterPool = perfPoolFactory.getPool(WriterContext::new);
        FastPool<byte[]> perfStringPool = perfPoolFactory.getFastPool(() -> new byte[256]);
        Pool<byte[]> perfBinaryPool = perfPoolFactory.getPool(() -> new byte[128 * 1024]);
        FastPool<ReaderContext> perfReaderPool = perfPoolFactory.getFastPool(ReaderContext::new);
        FastPool<ArrayDeque<ReaderContext>> perfStackPool = perfPoolFactory.getFastPool(() -> new ArrayDeque<>(64));

        MessagePackWriter writer = new MessagePackWriter(perfBufferPool, perfWriterPool);
        MessagePackReader reader = new MessagePackReader(perfStringPool, perfReaderPool, perfStackPool, false, false);

        byte[] payload = new byte[128 * 1024];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i % 128);

        Map<String, Object> map = new HashMap<>();
        map.put("command", "FORWARD");
        map.put("sessionId", 12345L);
        map.put("payload", payload);

        DynamicByteBuffer serialized = writer.serialize(new Document(map));
        serialized.flip();
        ByteBuffer data = ByteBuffer.allocateDirect(serialized.getBuffer().remaining());
        data.put(serialized.getBuffer());
        serialized.dispose();
        perfBufferPool.release(serialized);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            data.rewind();
            reader.deserialize(data);
        }

        // Measure
        long start = System.nanoTime();
        for (int i = 0; i < MEASURE_ITERATIONS; i++) {
            data.rewind();
            reader.deserialize(data);
        }
        long elapsed = System.nanoTime() - start;

        double avgMicros = (elapsed / 1000.0) / MEASURE_ITERATIONS;
        double throughputMBps = (128.0 * 1024 * MEASURE_ITERATIONS) / (elapsed / 1_000_000_000.0) / (1024 * 1024);

        System.out.printf("Deserialize 128KB: %.2f µs/op, %.2f MB/s%n", avgMicros, throughputMBps);
    }

    @Test
    public void timingRoundtrip128kb() {
        PoolFactory perfPoolFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(10)
                .setMaxPoolSize(100)
                .setOutOfPoolTimeout(1000)
                .setBlocking(true)
                .build();

        DisposablePool<DynamicByteBuffer> perfBufferPool = perfPoolFactory.getDisposablePool(() -> new DynamicByteBuffer(256 * 1024, true));
        Pool<WriterContext> perfWriterPool = perfPoolFactory.getPool(WriterContext::new);
        FastPool<byte[]> perfStringPool = perfPoolFactory.getFastPool(() -> new byte[256]);
        FastPool<ReaderContext> perfReaderPool = perfPoolFactory.getFastPool(ReaderContext::new);
        FastPool<ArrayDeque<ReaderContext>> perfStackPool = perfPoolFactory.getFastPool(() -> new ArrayDeque<>(64));

        MessagePackWriter writer = new MessagePackWriter(perfBufferPool, perfWriterPool);
        MessagePackReader reader = new MessagePackReader(perfStringPool, perfReaderPool, perfStackPool, false, false);

        byte[] payload = new byte[128 * 1024];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i % 128);

        Map<String, Object> map = new HashMap<>();
        map.put("command", "FORWARD");
        map.put("sessionId", 12345L);
        map.put("payload", payload);
        Document doc = new Document(map);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            DynamicByteBuffer buf = writer.serialize(doc);
            buf.flip();
            reader.deserialize(buf.getBuffer());
            buf.dispose();
            perfBufferPool.release(buf);
        }

        // Measure
        long start = System.nanoTime();
        for (int i = 0; i < MEASURE_ITERATIONS; i++) {
            DynamicByteBuffer buf = writer.serialize(doc);
            buf.flip();
            reader.deserialize(buf.getBuffer());
            buf.dispose();
            perfBufferPool.release(buf);
        }
        long elapsed = System.nanoTime() - start;

        double avgMicros = (elapsed / 1000.0) / MEASURE_ITERATIONS;
        double throughputMBps = (128.0 * 1024 * MEASURE_ITERATIONS) / (elapsed / 1_000_000_000.0) / (1024 * 1024);

        System.out.printf("Roundtrip 128KB: %.2f µs/op, %.2f MB/s%n", avgMicros, throughputMBps);
    }

    @Test
    public void timingManyFields() {
        PoolFactory perfPoolFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(10)
                .setMaxPoolSize(100)
                .setOutOfPoolTimeout(1000)
                .setBlocking(true)
                .build();

        DisposablePool<DynamicByteBuffer> perfBufferPool = perfPoolFactory.getDisposablePool(() -> new DynamicByteBuffer(512 * 1024, true));
        Pool<WriterContext> perfWriterPool = perfPoolFactory.getPool(WriterContext::new);
        FastPool<byte[]> perfStringPool = perfPoolFactory.getFastPool(() -> new byte[256]);
        Pool<byte[]> perfBinaryPool = perfPoolFactory.getPool(() -> new byte[1024]);
        FastPool<ReaderContext> perfReaderPool = perfPoolFactory.getFastPool(ReaderContext::new);
        FastPool<ArrayDeque<ReaderContext>> perfStackPool = perfPoolFactory.getFastPool(() -> new ArrayDeque<>(64));

        MessagePackWriter writer = new MessagePackWriter(perfBufferPool, perfWriterPool);
        MessagePackReader reader = new MessagePackReader(perfStringPool, perfReaderPool, perfStackPool, false, false);

        // 1000 nested objects
        Map<String, Object> fields = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            Map<String, Object> nested = new HashMap<>();
            nested.put("id", i);
            nested.put("name", "item_" + i);
            nested.put("active", i % 2 == 0);
            nested.put("score", i * 1.5);
            fields.put("field_" + i, nested);
        }
        Document doc = new Document(fields);

        // Pre-serialize for deserialize test
        DynamicByteBuffer preSerialized = writer.serialize(doc);
        preSerialized.flip();
        ByteBuffer data = ByteBuffer.allocateDirect(preSerialized.getBuffer().remaining());
        data.put(preSerialized.getBuffer());
        int dataSize = data.position();
        preSerialized.dispose();
        perfBufferPool.release(preSerialized);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            DynamicByteBuffer buf = writer.serialize(doc);
            buf.dispose();
            perfBufferPool.release(buf);
        }

        // Measure serialize
        long startSer = System.nanoTime();
        for (int i = 0; i < MEASURE_ITERATIONS; i++) {
            DynamicByteBuffer buf = writer.serialize(doc);
            buf.dispose();
            perfBufferPool.release(buf);
        }
        long elapsedSer = System.nanoTime() - startSer;

        // Warmup deserialize
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            data.rewind();
            reader.deserialize(data);
        }

        // Measure deserialize
        long startDe = System.nanoTime();
        for (int i = 0; i < MEASURE_ITERATIONS; i++) {
            data.rewind();
            reader.deserialize(data);
        }
        long elapsedDe = System.nanoTime() - startDe;

        double avgSerMicros = (elapsedSer / 1000.0) / MEASURE_ITERATIONS;
        double avgDeMicros = (elapsedDe / 1000.0) / MEASURE_ITERATIONS;
        double opsPerSec = MEASURE_ITERATIONS / (elapsedSer / 1_000_000_000.0);

        System.out.printf("Many fields (1000 nested, %d bytes):%n", dataSize);
        System.out.printf("  Serialize:   %.2f µs/op, %.0f ops/s%n", avgSerMicros, opsPerSec);
        System.out.printf("  Deserialize: %.2f µs/op, %.0f ops/s%n", avgDeMicros, MEASURE_ITERATIONS / (elapsedDe / 1_000_000_000.0));
    }

}
