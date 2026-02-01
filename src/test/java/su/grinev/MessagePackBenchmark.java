package su.grinev;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import su.grinev.bson.Document;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class MessagePackBenchmark {

    private MessagePackWriter messagePackWriter;
    private MessagePackReader messagePackReader;
    private Document document128kb;
    private ByteBuffer serialized128kb;
    private Document manyFieldsDocument;
    private ByteBuffer manyFieldsSerialized;
    private Document simpleDocument;
    private ByteBuffer simpleSerialized;
    private DisposablePool<DynamicByteBuffer> bufferPool;

    @Setup(Level.Trial)
    public void setup() {
        PoolFactory poolFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(10)
                .setMaxPoolSize(1000)
                .setBlocking(true)
                .setOutOfPoolTimeout(1000)
                .build();

        FastPool<byte[]> stringPool = poolFactory.getFastPool(() -> new byte[256]);
        Pool<byte[]> binaryPool = poolFactory.getPool(() -> new byte[4096]);
        FastPool<ReaderContext> readerContextPool = poolFactory.getFastPool(ReaderContext::new);
        FastPool<ArrayDeque<ReaderContext>> stackPool = poolFactory.getFastPool(() -> new ArrayDeque<>(64));
        Pool<WriterContext> writerContextPool = poolFactory.getPool(WriterContext::new);
        bufferPool = poolFactory.getDisposablePool(() -> new DynamicByteBuffer(256 * 1024, true));

        messagePackWriter = new MessagePackWriter(bufferPool, writerContextPool);
        messagePackReader = new MessagePackReader(stringPool, readerContextPool, stackPool, false, false);

        // Create document with 128KB binary payload
        byte[] largePayload = new byte[128 * 1024];
        for (int i = 0; i < largePayload.length; i++) {
            largePayload[i] = (byte) (i % 128);
        }

        Map<String, Object> payload128kb = new HashMap<>();
        payload128kb.put("command", "FORWARD_PACKET");
        payload128kb.put("sessionId", 12345L);
        payload128kb.put("timestamp", System.currentTimeMillis());
        payload128kb.put("payload", largePayload);
        document128kb = new Document(payload128kb);

        // Pre-serialize for deserialization benchmark
        DynamicByteBuffer buffer = messagePackWriter.serialize(document128kb);
        buffer.flip();
        serialized128kb = ByteBuffer.allocateDirect(buffer.getBuffer().remaining());
        serialized128kb.put(buffer.getBuffer());
        serialized128kb.flip();
        buffer.dispose();
        bufferPool.release(buffer);

        // Setup many fields benchmark - 1000 nested objects
        Map<String, Object> fields = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            Map<String, Object> nested = new HashMap<>();
            nested.put("id", i);
            nested.put("name", "item_" + i);
            nested.put("active", i % 2 == 0);
            nested.put("score", i * 1.5);
            nested.put("tags", List.of("tag1", "tag2", "tag3"));
            fields.put("field_" + i, nested);
        }
        manyFieldsDocument = new Document(fields);

        // Pre-serialize many fields for deserialization benchmark
        DynamicByteBuffer manyFieldsBuffer = messagePackWriter.serialize(manyFieldsDocument);
        manyFieldsBuffer.flip();
        manyFieldsSerialized = ByteBuffer.allocateDirect(manyFieldsBuffer.getBuffer().remaining());
        manyFieldsSerialized.put(manyFieldsBuffer.getBuffer());
        manyFieldsSerialized.flip();
        manyFieldsBuffer.dispose();
        bufferPool.release(manyFieldsBuffer);

        // Setup simple document benchmark
        Map<String, Object> simple = new HashMap<>();
        simple.put("id", 42);
        simple.put("name", "test");
        simple.put("active", true);
        simple.put("score", 3.14159);
        simpleDocument = new Document(simple);

        DynamicByteBuffer simpleBuffer = messagePackWriter.serialize(simpleDocument);
        simpleBuffer.flip();
        simpleSerialized = ByteBuffer.allocateDirect(simpleBuffer.getBuffer().remaining());
        simpleSerialized.put(simpleBuffer.getBuffer());
        simpleSerialized.flip();
        simpleBuffer.dispose();
        bufferPool.release(simpleBuffer);
    }

    // --- 128KB payload benchmarks ---

    @Benchmark
    public DynamicByteBuffer serialize128kb() {
        DynamicByteBuffer buffer = messagePackWriter.serialize(document128kb);
        buffer.flip();
        buffer.dispose();
        bufferPool.release(buffer);
        return buffer;
    }

    @Benchmark
    public Document deserialize128kb() {
        serialized128kb.rewind();
        return messagePackReader.deserialize(serialized128kb);
    }

    @Benchmark
    public Document roundtrip128kb() {
        DynamicByteBuffer buffer = messagePackWriter.serialize(document128kb);
        buffer.flip();
        Document result = messagePackReader.deserialize(buffer.getBuffer());
        buffer.dispose();
        bufferPool.release(buffer);
        return result;
    }

    // --- Many small objects benchmark ---

    @Benchmark
    public DynamicByteBuffer serializeManyFields() {
        DynamicByteBuffer buffer = messagePackWriter.serialize(manyFieldsDocument);
        buffer.flip();
        buffer.dispose();
        bufferPool.release(buffer);
        return buffer;
    }

    @Benchmark
    public Document deserializeManyFields() {
        manyFieldsSerialized.rewind();
        return messagePackReader.deserialize(manyFieldsSerialized);
    }

    @Benchmark
    public Document roundtripManyFields() {
        DynamicByteBuffer buffer = messagePackWriter.serialize(manyFieldsDocument);
        buffer.flip();
        Document result = messagePackReader.deserialize(buffer.getBuffer());
        buffer.dispose();
        bufferPool.release(buffer);
        return result;
    }

    // --- Simple object benchmark ---

    @Benchmark
    public DynamicByteBuffer serializeSimple() {
        DynamicByteBuffer buffer = messagePackWriter.serialize(simpleDocument);
        buffer.flip();
        buffer.dispose();
        bufferPool.release(buffer);
        return buffer;
    }

    @Benchmark
    public Document deserializeSimple() {
        simpleSerialized.rewind();
        return messagePackReader.deserialize(simpleSerialized);
    }

    @Benchmark
    public Document roundtripSimple() {
        DynamicByteBuffer buffer = messagePackWriter.serialize(simpleDocument);
        buffer.flip();
        Document result = messagePackReader.deserialize(buffer.getBuffer());
        buffer.dispose();
        bufferPool.release(buffer);
        return result;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MessagePackBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
