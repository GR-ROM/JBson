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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeserializationProfileTest {

    @Test
    public void profileDeserialization() throws Exception {
        final int WARMUP = 5000;
        final int ITERATIONS = 20000;

        PoolFactory poolFactory = PoolFactory.Builder.builder()
                .setMinPoolSize(100)
                .setMaxPoolSize(2000)
                .setBlocking(true)
                .setOutOfPoolTimeout(1000)
                .build();

        // Setup JSON
        DisposablePool<DynamicByteBuffer> jsonBufferPool = poolFactory.getDisposablePool(() -> new DynamicByteBuffer(512 * 1024, true));
        JsonWriter jsonWriter = new JsonWriter(jsonBufferPool);
        JsonParser jsonParser = new JsonParser();

        // Setup BSON
        BsonObjectWriter bsonWriter = new BsonObjectWriter(poolFactory, 512 * 1024, true);
        BsonObjectReader bsonReader = new BsonObjectReader(poolFactory, 512 * 1024, 128, true, () -> ByteBuffer.allocateDirect(4096));

        // Setup MessagePack
        FastPool<byte[]> msgpackStringPool = poolFactory.getFastPool(() -> new byte[256]);
        FastPool<ReaderContext> msgpackReaderContextPool = poolFactory.getFastPool(ReaderContext::new);
        FastPool<ArrayDeque<ReaderContext>> msgpackStackPool = poolFactory.getFastPool(() -> new ArrayDeque<>(64));
        Pool<WriterContext> msgpackWriterContextPool = poolFactory.getPool(WriterContext::new);
        DisposablePool<DynamicByteBuffer> msgpackBufferPool = poolFactory.getDisposablePool(() -> new DynamicByteBuffer(512 * 1024, true));
        MessagePackWriter msgpackWriter = new MessagePackWriter(msgpackBufferPool, msgpackWriterContextPool);
        MessagePackReader msgpackReader = new MessagePackReader(msgpackStringPool, msgpackReaderContextPool, msgpackStackPool, true, true);

        // Create test data - 1000 nested objects
        HashMap<String, Object> fields = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            HashMap<String, Object> nested = new HashMap<>();
            nested.put("id", i);
            nested.put("name", "item_" + i);
            nested.put("active", i % 2 == 0);
            nested.put("score", i * 1.5);
            fields.put("field_" + i, nested);
        }
        Document document = new Document(fields);

        // Pre-serialize JSON
        DynamicByteBuffer jsonPreBuffer = jsonWriter.serialize(document);
        jsonPreBuffer.flip();
        byte[] jsonBytes = new byte[jsonPreBuffer.getBuffer().remaining()];
        jsonPreBuffer.getBuffer().get(jsonBytes);
        jsonPreBuffer.dispose();

        // Pre-serialize BSON
        DynamicByteBuffer bsonPreBuffer = bsonWriter.serialize(document);
        bsonPreBuffer.flip();
        byte[] bsonBytes = new byte[bsonPreBuffer.getBuffer().remaining()];
        bsonPreBuffer.getBuffer().get(bsonBytes);
        bsonPreBuffer.dispose();

        // Pre-serialize MessagePack
        DynamicByteBuffer msgpackPreBuffer = msgpackWriter.serialize(document);
        msgpackPreBuffer.flip();
        byte[] msgpackBytes = new byte[msgpackPreBuffer.getBuffer().remaining()];
        msgpackPreBuffer.getBuffer().get(msgpackBytes);
        msgpackPreBuffer.dispose();

        // Pre-serialize Java native
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(fields);
        oos.close();
        byte[] javaBytes = baos.toByteArray();

        System.out.println("=== Serialized Sizes ===");
        System.out.println("JSON size:        " + jsonBytes.length + " bytes");
        System.out.println("BSON size:        " + bsonBytes.length + " bytes");
        System.out.println("MessagePack size: " + msgpackBytes.length + " bytes");
        System.out.println("Java native size: " + javaBytes.length + " bytes");
        System.out.println();

        // Warmup
        for (int i = 0; i < WARMUP; i++) {
            List<Token> tokens = new Tokenizer(jsonBytes).tokenize();
            jsonParser.parse(tokens);
            bsonReader.deserialize(ByteBuffer.wrap(bsonBytes));
            msgpackReader.deserialize(ByteBuffer.wrap(msgpackBytes));
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(javaBytes));
            ois.readObject();
            ois.close();
        }

        // Profile JSON
        List<Long> jsonTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            List<Token> tokens = new Tokenizer(jsonBytes).tokenize();
            jsonParser.parse(tokens);
            jsonTimes.add(System.nanoTime() - start);
        }

        // Profile BSON
        List<Long> bsonTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            ByteBuffer buf = ByteBuffer.wrap(bsonBytes);
            long start = System.nanoTime();
            bsonReader.deserialize(buf);
            bsonTimes.add(System.nanoTime() - start);
        }

        // Profile MessagePack
        List<Long> msgpackTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            ByteBuffer buf = ByteBuffer.wrap(msgpackBytes);
            long start = System.nanoTime();
            msgpackReader.deserialize(buf);
            msgpackTimes.add(System.nanoTime() - start);
        }

        // Profile Java native serialization
        List<Long> javaTimes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            ByteArrayInputStream bais = new ByteArrayInputStream(javaBytes);
            long start = System.nanoTime();
            ObjectInputStream ois = new ObjectInputStream(bais);
            ois.readObject();
            ois.close();
            javaTimes.add(System.nanoTime() - start);
        }

        jsonTimes.sort(Long::compareTo);
        bsonTimes.sort(Long::compareTo);
        msgpackTimes.sort(Long::compareTo);
        javaTimes.sort(Long::compareTo);

        System.out.println("=== Deserialization Profile ===");
        System.out.printf("JSON median:        %7.1f us%n", jsonTimes.get(ITERATIONS / 2) / 1000.0);
        System.out.printf("JSON p95:           %7.1f us%n", jsonTimes.get((int)(ITERATIONS * 0.95)) / 1000.0);
        System.out.printf("JSON min:           %7.1f us%n", jsonTimes.get(0) / 1000.0);
        System.out.println();
        System.out.printf("BSON median:        %7.1f us%n", bsonTimes.get(ITERATIONS / 2) / 1000.0);
        System.out.printf("BSON p95:           %7.1f us%n", bsonTimes.get((int)(ITERATIONS * 0.95)) / 1000.0);
        System.out.printf("BSON min:           %7.1f us%n", bsonTimes.get(0) / 1000.0);
        System.out.println();
        System.out.printf("MessagePack median: %7.1f us%n", msgpackTimes.get(ITERATIONS / 2) / 1000.0);
        System.out.printf("MessagePack p95:    %7.1f us%n", msgpackTimes.get((int)(ITERATIONS * 0.95)) / 1000.0);
        System.out.printf("MessagePack min:    %7.1f us%n", msgpackTimes.get(0) / 1000.0);
        System.out.println();
        System.out.printf("Java native median: %7.1f us%n", javaTimes.get(ITERATIONS / 2) / 1000.0);
        System.out.printf("Java native p95:    %7.1f us%n", javaTimes.get((int)(ITERATIONS * 0.95)) / 1000.0);
        System.out.printf("Java native min:    %7.1f us%n", javaTimes.get(0) / 1000.0);
        System.out.println();

        // Calculate speedup vs Java native
        double javaMedian = javaTimes.get(ITERATIONS / 2) / 1000.0;
        double jsonMedian = jsonTimes.get(ITERATIONS / 2) / 1000.0;
        double bsonMedian = bsonTimes.get(ITERATIONS / 2) / 1000.0;
        double msgpackMedian = msgpackTimes.get(ITERATIONS / 2) / 1000.0;
        System.out.println("=== Speedup vs Java Native ===");
        System.out.printf("JSON:        %.1fx faster%n", javaMedian / jsonMedian);
        System.out.printf("BSON:        %.1fx faster%n", javaMedian / bsonMedian);
        System.out.printf("MessagePack: %.1fx faster%n", javaMedian / msgpackMedian);
    }
}
