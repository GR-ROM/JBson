package su.grinev;

import org.bson.RawBsonDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import su.grinev.bson.BsonObjectWriter;
import su.grinev.bson.Document;
import su.grinev.pool.DynamicByteBuffer;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class BsonObjectWriterTestTest {
    private BsonObjectWriter writer;

    @BeforeEach
    void setUp() {
        writer = new BsonObjectWriter(1000, 10000);
    }

    private byte[] toByteArray(ByteBuffer buffer) {
        ByteBuffer dup = buffer.asReadOnlyBuffer();
        dup.rewind();
        byte[] bytes = new byte[dup.remaining()];
        dup.get(bytes);
        return bytes;
    }

    @Test
    void testEmptyDocument() {
        Map<String, Object> doc = new LinkedHashMap<>();
        DynamicByteBuffer buffer = writer.serialize(new Document(doc));
        byte[] bytes = toByteArray(buffer.getBuffer());
        RawBsonDocument raw = new RawBsonDocument(bytes);
        assertTrue(raw.isEmpty(), "Document should be empty");
    }

    @Test
    void testEmptyArray() {
        Map<String, Object> doc = Collections.singletonMap("arr", Collections.emptyList());
        DynamicByteBuffer buffer = writer.serialize(new Document(doc));
        byte[] bytes = toByteArray(buffer.getBuffer());
        RawBsonDocument raw = new RawBsonDocument(bytes);
        assertEquals(0, raw.getArray("arr").size());
    }

    @Test
    void testPrimitiveAndStringTypes() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("str", "hello");
        doc.put("int", 42);
        doc.put("longVal", 1234567890123L);
        doc.put("dbl", 3.14);
        doc.put("boolT", true);
        doc.put("boolF", false);
        doc.put("nil", null);

        DynamicByteBuffer buffer = writer.serialize(new Document(doc));
        byte[] bytes = toByteArray(buffer.getBuffer());
        RawBsonDocument raw = new RawBsonDocument(bytes);

        assertEquals("hello", raw.getString("str").getValue());
        assertEquals(42, raw.getInt32("int").getValue());
        assertEquals(1234567890123L, raw.getInt64("longVal").getValue());
        assertEquals(3.14, raw.getDouble("dbl").getValue());
        assertTrue(raw.getBoolean("boolT").getValue());
        assertFalse(raw.getBoolean("boolF").getValue());
        assertTrue(raw.containsKey("nil") && raw.isNull("nil"));
    }

    @Test
    void testBinaryData() {
        byte[] data = new byte[] {0x01, 0x02, 0x03, 0x04};
        Map<String, Object> doc = Collections.singletonMap("bin", data);

        DynamicByteBuffer buffer = writer.serialize(new Document(doc));
        byte[] bytes = toByteArray(buffer.getBuffer());
        RawBsonDocument raw = new RawBsonDocument(bytes);

        byte[] read = raw.getBinary("bin").getData();
        assertArrayEquals(data, read);
    }

    @Test
    void testNestedDocumentAndArray() {
        Map<String, Object> innerDoc = new LinkedHashMap<>();
        innerDoc.put("x", 10);
        innerDoc.put("y", "yes");

        List<Object> innerArr = Arrays.asList(1, "two", null);
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("doc", innerDoc);
        nested.put("arr", innerArr);

        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("nested", nested);

        DynamicByteBuffer buffer = writer.serialize(new Document(doc));
        byte[] bytes = toByteArray(buffer.getBuffer());
        RawBsonDocument raw = new RawBsonDocument(bytes);

        var nestedRaw = raw.getDocument("nested");
        assertEquals(10, nestedRaw.getDocument("doc").getInt32("x").getValue());
        assertEquals("yes", nestedRaw.getDocument("doc").getString("y").getValue());

        var arr = nestedRaw.getArray("arr");
        assertEquals(1, arr.get(0).asInt32().getValue());
        assertEquals("two", arr.get(1).asString().getValue());
        assertTrue(arr.get(2).isNull());
    }


    @Test
    void testListOfDocuments() {
        Map<String, Object> doc1 = Collections.singletonMap("a", 1);
        Map<String, Object> doc2 = Collections.singletonMap("b", 2);
        List<Object> list = Arrays.asList(doc1, doc2);
        Map<String, Object> doc = Collections.singletonMap("list", list);

        DynamicByteBuffer buffer = writer.serialize(new Document(doc));

        byte[] bytes = toByteArray(buffer.getBuffer());
        RawBsonDocument raw = new RawBsonDocument(bytes);

        var arr = raw.getArray("list");
        assertEquals(1, arr.get(0).asDocument().getInt32("a").getValue());
        assertEquals(2, arr.get(1).asDocument().getInt32("b").getValue());
    }

    @Test
    void testSpecialCharKeys() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("sp c!@#$%^&*()", "v");

        DynamicByteBuffer buffer = writer.serialize(new Document(doc));
        byte[] bytes = toByteArray(buffer.getBuffer());
        RawBsonDocument raw = new RawBsonDocument(bytes);
        assertEquals("v", raw.getString("sp c!@#$%^&*()").getValue());
    }

    @Test
    void testUnsupportedTypeThrows() {
        Map<String, Object> doc = Collections.singletonMap("date", new Date());
        assertThrows(IllegalArgumentException.class, () -> writer.serialize(new Document(doc)));
    }
}
