package su.grinev;

import annotation.Tag;
import org.junit.jupiter.api.Test;
import su.grinev.bson.BsonObjectReader;
import su.grinev.bson.BsonObjectWriter;
import su.grinev.messagepack.CompactMap;
import su.grinev.messagepack.MessagePackReader;
import su.grinev.messagepack.MessagePackWriter;
import su.grinev.pool.DynamicByteBuffer;
import su.grinev.pool.PoolFactory;
import su.grinev.test.VpnForwardPacketDto;
import su.grinev.test.VpnRequestDto;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;
import static su.grinev.test.Command.FOO;

/**
 * The allocation-free document path: int keys go into CompactMaps unboxed (including the 1488
 * discriminator and overflow tags), the reader hands back a reusable root, and Binder.unbind
 * produces CompactMaps that the writer walks without an iterator allocation.
 */
class CompactDocumentTest {

    public static class WideTags {
        @Tag(0) private int a;
        @Tag(15) private int b;
        @Tag(16) private int c;      // first overflow slot
        @Tag(300) private String d;  // uint16 tag

        public WideTags() {}

        WideTags(int a, int b, int c, String d) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.d = d;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof WideTags w && a == w.a && b == w.b && c == w.c && Objects.equals(d, w.d);
        }

        @Override
        public int hashCode() {
            return Objects.hash(a, b, c, d);
        }

        @Override
        public String toString() {
            return "WideTags{" + a + "," + b + "," + c + "," + d + "}";
        }
    }

    private static ByteBuffer encode(MessagePackWriter writer, BinaryDocument doc) {
        DynamicByteBuffer buf = new DynamicByteBuffer(4096, true);
        writer.serialize(buf, doc);
        return buf.getBuffer();
    }

    @Test
    void intKeysLandInCompactMapWithoutBoxingPath() {
        // Hand-built: {0: 42, 1488: "D", 16: 7, 300: 8, -5: 9, "s": 10}
        ByteBuffer wire = ByteBuffer.allocate(64);
        wire.put((byte) 0x86);
        wire.put((byte) 0x00).put((byte) 0x2A);                                   // 0 -> 42
        wire.put((byte) 0xCD).putShort((short) 1488).put((byte) 0xA1).put((byte) 'D'); // 1488 -> "D"
        wire.put((byte) 0x10).put((byte) 0x07);                                   // 16 -> 7
        wire.put((byte) 0xCD).putShort((short) 300).put((byte) 0x08);             // 300 -> 8
        wire.put((byte) 0xFB).put((byte) 0x09);                                   // -5 -> 9
        wire.put((byte) 0xA1).put((byte) 's').put((byte) 0x0A);                   // "s" -> 10
        wire.flip();

        MessagePackReader reader = new MessagePackReader(false, false);
        reader.setReadLengthHeader(false);
        BinaryDocument doc = reader.deserialize(wire);

        assertInstanceOf(CompactMap.class, doc.getDocumentMap());
        assertEquals(6, doc.getDocumentMap().size());
        assertEquals(42, doc.get("0"));
        assertEquals("D", doc.get("1488"));
        assertEquals(7, doc.get("16"));
        assertEquals(8, doc.get("300"));
        assertEquals(9, doc.getDocumentMap().get(-5));
        assertEquals(10, doc.getDocumentMap().get("s"));
    }

    @Test
    void reusableRootIsTheSameInstanceAndClearedBetweenCalls() {
        MessagePackWriter writer = new MessagePackWriter();
        MessagePackReader reader = new MessagePackReader(false, false);

        Map<Object, Object> first = new HashMap<>();
        first.put(0, "one");
        first.put(1, Map.of(0, 1));
        Map<Object, Object> second = new HashMap<>();
        second.put(2, "two");

        BinaryDocument d1 = reader.deserialize(encode(writer, new BinaryDocument(first)));
        assertEquals("one", d1.get("0"));
        assertEquals(1, d1.get("1.0"));

        BinaryDocument d2 = reader.deserialize(encode(writer, new BinaryDocument(second)));
        assertSame(d1, d2, "per-thread root document is reused");
        assertEquals("two", d2.get("2"));
        assertNull(d2.get("0"), "previous document's entries are gone");
        assertEquals(1, d2.getDocumentMap().size());
    }

    @Test
    void callerSuppliedHashMapRootStillWorks() {
        MessagePackWriter writer = new MessagePackWriter();
        MessagePackReader reader = new MessagePackReader(false, false);
        Map<Object, Object> src = new HashMap<>();
        src.put(0, "x");
        src.put(1488, "disc");

        BinaryDocument out = new BinaryDocument(new HashMap<>());
        reader.deserialize(encode(writer, new BinaryDocument(src)), out);
        assertInstanceOf(HashMap.class, out.getDocumentMap());
        assertEquals("x", out.get("0"));
        assertEquals("disc", out.get("1488"));
    }

    @Test
    void unbindProducesCompactMapsAndRoundTripsThroughMessagePack() {
        Binder binder = new Binder(Binder.ClassNameMode.FULL_NAME);
        ByteBuffer packet = ByteBuffer.wrap(new byte[]{1, 2, 3});
        VpnRequestDto<VpnForwardPacketDto> dto = VpnRequestDto.wrap(FOO,
                VpnForwardPacketDto.builder().packet(packet).build());

        BinaryDocument doc = binder.unbind(dto);
        assertInstanceOf(CompactMap.class, doc.getDocumentMap());
        assertInstanceOf(CompactMap.class, doc.getDocumentMap().get(1), "nested document");

        MessagePackWriter writer = new MessagePackWriter();
        MessagePackReader reader = new MessagePackReader(true, true);
        BinaryDocument decoded = reader.deserialize(encode(writer, doc));
        VpnRequestDto<?> back = binder.bind(VpnRequestDto.class, decoded);
        assertEquals(FOO, back.getCommand());
        assertInstanceOf(VpnForwardPacketDto.class, back.getData());
        ByteBuffer packetOut = ((VpnForwardPacketDto) back.getData()).getPacket();
        byte[] bytes = new byte[packetOut.remaining()];
        packetOut.get(bytes);
        assertArrayEquals(new byte[]{1, 2, 3}, bytes);
    }

    @Test
    void tagsBeyondFifteenRoundTripViaOverflow() {
        Binder.registerClass(WideTags.class);
        Binder binder = new Binder(Binder.ClassNameMode.FULL_NAME);
        WideTags in = new WideTags(1, 2, 3, "three hundred");

        BinaryDocument doc = binder.unbind(in);
        assertInstanceOf(CompactMap.class, doc.getDocumentMap());
        assertEquals(4, doc.getDocumentMap().size());

        MessagePackWriter writer = new MessagePackWriter();
        MessagePackReader reader = new MessagePackReader(false, false);
        WideTags out = binder.bind(WideTags.class, reader.deserialize(encode(writer, doc)));
        assertEquals(in, out);
    }

    @Test
    void compactDocumentsRoundTripThroughBsonToo() {
        PoolFactory pf = PoolFactory.Builder.builder().setMinPoolSize(1).setMaxPoolSize(4)
                .setOutOfPoolTimeout(1000).setBlocking(false).build();
        BsonObjectWriter bsonWriter = new BsonObjectWriter(pf, 4096, true);
        BsonObjectReader bsonReader = new BsonObjectReader(pf, 4096, true, () -> ByteBuffer.allocateDirect(4096));
        Binder binder = new Binder(Binder.ClassNameMode.FULL_NAME);
        WideTags in = new WideTags(4, 5, 6, "bson");

        DynamicByteBuffer buf = new DynamicByteBuffer(4096, true);
        bsonWriter.serialize(buf, binder.unbind(in));
        BinaryDocument decoded = new BinaryDocument(new HashMap<>());
        bsonReader.deserialize(buf.getBuffer(), decoded);
        assertEquals(in, binder.bind(WideTags.class, decoded));
    }
}
