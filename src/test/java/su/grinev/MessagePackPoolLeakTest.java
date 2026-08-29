package su.grinev;

import org.junit.jupiter.api.Test;
import su.grinev.messagepack.MessagePackException;
import su.grinev.messagepack.MessagePackReader;

import java.nio.ByteBuffer;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that the reader stays usable after deserialization fails mid-parse on malformed
 * input: the per-thread frame stack must be unwound in finally, so frames left over from a
 * failed document never leak into the next one.
 *
 * Historically (J6 bug) the frames came from a pool and the finally block did stack.clear()
 * without releasing them, exhausting the pool after repeated failures.
 */
public class MessagePackPoolLeakTest {

    /**
     * Repeated malformed packets with nested structures: 50 failures in a row, the reader
     * must still decode a valid document afterwards.
     */
    @Test
    void repeatedMalformedPackets_poolRemainsHealthy() {
        MessagePackReader reader = new MessagePackReader(false, false);
        reader.setReadLengthHeader(false);

        byte[] malformed = craftNestedMapThenTruncate();

        for (int i = 0; i < 50; i++) {
            ByteBuffer buf = ByteBuffer.wrap(malformed);
            try {
                reader.deserialize(buf, new BinaryDocument(new HashMap<>()));
                fail("Should have thrown on malformed data");
            } catch (MessagePackException | java.nio.BufferUnderflowException e) {
                // Expected
            }
        }

        // Valid payload must still work after 50 failures
        ByteBuffer valid = craftValidPayload();
        BinaryDocument doc = new BinaryDocument(new HashMap<>());
        assertDoesNotThrow(() -> reader.deserialize(valid, doc));
        assertEquals(42, doc.get("0"));
    }

    /**
     * Deeply nested malformed packets (3 levels = 3 frames left open per failure)
     * must also leave the reader healthy.
     */
    @Test
    void deeplyNestedMalformed_poolRemainsHealthy() {
        MessagePackReader reader = new MessagePackReader(false, false);
        reader.setReadLengthHeader(false);

        byte[] deepMalformed = craftDeeplyNestedThenTruncate();

        for (int i = 0; i < 50; i++) {
            ByteBuffer buf = ByteBuffer.wrap(deepMalformed);
            try {
                reader.deserialize(buf, new BinaryDocument(new HashMap<>()));
            } catch (MessagePackException | java.nio.BufferUnderflowException e) {
                // Expected
            }
        }

        ByteBuffer valid = craftValidPayload();
        BinaryDocument doc = new BinaryDocument(new HashMap<>());
        assertDoesNotThrow(() -> reader.deserialize(valid, doc));
        assertEquals(42, doc.get("0"));
    }

    /**
     * Mix of valid and malformed packets — the reader must stay healthy throughout.
     */
    @Test
    void mixedValidAndMalformed_poolRemainsHealthy() {
        MessagePackReader reader = new MessagePackReader(false, false);
        reader.setReadLengthHeader(false);

        for (int i = 0; i < 100; i++) {
            if (i % 3 == 0) {
                // Valid packet
                ByteBuffer valid = craftValidPayload();
                BinaryDocument doc = new BinaryDocument(new HashMap<>());
                assertDoesNotThrow(() -> reader.deserialize(valid, doc));
                assertEquals(42, doc.get("0"));
            } else {
                // Malformed packet
                ByteBuffer buf = ByteBuffer.wrap(craftNestedMapThenTruncate());
                try {
                    reader.deserialize(buf, new BinaryDocument(new HashMap<>()));
                } catch (MessagePackException | java.nio.BufferUnderflowException e) {
                    // Expected
                }
            }
        }
    }

    // --- Payload helpers ---

    /** { 0: { <truncated — 2 entries declared but 0 provided> } } — leaks 2 contexts */
    private byte[] craftNestedMapThenTruncate() {
        return new byte[]{
                (byte) 0x81,  // fixmap, 1 entry (root)
                0x00,         // key: 0
                (byte) 0x82   // fixmap, 2 entries (nested) — truncated!
        };
    }

    /** { 0: { 0: { 0: { <truncated> } } } } — leaks 4 contexts */
    private byte[] craftDeeplyNestedThenTruncate() {
        return new byte[]{
                (byte) 0x81,  // root: fixmap, 1 entry
                0x00,         // key: 0
                (byte) 0x81,  // level 1: fixmap, 1 entry
                0x00,         // key: 0
                (byte) 0x81,  // level 2: fixmap, 1 entry
                0x00,         // key: 0
                (byte) 0x82   // level 3: fixmap, 2 entries — truncated!
        };
    }

    private ByteBuffer craftValidPayload() {
        return ByteBuffer.wrap(new byte[]{
                (byte) 0x81,  // fixmap, 1 entry
                0x00,         // key: 0
                0x2A          // value: 42
        });
    }
}
