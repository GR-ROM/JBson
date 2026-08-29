package su.grinev;

import java.nio.ByteBuffer;
import java.util.HashMap;

public interface Deserializer {
    void deserialize(ByteBuffer buffer, BinaryDocument document);

    /**
     * Decodes one document and returns it. The default builds a fresh HashMap-rooted document per
     * call; an implementation may instead hand back a reusable per-thread document (see
     * {@code MessagePackReader}), valid until this thread's next call — consume it before then.
     */
    default BinaryDocument deserialize(ByteBuffer buffer) {
        BinaryDocument document = new BinaryDocument(new HashMap<>());
        deserialize(buffer, document);
        return document;
    }
}
