package su.grinev.bson;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class BsonReader {
    private final Pool<ReaderContext> contextPool = new Pool<>(1000, 10000, ReaderContext::new);
    private final ObjectReader objectReader = new ObjectReader(contextPool);

    public Map<String, Object> deserialize(ByteBuffer buffer) {
        Map<String, Object> rootDocument = new HashMap<>();
        Deque<ReaderContext> stack = new ArrayDeque<>(64);

        buffer.order(ByteOrder.LITTLE_ENDIAN);
        ReaderContext readerContext = contextPool.get();
        stack.addLast(readerContext.setPos(buffer.position()).setValue(rootDocument));

        while (!stack.isEmpty()) {
            readerContext = stack.removeLast();

            buffer.position(readerContext.getPos());
            contextPool.release(readerContext);
            int len = buffer.getInt();

            if (readerContext.getValue() instanceof Map m) {
                while (objectReader.readElement(buffer, stack, m, null)) {
                }
            } else if (readerContext.getValue() instanceof List l) {
                while (objectReader.readElement(buffer, stack, null, l)) {
                }
            }
        }

        return rootDocument;
    }

}
