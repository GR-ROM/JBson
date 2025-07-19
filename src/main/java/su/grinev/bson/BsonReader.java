package su.grinev.bson;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class BsonReader {
    private final ObjectReader objectReader;
    private final Pool<ReaderContext> contextPool;

    public BsonReader() {
        contextPool = new Pool<>(10000, ReaderContext::new);
        objectReader = new ObjectReader(contextPool);
    }

    public Map<String, Object> deserialize(ByteBuffer buffer) {
        Map<String, Object> rootDocument = new HashMap<>();
        Deque<ReaderContext> stack = new ArrayDeque<>(64);

        buffer.order(ByteOrder.LITTLE_ENDIAN);
        ReaderContext readerContext = contextPool.get();
        stack.addLast(readerContext.setPos(buffer.position())
                .setKey("")
                .setValue(rootDocument)
        );

        while (!stack.isEmpty()) {
            readerContext = stack.removeLast();

            buffer.position(readerContext.getPos());
            contextPool.release(readerContext);
            int len = buffer.getInt();

            Map.Entry<String, Object> element;

            if (readerContext.getValue() instanceof Map m) {
                while ((element = objectReader.readElement(buffer, stack)) != null) {
                    m.put(element.getKey(), element.getValue());
                }
            } else if (readerContext.getValue() instanceof List l) {
                while ((element = objectReader.readElement(buffer, stack)) != null) {
                    l.add(element.getValue());
                }

            }
        }

        return rootDocument;
    }

}
