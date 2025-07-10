package su.grinev.bson;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class BsonReader {
    private final ObjectReader objectReader;
    private final Pool<Context> contextPool;

    public BsonReader() {
        contextPool = new Pool<>(10000, () -> new Context(0));
        objectReader = new ObjectReader(contextPool);
    }

    public Map<String, Object> deserialize(ByteBuffer buffer) {
        Map<String, Object> rootDocument = new HashMap<>();
        Deque<Context> stack = new LinkedList<>();
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        Context context = contextPool.get();
        stack.addLast(context.setPos(buffer.position())
                .setKey("")
                .setValue(rootDocument)
        );

        while (!stack.isEmpty()) {
            context = stack.removeFirst();

            buffer.position(context.getPos());
            contextPool.release(context);
            int len = buffer.getInt();

            Map.Entry<String, Object> element;

            if (context.getValue() instanceof Map m) {
                while ((element = objectReader.readElement(buffer, stack)) != null) {
                    m.put(element.getKey(), element.getValue());
                }
            } else if (context.getValue() instanceof List l) {
                while ((element = objectReader.readElement(buffer, stack)) != null) {
                    l.add(element.getValue());
                }

            }
        }

        return rootDocument;
    }

}
