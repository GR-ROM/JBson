package su.grinev.bson;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class BsonReader {

    private final ObjectReader objectReader;
    private final Pool<Context> contextPool;

    public BsonReader() {
        contextPool = new Pool<>(10000, () -> new Context(0, null));
        objectReader = new ObjectReader(contextPool);
    }

    public Map<String, Object> parseObject(ByteBuffer buffer) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        LinkedList<Context> stack = new LinkedList<>();
        Map<String, Object> rootObject = new HashMap<>();
        Context context = contextPool.get();
        stack.addLast(context.setPos(buffer.position()).setValue(rootObject));

        while (!stack.isEmpty()) {
            context = stack.removeFirst();
            buffer.position(context.getPos());
            Object value = context.getValue();
            contextPool.release(context);

            if (value instanceof Map<?,?> m) {
                Map<String, Object> current = (Map<String, Object>) m;
                int len = buffer.getInt();

                while (buffer.position() < buffer.limit()) {
                    byte type = buffer.get();
                    if (type == 0) {
                        break;
                    }
                    Map.Entry<String, Object> entry = objectReader.readObject(buffer, stack, type);
                    current.put(entry.getKey(), entry.getValue());
                }
            } else if (value instanceof List<?> l) {
                List<Object> current = (List<Object>) l;
                int len = buffer.getInt();

                while (buffer.position() < buffer.limit()) {
                    byte type = buffer.get();
                    if (type == 0) {
                        break;
                    }
                    Map.Entry<String, Object> entry = objectReader.readObject(buffer, stack, type);
                    current.add(entry.getValue());
                }
            }
        }

        return rootObject;
    }
}
