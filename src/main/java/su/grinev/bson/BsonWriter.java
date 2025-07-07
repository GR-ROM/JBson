package su.grinev.bson;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BsonWriter {
    private final LinkedList<WriterContext> stack = new LinkedList<>();
    private final Node lengthTreeRootNode = new Node(null);
    private ByteBuffer buffer = ByteBuffer.wrap(new byte[1024]);
    private boolean needTraverseObject;

    public ByteBuffer serialize(Map<String, Object> document) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        stack.addLast(new WriterContext(0, document, lengthTreeRootNode, 0));

        while (!stack.isEmpty()) {
            WriterContext context = stack.getLast();
            writeDocument(context.object, context.node, context);
        }

        writeLengths(lengthTreeRootNode);
        return buffer.flip();
    }

    @SuppressWarnings("unchecked")
    private void writeDocument(Map<String, Object> document, Node node, WriterContext ctx) {
        if (ctx.idx == 0) {
            ensureCapacity(4);
            node.lengthPos = buffer.position();
            buffer.position(buffer.position() + 4); // reserve space for length
        }

        List<Map.Entry<String, Object>> entries = document.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList();
        needTraverseObject = false;
        while (ctx.idx < entries.size()) {
            ctx.len += writeElement(entries.get(ctx.idx).getKey(), entries.get(ctx.idx).getValue(), node);
            ctx.idx++;
            if (needTraverseObject) {
                return;
            }
        }

        Node current = ctx.node.parent;
        while (current != null) {
            current.length += ctx.node.length;
            current = current.parent;
        }

        stack.removeLast();
        ensureCapacity(1);
        buffer.put((byte) 0x00); // document terminator
        ctx.len += 1;
        node.length = ctx.len;
    }

    private void writeArray(List<Object> list, Node node, int idx) {
        ensureCapacity(4);
        node.lengthPos = buffer.position();
        buffer.position(buffer.position() + 4); // reserve space for length

        int len = 0;
        for (int i = 0; i < list.size(); i++) {
            String key = Integer.toString(i);
            len += writeElement(key, list.get(i), node);
        }

        ensureCapacity(1);
        buffer.put((byte) 0x00); // array terminator
        len += 1;
        node.length = len;
    }

    @SuppressWarnings("unchecked")
    private int writeElement(String key, Object value, Node parentNode) {
        int start = buffer.position();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        if (value instanceof String s) {
            byte[] strBytes = s.getBytes(StandardCharsets.UTF_8);
            ensureCapacity(1 + keyBytes.length + 1 + 4 + strBytes.length + 1);
            buffer.put((byte) 0x02); // string
            buffer.put(keyBytes).put((byte) 0x00);
            buffer.putInt(strBytes.length + 1);
            buffer.put(strBytes).put((byte) 0x00);
        } else if (value instanceof Integer i) {
            ensureCapacity(1 + keyBytes.length + 1 + 4);
            buffer.put((byte) 0x10); // int32
            buffer.put(keyBytes).put((byte) 0x00);
            buffer.putInt(i);
        } else if (value instanceof Long l) {
            ensureCapacity(1 + keyBytes.length + 1 + 8);
            buffer.put((byte) 0x12); // int64
            buffer.put(keyBytes).put((byte) 0x00);
            buffer.putLong(l);
        } else if (value instanceof Double d) {
            ensureCapacity(1 + keyBytes.length + 1 + 8);
            buffer.put((byte) 0x01); // double
            buffer.put(keyBytes).put((byte) 0x00);
            buffer.putDouble(d);
        } else if (value instanceof Boolean b) {
            ensureCapacity(1 + keyBytes.length + 1 + 1);
            buffer.put((byte) 0x08); // boolean
            buffer.put(keyBytes).put((byte) 0x00);
            buffer.put((byte) (b ? 1 : 0));
        } else if (value == null) {
            ensureCapacity(1 + keyBytes.length + 1);
            buffer.put((byte) 0x0A); // null
            buffer.put(keyBytes).put((byte) 0x00);
        } else if (value instanceof byte[] bytes) {  // binary data
            ensureCapacity(1 + keyBytes.length + 1 + 4 + 1 + bytes.length);
            buffer.put((byte) 0x05)                 // type
                    .put(keyBytes).put((byte) 0x00) // cstring
                    .putInt(bytes.length)           // block length
                    .put((byte) 0x00)               // generic subtype
                    .put(bytes);                    // data
        } else if (value instanceof Map<?, ?> m) {
            ensureCapacity(1 + keyBytes.length + 1);
            buffer.put((byte) 0x03); // embedded document
            buffer.put(keyBytes).put((byte) 0x00);

            Node child = new Node(parentNode);
            parentNode.nested.add(child);
            needTraverseObject = true;
            stack.addLast(new WriterContext(buffer.position(), (Map<String, Object>) m, child, 0));
        } else if (value instanceof List<?> list) {
            ensureCapacity(1 + keyBytes.length + 1);
            buffer.put((byte) 0x04); // array
            buffer.put(keyBytes).put((byte) 0x00);

            Node child = new Node(parentNode);
            parentNode.nested.add(child);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass());
        }

        return buffer.position() - start;
    }

    private void writeLengths(Node node) {
        buffer.putInt(node.lengthPos, node.length + 4);
        for (Node child : node.nested) {
            writeLengths(child);
        }
    }

    private void ensureCapacity(int additional) {
        if (buffer.remaining() < additional) {
            ByteBuffer oldBuffer = buffer;
            buffer = ByteBuffer.allocateDirect(Math.max(oldBuffer.capacity() * 2, oldBuffer.capacity() + additional));
            oldBuffer.flip();
            buffer.put(oldBuffer);
        }
    }

    static class WriterContext {
        int position;
        Map<String, Object> object;
        Node node;
        int idx;
        int len;

        WriterContext(int position, Map<String, Object> object, Node node, int idx) {
            this.position = position;
            this.object = object;
            this.node = node;
            this.idx = idx;
        }
    }

    static class Node {
        final Node parent;
        final List<Node> nested = new ArrayList<>();
        int length = 0;
        int lengthPos = -1;

        Node(Node parent) {
            this.parent = parent;
        }
    }
}
