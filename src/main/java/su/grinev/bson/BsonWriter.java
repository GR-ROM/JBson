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
    private ByteBuffer buffer = ByteBuffer.wrap(new byte[1 * 1024 * 1024]);
    private boolean needTraverseObject;

    public ByteBuffer serialize(Map<String, Object> document) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.rewind();
        stack.addLast(new WriterContext(0, document, lengthTreeRootNode, 0));

        while (!stack.isEmpty()) {
            WriterContext context = stack.getLast();

            if (context.object instanceof Map m) {
                writeDocument((Map<String, Object>) m, context.node, context);
            } else if (context.object instanceof List l) {
                writeDocument((List<Object>) l, context.node, context);
            }
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

        List<Map.Entry<String, Object>> entries = document.entrySet().stream()
                .toList();

        needTraverseObject = false;
        while (ctx.idx < entries.size()) {
            ctx.len += writeElement(entries.get(ctx.idx).getKey(), entries.get(ctx.idx).getValue(), node);
            ctx.idx++;
            if (needTraverseObject) {
                return;
            }
        }

        updateParentLengths(ctx);
        appendTerminator(node, ctx);

        stack.removeLast();
    }

    private void writeDocument(List<Object> document, Node node, WriterContext ctx) {
        if (ctx.idx == 0) {
            ensureCapacity(4);
            node.lengthPos = buffer.position();
            buffer.position(buffer.position() + 4); // reserve space for length
        }
        needTraverseObject = false;

        for (; ctx.idx < document.size(); ctx.idx++) {
            ctx.len += writeElement(Integer.toString(ctx.idx), document.get(ctx.idx), node);
            if (needTraverseObject) {
                return;
            }
        }

        updateParentLengths(ctx);
        appendTerminator(node, ctx);

        stack.removeLast();
    }

    private void appendTerminator(Node node, WriterContext ctx) {
        ensureCapacity(1);
        buffer.put((byte) 0x00);
        ctx.len += 1;
        node.length = ctx.len;
    }

    private static void updateParentLengths(WriterContext ctx) {
        Node current = ctx.node.parent;
        while (current != null) {
            current.length += ctx.node.length;
            current = current.parent;
        }
    }

    @SuppressWarnings("unchecked")
    private int writeElement(String key, Object value, Node parentNode) {
        int start = buffer.position();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        if (value instanceof String s) {
            byte[] strBytes = s.getBytes(StandardCharsets.UTF_8);
            ensureCapacity(1 + keyBytes.length + 1 + 4 + strBytes.length + 1);
            buffer.put((byte) 0x02); // string
            appendCString(keyBytes);
            buffer.putInt(strBytes.length + 1);
            appendCString(strBytes);
        } else if (value instanceof Integer i) {
            ensureCapacity(1 + keyBytes.length + 1 + 4);
            buffer.put((byte) 0x10); // int32
            appendCString(keyBytes);
            buffer.putInt(i);
        } else if (value instanceof Long l) {
            ensureCapacity(1 + keyBytes.length + 1 + 8);
            buffer.put((byte) 0x12); // int64
            appendCString(keyBytes);
            buffer.putLong(l);
        } else if (value instanceof Double d) {
            ensureCapacity(1 + keyBytes.length + 1 + 8);
            buffer.put((byte) 0x01); // double
            appendCString(keyBytes);
            buffer.putDouble(d);
        } else if (value instanceof Boolean b) {
            ensureCapacity(1 + keyBytes.length + 1 + 1);
            buffer.put((byte) 0x08); // boolean
            appendCString(keyBytes);
            buffer.put((byte) (b ? 1 : 0));
        } else if (value == null) {
            ensureCapacity(1 + keyBytes.length + 1);
            buffer.put((byte) 0x0A); // null
            appendCString(keyBytes);
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
            appendCString(keyBytes);

            Node child = new Node(parentNode);
            parentNode.nested.add(child);
            needTraverseObject = true;
            stack.addLast(new WriterContext(buffer.position(), m, child, 0));
        } else if (value instanceof List<?> list) {
            ensureCapacity(1 + keyBytes.length + 1);
            buffer.put((byte) 0x04); // array
            appendCString(keyBytes);

            Node child = new Node(parentNode);
            parentNode.nested.add(child);
            needTraverseObject = true;
            stack.addLast(new WriterContext(buffer.position(), list, child, 0));
        } else {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass());
        }

        return buffer.position() - start;
    }

    private void appendCString(byte[] keyBytes) {
        buffer.put(keyBytes).put((byte) 0x00);
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
        Object object;
        Node node;
        int idx;
        int len;

        WriterContext(int position, Object object, Node node, int idx) {
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
