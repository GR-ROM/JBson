package su.grinev.bson;

import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

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
    private Pool<WriterContext> writerContextPool = new Pool<>(10000, WriterContext::new);
    private boolean needTraverseObject;

    public ByteBuffer serialize(Map<String, Object> document) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.rewind();

        WriterContext writerContext = writerContextPool.get();
        stack.addLast(writerContext.setNode(lengthTreeRootNode)
                .setIdx(0)
                .setLen(0)
                .setMapEntries(document.entrySet().stream().toList())
                .setListEntries(null));

        while (!stack.isEmpty()) {
            WriterContext ctx = stack.getLast();
            Node node = ctx.node;

            if (ctx.idx == 0) {
                ensureCapacity(4);
                node.lengthPos = buffer.position();
                buffer.position(buffer.position() + 4); // reserve space for length
            }

            needTraverseObject = false;

            if (ctx.mapEntries != null) {
                while (ctx.idx < ctx.mapEntries.size()) {
                    ctx.len += writeElement(ctx.mapEntries.get(ctx.idx).getKey(), ctx.mapEntries.get(ctx.idx).getValue(), node);
                    ctx.idx++;
                    if (needTraverseObject) {
                        break;
                    }
                }
            } else if (ctx.listEntries != null) {
                while (ctx.idx < ctx.listEntries.size()) {
                    ctx.len += writeElement(Integer.toString(ctx.idx), ctx.listEntries.get(ctx.idx), node);
                    ctx.idx++;
                    if (needTraverseObject) {
                        break;
                    }
                }
            }

            if (!needTraverseObject) {
                updateParentLengths(ctx);
                appendTerminator(node, ctx);
                stack.removeLast();
            }
        }

        patchLengths(lengthTreeRootNode);
        return buffer.flip();
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
            buffer.put((byte) 0x0A);             // null
            appendCString(keyBytes);
        } else if (value instanceof byte[] bytes) { // binary data
            ensureCapacity(1 + keyBytes.length + 1 + 4 + 1 + bytes.length);
            buffer.put((byte) 0x05);            // type
            appendCString(keyBytes);
            buffer.putInt(bytes.length)      // block length
                    .put((byte) 0x00)           // generic subtype
                    .put(bytes);               // data
        } else if (value instanceof Map) {
            ensureCapacity(1 + keyBytes.length + 1);
            buffer.put((byte) 0x03);            // embedded document
            appendCString(keyBytes);

            Node child = new Node(parentNode);
            parentNode.nested.add(child);
            needTraverseObject = true;

            WriterContext writerContext = writerContextPool.get();
            stack.addLast(writerContext.setNode(child)
                    .setIdx(0)
                    .setLen(0)
                    .setMapEntries(((Map<String, Object>) value).entrySet().stream().toList())
                    .setListEntries(null)
            );
        } else if (value instanceof List) {
            ensureCapacity(1 + keyBytes.length + 1);
            buffer.put((byte) 0x04); // array
            appendCString(keyBytes);

            Node child = new Node(parentNode);
            parentNode.nested.add(child);
            needTraverseObject = true;

            WriterContext writerContext = writerContextPool.get();
            stack.addLast(writerContext.setNode(child)
                    .setIdx(0)
                    .setLen(0)
                    .setMapEntries(null)
                    .setListEntries((List<Object>) value)
            );
        } else {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass());
        }

        return buffer.position() - start;
    }

    private void appendCString(byte[] keyBytes) {
        buffer.put(keyBytes).put((byte) 0x00);
    }

    private void patchLengths(Node node) {
        buffer.putInt(node.lengthPos, node.length + 4);
        for (Node child : node.nested) {
            patchLengths(child);
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

    @Setter
    @Accessors(chain = true)
    @NoArgsConstructor
    static class WriterContext {
        Node node;
        int idx;
        int len;
        List<Map.Entry<String, Object>> mapEntries;
        List<Object> listEntries;
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
