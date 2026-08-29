package su.grinev.messagepack;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import su.grinev.BinaryDocument;
import su.grinev.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

/**
 * MessagePack decoder into a {@link BinaryDocument}: an iterative walk with an explicit stack of
 * {@link ReaderContext} frames, one per open container.
 *
 * <p>The frames come from a per-thread array ({@link ReaderState}), not from a shared pool — see
 * {@link MessagePackWriter} for why. The same state object also carries the string scratch buffer,
 * the string cache and the nested-map pool, so one {@code ThreadLocal} lookup per
 * {@link #deserialize} replaces the three (plus two per string) the reader used to make.
 *
 * <p>Maps come from a per-thread pool of {@link CompactMap}s that is reset at the start of every
 * {@code deserialize} on that thread — including the root, for {@link #deserialize(ByteBuffer)}. A
 * decoded document must therefore be consumed (bound) before the same thread decodes the next one.
 * That is what keeps the hot path allocation-free: with a pooled root the only allocation left
 * for a packet document is the {@code slice()} view over its binary payload.
 */
@Slf4j
public class MessagePackReader implements Deserializer {
    private static final int INITIAL_FRAMES = 16;
    private static final int STRING_BUFFER_SIZE = 256;
    private static final int DEFAULT_MAX_COLLECTION_SIZE = 65536;
    private static final int DEFAULT_MAP_POOL_SIZE = 16;

    private final boolean useProjectionsForByteBuffer;
    private final boolean useByteBufferForBinary;
    private final int maxCollectionSize;
    private final ThreadLocal<ReaderState> state = ThreadLocal.withInitial(ReaderState::new);
    @Setter
    @Getter
    private boolean readLengthHeader;
    @Setter
    @Getter
    private boolean timestampAsEpochMillis;

    public MessagePackReader(boolean useProjectionsForByteBuffer, boolean useByteBufferForBinary) {
        this(useProjectionsForByteBuffer, useByteBufferForBinary, DEFAULT_MAX_COLLECTION_SIZE);
    }

    public MessagePackReader(boolean useProjectionsForByteBuffer, boolean useByteBufferForBinary, int maxCollectionSize) {
        this.useProjectionsForByteBuffer = useProjectionsForByteBuffer;
        this.useByteBufferForBinary = useByteBufferForBinary;
        this.maxCollectionSize = maxCollectionSize;
        this.readLengthHeader = true;
    }

    /** Decodes into the caller's document; its root map is filled in place. */
    public void deserialize(ByteBuffer buffer, BinaryDocument binaryDocument) {
        ReaderState st = state.get();
        st.mapPool.reset();
        decode(buffer, binaryDocument.getDocumentMap(), st);
    }

    /**
     * Decodes into this thread's reusable document, whose root is a pooled {@link CompactMap}: no
     * allocation for the document at all. The returned document is overwritten by this thread's
     * next {@code deserialize} call of either form — bind it or copy what you need before then.
     */
    @Override
    public BinaryDocument deserialize(ByteBuffer buffer) {
        ReaderState st = state.get();
        st.mapPool.reset();
        st.mapPool.get();   // claims (and clears) slot 0: the root behind st.rootDocument
        decode(buffer, st.rootDocument.getDocumentMap(), st);
        return st.rootDocument;
    }

    private void decode(ByteBuffer buffer, Map<Object, Object> root, ReaderState st) {
        // The length header counts bytes from the frame's start (see MessagePackWriter), and a frame
        // is read in-place from the buffer's CURRENT position — not absolute index 0 (callers no longer
        // slice() per frame, so several frames can share one buffer at non-zero offsets). Capture the
        // start so the over-read check below measures bytes consumed for THIS frame; comparing against
        // the absolute position would falsely warn for every non-first frame.
        final int startPosition = buffer.position();
        int length = -1;
        if (readLengthHeader) {
            length = buffer.getInt();
        }

        try {
            final int rootSize = getMapSize(buffer);
            st.push().initMap(root, rootSize);

            while (st.depth > 0) {
                ReaderContext current = st.frames[st.depth - 1];
                int level = st.depth;

                if (!current.isArray) {
                    Map<Object, Object> map = current.objectMap;
                    CompactMap compact = current.compact;
                    while (current.remaining-- > 0) {
                        if (readIntKey(buffer, st)) {
                            // Tag keys are ints: put them unboxed into a CompactMap (the pooled nested
                            // maps and the reusable root), boxed only into a caller-supplied HashMap.
                            int key = st.intKey;
                            Object value = readValue(buffer, st, false);
                            if (compact != null) {
                                compact.putInt(key, value);
                            } else {
                                map.put(key, value);
                            }
                        } else {
                            Object key = readValue(buffer, st, true);
                            Object value = readValue(buffer, st, false);
                            map.put(key, value);
                        }

                        if (st.depth > level) {
                            break;
                        }
                    }
                } else {
                    List<Object> list = current.array;
                    while (current.remaining-- > 0) {
                        Object value = readValue(buffer, st, false);
                        list.add(value);

                        if (st.depth > level) {
                            break;
                        }
                    }
                }

                if (st.depth == level) {
                    st.pop();
                }
            }
        } finally {
            // Only non-empty after an exception mid-document: drop the containers so the frames do
            // not pin them until this thread's next deserialize.
            st.unwind();
        }

        if (length > -1 && length < buffer.position() - startPosition) {
            log.warn("Buffer is too small");
        }
    }

    /**
     * Reads the next value as an {@code int} key when it is in one of the integer formats a tag is
     * written in (fixint, uint8/16, int8/16/32); stores it in {@code st.intKey} and returns true.
     * Anything else (a string key, uint32, a container) leaves the buffer untouched and returns
     * false, and the general {@link #readValue} path takes over.
     */
    private static boolean readIntKey(ByteBuffer buffer, ReaderState st) {
        int pos = buffer.position();
        byte b = buffer.get();
        if ((b & 0x80) == 0 || (b & 0xE0) == 0xE0) {
            st.intKey = b;   // positive or negative fixint
            return true;
        }
        switch (b & 0xFF) {
            case 0xCC -> st.intKey = buffer.get() & 0xFF;          // UINT8
            case 0xCD -> st.intKey = buffer.getShort() & 0xFFFF;   // UINT16
            case 0xD0 -> st.intKey = buffer.get();                 // INT8
            case 0xD1 -> st.intKey = buffer.getShort();            // INT16
            case 0xD2 -> st.intKey = buffer.getInt();              // INT32
            default -> {
                buffer.position(pos);
                return false;
            }
        }
        return true;
    }

    private Object readValue(ByteBuffer buffer, ReaderState st, boolean isKey) {
        byte b = buffer.get();
        if ((b & 0x80) == 0) {
            // Positive fixint: 0x00-0x7F (most common for small integers)
            return (int) b;
        }

        int unsigned = b & 0xFF;

        if (unsigned >= 0xA0 && unsigned <= 0xBF) {
            // Fixstr: 0xA0-0xBF
            return readString(buffer, unsigned & 0x1F, st);
        }

        if (unsigned <= 0x8F) {
            // Fixmap: 0x80-0x8F - push to stack
            return readMap(st, unsigned & 0x0F, isKey);
        }

        if (unsigned >= 0xE0) {
            // Negative fixint: 0xE0-0xFF
            return (int) b;
        }

        if (unsigned <= 0x9F) {
            // Fixarray: 0x90-0x9F - push to stack
            return readArray(st, unsigned & 0x0F, isKey);
        }

        // Less common types
        return switch (unsigned) {
            case 0xC0 -> null;  // NIL
            case 0xC2 -> false; // FALSE
            case 0xC3 -> true;  // TRUE
            case 0xCC -> buffer.get() & 0xFF;     // UINT8
            case 0xCD -> buffer.getShort() & 0xFFFF; // UINT16
            case 0xCE -> buffer.getInt() & 0xFFFFFFFFL; // UINT32
            case 0xCF -> buffer.getLong(); // UINT64
            case 0xD0 -> (int) buffer.get();   // INT8
            case 0xD1 -> (int) buffer.getShort(); // INT16
            case 0xD2 -> buffer.getInt();  // INT32
            case 0xD3 -> buffer.getLong(); // INT64
            case 0xCA -> buffer.getFloat();  // FLOAT32
            case 0xCB -> buffer.getDouble(); // FLOAT64
            case 0xD9 -> readString(buffer, buffer.get() & 0xFF, st);    // STR8
            case 0xDA -> readString(buffer, buffer.getShort() & 0xFFFF, st); // STR16
            case 0xDB -> readString(buffer, buffer.getInt(), st); // STR32
            case 0xC4 -> readBinary(buffer, buffer.get() & 0xFF);    // BIN8
            case 0xC5 -> readBinary(buffer, buffer.getShort() & 0xFFFF); // BIN16
            case 0xC6 -> readBinary(buffer, buffer.getInt()); // BIN32
            case 0xDC -> readSizedArray(st, buffer.getShort() & 0xFFFF, isKey); // ARRAY16
            case 0xDD -> readSizedArray(st, buffer.getInt(), isKey); // ARRAY32
            case 0xDE -> readSizedMap(st, buffer.getShort() & 0xFFFF, isKey); // MAP16
            case 0xDF -> readSizedMap(st, buffer.getInt(), isKey); // MAP32
            case 0xD4 -> readExtension(buffer, 1);
            case 0xD5 -> readExtension(buffer, 2);
            case 0xD6 -> readExtension(buffer, 4);
            case 0xD7 -> readExtension(buffer, 8);
            case 0xD8 -> readExtension(buffer, 16);
            case 0xC7 -> readExtension(buffer, buffer.get() & 0xFF);
            case 0xC8 -> readExtension(buffer, buffer.getShort() & 0xFFFF);
            case 0xC9 -> readExtension(buffer, buffer.getInt());
            case 0xC1 -> throw new MessagePackException("Invalid format byte 0xC1");
            default -> throw new MessagePackException("Unknown format byte 0x" + Integer.toHexString(unsigned));
        };
    }

    private Map<Object, Object> readSizedMap(ReaderState st, int size, boolean isKey) {
        validateCollectionSize(size, "map");
        return readMap(st, size, isKey);
    }

    private Map<Object, Object> readMap(ReaderState st, int size, boolean isKey) {
        if (isKey) {
            throw new MessagePackException("Map cannot be used as key");
        }
        Map<Object, Object> map = st.mapPool.get();
        st.push().initMap(map, size);
        return map;
    }

    private List<Object> readSizedArray(ReaderState st, int size, boolean isKey) {
        validateCollectionSize(size, "array");
        return readArray(st, size, isKey);
    }

    private List<Object> readArray(ReaderState st, int size, boolean isKey) {
        if (isKey) {
            throw new MessagePackException("List cannot be used as key");
        }
        List<Object> list = new ArrayList<>(size);
        if (size > 0) {
            st.push().initArray(list, size);
        }
        return list;
    }

    private void validateCollectionSize(int size, String type) {
        if (size < 0) {
            throw new MessagePackException("Negative " + type + " size: " + size);
        }
        if (size > maxCollectionSize) {
            throw new MessagePackException(type + " size " + size + " exceeds maximum " + maxCollectionSize);
        }
    }

    private void validateDataLength(int length, ByteBuffer buffer, String type) {
        if (length < 0) {
            throw new MessagePackException("Negative " + type + " length: " + length);
        }
        if (length > buffer.remaining()) {
            throw new MessagePackException(type + " length " + length + " exceeds remaining buffer " + buffer.remaining());
        }
    }

    private Object readBinary(ByteBuffer buffer, int length) {
        validateDataLength(length, buffer, "binary");
        if (useByteBufferForBinary) {
            ByteBuffer byteBuffer;
            if (useProjectionsForByteBuffer) {
                byteBuffer = buffer.slice(buffer.position(), length);
                buffer.position(buffer.position() + length);
            } else {
                byteBuffer = ByteBuffer.allocateDirect(length);
                int oldLimit = buffer.limit();
                buffer.limit(buffer.position() + length);
                byteBuffer.put(buffer);
                buffer.limit(oldLimit);
                byteBuffer.flip();
            }
            return byteBuffer;
        } else {
            byte[] data = new byte[length];
            buffer.get(data, 0, length);
            return data;
        }
    }

    private Object readExtension(ByteBuffer buffer, int length) {
        if (length < 0) {
            throw new MessagePackException("Negative extension length: " + length);
        }
        if (length + 1 < 0 || length + 1 > buffer.remaining()) { // +1 for ext type byte; overflow-safe
            throw new MessagePackException("extension length " + length + " exceeds remaining buffer " + buffer.remaining());
        }
        byte extType = buffer.get();
        if (extType == -1) {
            return readTimestamp(buffer, length);
        }
        byte[] data = new byte[length];
        buffer.get(data);
        return new MessagePackExtension(extType, data);
    }

    private Object readTimestamp(ByteBuffer buffer, int length) {
        return switch (length) {
            case 4 -> {
                long seconds = buffer.getInt() & 0xFFFFFFFFL;
                yield timestampAsEpochMillis ? seconds * 1000L : Instant.ofEpochSecond(seconds);
            }
            case 8 -> {
                long val = buffer.getLong();
                int nanos = (int) (val >>> 34);
                long seconds = val & 0x3FFFFFFFFL;
                yield timestampAsEpochMillis ? seconds * 1000L + nanos / 1_000_000L : Instant.ofEpochSecond(seconds, nanos);
            }
            case 12 -> {
                int nanos = buffer.getInt();
                long seconds = buffer.getLong();
                yield timestampAsEpochMillis ? seconds * 1000L + nanos / 1_000_000L : Instant.ofEpochSecond(seconds, nanos);
            }
            default -> throw new MessagePackException("Invalid timestamp length: " + length);
        };
    }

    private String readString(ByteBuffer buffer, int len, ReaderState st) {
        validateDataLength(len, buffer, "string");
        byte[] strBuf = st.strBuf;
        if (strBuf.length < len) {
            strBuf = new byte[Math.max(len, strBuf.length * 2)];
            st.strBuf = strBuf;
        }
        buffer.get(strBuf, 0, len);
        return st.strings.intern(strBuf, 0, len);
    }

    private int getMapSize(ByteBuffer buffer) {
        byte b = buffer.get();
        int unsigned = b & 0xFF;

        int size;
        if (unsigned >= 0x80 && unsigned <= 0x8F) {
            size = unsigned & 0x0F;
        } else if (unsigned == 0xDE) {
            size = buffer.getShort() & 0xFFFF;
        } else if (unsigned == 0xDF) {
            size = buffer.getInt();
        } else {
            throw new MessagePackException("Unexpected type 0x" + Integer.toHexString(unsigned));
        }
        validateCollectionSize(size, "root map");
        return size;
    }

    /**
     * Everything one thread needs while decoding — the frame stack, string scratch, string cache,
     * map pool and the reusable root document — behind a single {@code ThreadLocal} lookup per
     * {@link #deserialize}.
     */
    static final class ReaderState {
        ReaderContext[] frames = new ReaderContext[INITIAL_FRAMES];
        int depth;
        int intKey;                       // out-parameter of readIntKey
        byte[] strBuf = new byte[STRING_BUFFER_SIZE];
        final MapPool mapPool = new MapPool(DEFAULT_MAP_POOL_SIZE);
        final StringCache strings = new StringCache();
        /** Wraps the pool's slot 0, which reset()+get() hands out first — so the root is always this map. */
        final BinaryDocument rootDocument = new BinaryDocument(mapPool.root());

        /** Opens a new frame on top of the stack; the caller initialises it. */
        ReaderContext push() {
            if (depth == frames.length) {
                frames = Arrays.copyOf(frames, depth * 2);
            }
            ReaderContext frame = frames[depth];
            if (frame == null) {
                frames[depth] = frame = new ReaderContext();
            }
            depth++;
            return frame;
        }

        void pop() {
            frames[--depth].clear();
        }

        void unwind() {
            while (depth > 0) {
                pop();
            }
        }
    }

    static final class MapPool {
        private final CompactMap[] maps;
        private int index;

        MapPool(int size) {
            this.maps = new CompactMap[size];
            for (int i = 0; i < size; i++) {
                maps[i] = new CompactMap();
            }
        }

        /** Slot 0 — the first map {@link #get()} returns after a {@link #reset()}. */
        CompactMap root() {
            return maps[0];
        }

        Map<Object, Object> get() {
            if (index < maps.length) {
                CompactMap map = maps[index++];
                map.clear();
                return map;
            }
            return new CompactMap();
        }

        void reset() {
            index = 0;
        }
    }

    /**
     * Thread-local open-addressing string cache. After warmup with protocol strings,
     * {@link #intern} returns cached instances with zero allocation.
     */
    static final class StringCache {
        private static final int CAPACITY = 64;
        private static final int MASK = CAPACITY - 1;
        private final byte[][] cachedBytes = new byte[CAPACITY][];
        private final String[] cachedStrings = new String[CAPACITY];

        String intern(byte[] buf, int offset, int len) {
            int h = hash(buf, offset, len) & MASK;
            byte[] existing = cachedBytes[h];
            if (existing != null && existing.length == len && bytesEqual(existing, buf, offset, len)) {
                return cachedStrings[h];
            }
            String s = new String(buf, offset, len, StandardCharsets.UTF_8);
            byte[] copy = new byte[len];
            System.arraycopy(buf, offset, copy, 0, len);
            cachedBytes[h] = copy;
            cachedStrings[h] = s;
            return s;
        }

        private static int hash(byte[] buf, int offset, int len) {
            int h = 1;
            for (int i = 0; i < len; i++) {
                h = 31 * h + buf[offset + i];
            }
            return h;
        }

        private static boolean bytesEqual(byte[] a, byte[] b, int bOffset, int len) {
            for (int i = 0; i < len; i++) {
                if (a[i] != b[bOffset + i]) return false;
            }
            return true;
        }
    }
}
