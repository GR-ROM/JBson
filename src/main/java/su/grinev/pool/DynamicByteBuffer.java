package su.grinev.pool;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Mirrors the {@link ByteBuffer} API by delegation. {@code ByteBuffer} cannot be
 * subclassed outside {@code java.nio} (all its constructors are package-private),
 * so this facade is the closest achievable "is-a": every relative/absolute
 * get/put and state method delegates to the backing buffer, letting call sites
 * drop {@code getBuffer()} except where a real {@code ByteBuffer} is required by
 * type (e.g. {@code SocketChannel.write}).
 *
 * Note: {@code getBuffer()} / {@link #duplicate()} / {@link #slice()} views detach
 * if {@code ensureCapacity} reallocates — prefer calling through this class.
 */
public class DynamicByteBuffer extends ArenaByteBuffer implements Disposable {
    private Runnable onDispose;

    // Framing headroom (opt-in, set only via the origin-offset constructor). The payload is written
    // starting at originOffset, reserving [0, originOffset) so a protocol header can be spliced in place
    // right before the payload — no copy into a second buffer. originSize is the payload length, set by
    // the producer (e.g. the TUN reader) after it writes the payload. Both default to 0, so buffers made
    // through the other constructors behave exactly as before.
    private int originOffset;
    private int originSize;

    public DynamicByteBuffer(int capacity) {
        super(capacity);
    }

    /** Explicit release mode (AUTO = GC-managed default, MANUAL = deterministic destroy()). */
    public DynamicByteBuffer(int capacity, Release release) {
        super(capacity, release);
    }

    /**
     * Compatibility constructor: arena-backed memory is always native, so the
     * {@code direct} flag is no longer meaningful and is ignored. Defaults to AUTO release.
     */
    public DynamicByteBuffer(int capacity, boolean direct) {
        super(capacity);
    }

    /**
     * Reserves {@code originOffset} bytes of headroom at the front so a producer can write its payload
     * at {@code originOffset} and a framing header can later be spliced into the headroom in place,
     * ending exactly where the payload begins (see the fast-path outbound serializer). Kept as a
     * dedicated constructor so the existing ones stay byte-for-byte compatible.
     */
    public DynamicByteBuffer(int capacity, int originOffset, Release release) {
        super(capacity, release);
        this.originOffset = originOffset;
    }

    /** Covariant override so a {@code Supplier<DynamicByteBuffer>} can chain it — see the supertype. */
    @Override
    public DynamicByteBuffer fixCapacity() {
        super.fixCapacity();
        return this;
    }

    /** Start offset of the payload within the buffer; header headroom is {@code [0, originOffset)}. */
    public int getOriginOffset() {
        return originOffset;
    }

    public void setOriginOffset(int originOffset) {
        this.originOffset = originOffset;
    }

    /** Payload length written at {@code originOffset}; set by the producer after writing the payload. */
    public int getOriginSize() {
        return originSize;
    }

    public void setOriginSize(int originSize) {
        this.originSize = originSize;
    }

    public void initBuffer() {
        super.buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.clear();
    }

    public DynamicByteBuffer position(int newPosition) {
        buffer.position(newPosition);
        return this;
    }

    public int position() {
        return buffer.position();
    }

    public DynamicByteBuffer put(byte b) {
        buffer.put(b);
        return this;
    }

    public DynamicByteBuffer put(byte[] b) {
        buffer.put(b);
        return this;
    }

    public DynamicByteBuffer put(byte[] b, int off, int len) {
        buffer.put(b, off, len);
        return this;
    }

    public DynamicByteBuffer putInt(int i) {
        buffer.putInt(i);
        return this;
    }

    public DynamicByteBuffer putInt(int pos, int i) {
        buffer.putInt(pos, i);
        return this;
    }

    public DynamicByteBuffer putShort(short s) {
        buffer.putShort(s);
        return this;
    }

    public DynamicByteBuffer flip() {
        buffer.flip();
        return this;
    }

    public DynamicByteBuffer rewind() {
        buffer.rewind();
        return this;
    }

    public DynamicByteBuffer putLong(long l) {
        buffer.putLong(l);
        return this;
    }

    public DynamicByteBuffer putLong(int pos, long l) {
        buffer.putLong(pos, l);
        return this;
    }

    public DynamicByteBuffer putFloat(float f) {
        buffer.putFloat(f);
        return this;
    }

    public DynamicByteBuffer putDouble(double d) {
        buffer.putDouble(d);
        return this;
    }

    public DynamicByteBuffer putByteBuffer(ByteBuffer byteBuffer) {
        buffer.put(byteBuffer);
        return this;
    }

    public DynamicByteBuffer put(ByteBuffer src) {
        buffer.put(src);
        return this;
    }

    public DynamicByteBuffer put(int index, byte b) {
        buffer.put(index, b);
        return this;
    }

    public DynamicByteBuffer putShort(int index, short s) {
        buffer.putShort(index, s);
        return this;
    }

    public DynamicByteBuffer putFloat(int index, float f) {
        buffer.putFloat(index, f);
        return this;
    }

    public DynamicByteBuffer putDouble(int index, double d) {
        buffer.putDouble(index, d);
        return this;
    }

    public byte get() {
        return buffer.get();
    }

    public byte get(int index) {
        return buffer.get(index);
    }

    public DynamicByteBuffer get(byte[] dst) {
        buffer.get(dst);
        return this;
    }

    public DynamicByteBuffer get(byte[] dst, int offset, int length) {
        buffer.get(dst, offset, length);
        return this;
    }

    public short getShort() {
        return buffer.getShort();
    }

    public short getShort(int index) {
        return buffer.getShort(index);
    }

    public int getInt() {
        return buffer.getInt();
    }

    public int getInt(int index) {
        return buffer.getInt(index);
    }

    public long getLong() {
        return buffer.getLong();
    }

    public long getLong(int index) {
        return buffer.getLong(index);
    }

    public float getFloat() {
        return buffer.getFloat();
    }

    public float getFloat(int index) {
        return buffer.getFloat(index);
    }

    public double getDouble() {
        return buffer.getDouble();
    }

    public double getDouble(int index) {
        return buffer.getDouble(index);
    }

    public DynamicByteBuffer clear() {
        buffer.clear();
        return this;
    }

    public DynamicByteBuffer compact() {
        buffer.compact();
        return this;
    }

    public DynamicByteBuffer mark() {
        buffer.mark();
        return this;
    }

    public DynamicByteBuffer reset() {
        buffer.reset();
        return this;
    }

    public int limit() {
        return buffer.limit();
    }

    public DynamicByteBuffer limit(int newLimit) {
        buffer.limit(newLimit);
        return this;
    }

    public int remaining() {
        return buffer.remaining();
    }

    public boolean hasRemaining() {
        return buffer.hasRemaining();
    }

    public ByteOrder order() {
        return buffer.order();
    }

    public DynamicByteBuffer order(ByteOrder order) {
        buffer.order(order);
        return this;
    }

    /** View over the backing buffer — detaches if ensureCapacity reallocates. */
    public ByteBuffer duplicate() {
        return buffer.duplicate();
    }

    /** View over the backing buffer — detaches if ensureCapacity reallocates. */
    public ByteBuffer slice() {
        return buffer.slice();
    }

    @Override
    public void setOnDispose(Runnable onDispose) {
        this.onDispose = onDispose;
    }

    @Override
    public Runnable getOnDispose() {
        return onDispose;
    }

    @Override
    public void dispose() {
        this.onDispose.run();
    }

    // destroy() is inherited from ArenaByteBuffer: it closes the arena (deterministic free).
    // A Cleaner in ArenaByteBuffer also closes the arena if the buffer is dropped without destroy().

    @Override
    public void close() {
        dispose();
    }
}
