package su.grinev.pool;


import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A {@link Disposable}, arena-backed native byte buffer.
 *
 * The backing native segment is allocated once, up front, at the requested
 * capacity — sized for the buffer's expected high-water mark. Writing into it
 * never reallocates as long as the content fits, which is the intended (and
 * common) case for a pooled buffer reused many times.
 *
 * Release mode (see {@link Release}):
 * <ul>
 *   <li><b>AUTO</b> (default) — a direct {@link ByteBuffer}, freed by the GC when the buffer becomes
 *       unreachable; the segment is a view of it. {@link #destroy()} is a no-op. Safe by construction —
 *       a dropped buffer never leaks. Deliberately <i>not</i> an {@code Arena.ofAuto()} segment: a
 *       session-backed buffer makes every {@code read/write(ByteBuffer[])} allocate (see
 *       {@code allocateAuto}). This is also the only mode that does not require the FFM
 *       {@code Arena.of{Shared,Confined}} API, so it is the path a Java 21 build uses.</li>
 *   <li><b>MANUAL</b> — {@code Arena.ofShared()}: {@link #destroy()} frees the segment deterministically.
 *       A {@link Cleaner} closes the arena if the buffer is dropped without {@code destroy()} (ofShared
 *       memory is otherwise never reclaimed by the GC). Use only when you take explicit ownership and want
 *       to reclaim native memory the instant you are done with it.</li>
 * </ul>
 *
 * {@link #ensureCapacity(int)} is only a safety net: if the content ever outgrows the preallocated
 * capacity it allocates a larger segment from a fresh arena and copies the content over (MANUAL closes the
 * old arena immediately; AUTO leaves it to the GC). FFM segments cannot be resized in place.
 *
 * {@link #dispose()} recycles the buffer back to its pool — orthogonal to the release mode.
 */
public class ArenaByteBuffer implements Disposable, RefCounted {

    /** Native-memory reclamation strategy. */
    public enum Release { AUTO, MANUAL }

    private static final Cleaner CLEANER = Cleaner.create();

    /**
     * Holds the live arena for the {@link Cleaner} (MANUAL mode only). Kept in a separate object that does
     * NOT reference the enclosing {@link ArenaByteBuffer} (otherwise the buffer could never become
     * unreachable and the cleaner would never run).
     */
    private static final class ArenaHolder implements Runnable {
        private Arena arena;

        @Override
        public void run() {
            Arena a = arena;
            arena = null;
            if (a != null) {
                try {
                    a.close();
                } catch (RuntimeException ignored) {
                    // already closed (e.g. by ensureCapacity) — nothing to free
                }
            }
        }
    }

    private final Release release;
    private boolean fixedCapacity;
    private Runnable onDispose;
    private final ArenaHolder holder;          // MANUAL only (null in AUTO)
    private final Cleaner.Cleanable cleanable; // MANUAL only (null in AUTO)
    private Arena arena;
    private MemorySegment segment;

    /**
     * Owners holding this buffer. One at birth and one after every checkout: a buffer that came from
     * a pool has exactly one owner until somebody says otherwise. Recycling is driven from here, not
     * from the {@code dispose()} call site, which is what makes a second consumer expressible at all.
     */
    private final AtomicInteger refCnt = new AtomicInteger(1);
    /** Non-null while the leak detector is watching this checkout — see {@link LeakDetector}. */
    private LeakTracker leakTracker;
    protected ByteBuffer buffer;
    /** Set by {@link #invalidate()}; only ever true in quarantine mode. */
    private boolean invalidated;

    /** What an invalidated buffer points at: any access to it fails, which is the whole idea. */
    private static final ByteBuffer DEAD = ByteBuffer.allocate(0);

    /** GC-managed (AUTO) buffer — the safe default. */
    public ArenaByteBuffer(int capacity) {
        this(capacity, Release.AUTO);
    }

    public ArenaByteBuffer(int capacity, Release release) {
        this.release = release;
        if (release == Release.MANUAL) {
            this.arena = Arena.ofShared();
            this.segment = arena.allocate(capacity);
            this.buffer = segment.asByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
            this.holder = new ArenaHolder();
            this.holder.arena = arena;
            this.cleanable = CLEANER.register(this, holder);
        } else {
            this.arena = null;
            this.buffer = allocateAuto(capacity).order(ByteOrder.LITTLE_ENDIAN);
            this.segment = MemorySegment.ofBuffer(buffer);
            this.holder = null;
            this.cleanable = null;
        }
    }

    /**
     * AUTO memory comes from {@link ByteBuffer#allocateDirect}, not from an arena, and the segment is
     * derived from the buffer rather than the other way round. The difference is invisible to every
     * caller and decisive for the channel: {@code SocketChannel.read/write(ByteBuffer[])} acquires the
     * memory session of every buffer that has one and allocates a releaser per buffer per call to let
     * go of it again — 40 bytes a buffer, 640 a gather-write of sixteen, and on a node that is ~0.8 GB
     * per three minutes, the single largest allocation left once its own were gone. A buffer with no
     * session is the case the JDK special-cases to nothing. The GC still frees the memory, and it is
     * still counted against {@code -XX:MaxDirectMemorySize}, exactly as an auto arena's was.
     */
    private static ByteBuffer allocateAuto(int capacity) {
        return ByteBuffer.allocateDirect(capacity);
    }

    /** Capacity of the preallocated native segment in bytes. Writes up to this size never reallocate. */
    public int capacity() {
        return (int) segment.byteSize();
    }

    /**
     * Declares this buffer's capacity final: {@link #ensureCapacity(int)} then throws instead of growing.
     *
     * For a buffer whose content is bounded by a protocol (an MTU-sized frame, a TLS record, a chunked
     * upload), a growth is not a resize — it is the sizing being wrong, and growing hides that. It also
     * costs more than the allocation: the fresh segment is outside whatever direct-memory budget the
     * pool ceilings were computed against, and every {@code duplicate()}/{@code slice()} view taken
     * earlier silently detaches from the buffer it was meant to alias. Opt-in, because a general
     * serializer writing an unbounded document legitimately needs to grow.
     *
     * @return this, so a pool supplier can read {@code () -> new ArenaByteBuffer(n).fixCapacity()}
     */
    public ArenaByteBuffer fixCapacity() {
        this.fixedCapacity = true;
        return this;
    }

    /** Whether {@link #ensureCapacity(int)} may grow this buffer. */
    public boolean isFixedCapacity() {
        return fixedCapacity;
    }

    /**
     * Safety net for the rare case where content outgrows the preallocated
     * capacity. A no-op — and zero allocations — while the existing capacity
     * suffices, which is the expected path for a properly sized buffer.
     *
     * @throws IllegalStateException if the buffer was declared {@link #fixCapacity() fixed} and the
     *                               content does not fit
     */
    public void ensureCapacity(int additionalCapacity) {
        if (buffer.remaining() >= additionalCapacity) {
            return;
        }
        if (fixedCapacity) {
            throw new IllegalStateException("fixed-capacity buffer cannot grow: need " + additionalCapacity
                    + " B, only " + buffer.remaining() + " B left of " + capacity()
                    + " B (position " + buffer.position() + ")");
        }
        int used = buffer.position();
        int newCapacity = Math.max(capacity() * 2, used + additionalCapacity);
        // Preserve the caller's byte order: msgpack writes BIG_ENDIAN, BSON LITTLE_ENDIAN —
        // hardcoding LE here would silently flip the order of a big-endian stream mid-write.
        ByteOrder order = buffer.order();

        if (release == Release.MANUAL) {
            Arena newArena = Arena.ofShared();
            MemorySegment newSegment = newArena.allocate(newCapacity);
            MemorySegment.copy(segment, 0, newSegment, 0, used);
            Arena oldArena = arena;
            this.arena = newArena;
            this.segment = newSegment;
            this.buffer = newSegment.asByteBuffer().order(order);
            holder.arena = newArena;   // the cleaner now tracks the new arena
            oldArena.close();          // free the old segment now
        } else {
            ByteBuffer newBuffer = allocateAuto(newCapacity).order(order);
            MemorySegment newSegment = MemorySegment.ofBuffer(newBuffer);
            MemorySegment.copy(segment, 0, newSegment, 0, used);
            this.segment = newSegment;
            this.buffer = newBuffer;   // the old one is left to the GC
        }
        this.buffer.position(used);
    }

    /**
     * The backing native segment, for callers that need FFM rather than {@code ByteBuffer} — the
     * Vector API reads from a segment, and {@code MemorySegment.ofBuffer} on every call would put an
     * allocation on the hot path. Returned as-is rather than sliced, so offsets match
     * {@link #getBuffer()}'s absolute indices.
     *
     * <p>Follows the buffer across {@link #ensureCapacity}: a grown buffer is a new segment, so do not
     * cache this across a possible growth (fixed-capacity buffers cannot grow, see {@code fixCapacity}).
     */
    public MemorySegment memorySegment() {
        return segment;
    }

    /** Native base address of the backing segment. */
    public long address() {
        return segment.address();
    }

    /** Whether the backing native memory is still allocated (MANUAL: arena not yet closed; AUTO: until GC). */
    public boolean isAlive() {
        return segment.scope().isAlive();
    }

    // ---- reference counting -------------------------------------------------

    @Override
    public int refCnt() {
        return refCnt.get();
    }

    @Override
    public ArenaByteBuffer retain() {
        int current = refCnt.getAndIncrement();
        if (current <= 0) {
            // Already recycled: the buffer this caller thinks it is sharing may already belong to
            // someone else. Undo and fail here rather than hand out a second owner of a live buffer.
            refCnt.decrementAndGet();
            throw new IllegalReferenceCountException("retain() on a buffer already returned to the pool");
        }
        return this;
    }

    @Override
    public boolean release() {
        int remaining = refCnt.decrementAndGet();
        if (remaining > 0) {
            return false;
        }
        if (remaining < 0) {
            refCnt.incrementAndGet();
            throw new IllegalReferenceCountException(0, 1);
        }
        recycle();
        return true;
    }

    @Override
    public ArenaByteBuffer touch(Object hint) {
        LeakTracker tracker = leakTracker;
        if (tracker != null) {
            tracker.record(hint);
        }
        return this;
    }

    /**
     * The backing buffer.
     *
     * <p>The check is folded away unless quarantine is on, and exists so a stale access reports what
     * actually happened instead of an {@link IndexOutOfBoundsException} from the zero-length stand-in.
     */
    public ByteBuffer getBuffer() {
        if (LeakDetector.quarantineEnabled() && invalidated) {
            throw new IllegalReferenceCountException(
                    "use after release: this buffer was returned to its pool and destroyed "
                            + "(quarantine mode). Whoever still holds a reference to it should not.");
        }
        return buffer;
    }

    /**
     * Kills the buffer instead of recycling it: the memory is freed and the facade is pointed at a
     * zero-length stand-in, so every later access fails — through {@link #getBuffer()} with an
     * explanation, and through the delegating methods with a bounds error at the offending line.
     *
     * <p>Only ever called in quarantine mode, by the pool, in place of returning the object to a
     * shard. Not reversible: a quarantined buffer is never handed out again, which is exactly what
     * makes a stale write impossible to mistake for a valid one.
     */
    void invalidate() {
        invalidated = true;
        this.buffer = DEAD;
        destroy();
    }

    /** Hands the buffer back to its pool. Called only when the last owner let go. */
    private void recycle() {
        LeakTracker tracker = leakTracker;
        if (tracker != null) {
            leakTracker = null;
            tracker.close();
        }
        if (onDispose != null) {
            onDispose.run();
        }
    }

    /**
     * Re-arms the buffer for a new owner as it leaves the pool, and starts watching it. Called by the
     * pool, never by application code — a buffer that is already checked out has a live owner, and
     * resetting the count under them is how two owners end up believing they are the only one.
     */
    void reviveForCheckout(LeakDetector detector) {
        refCnt.set(1);
        this.leakTracker = detector == null ? null : detector.track(this);
    }

    /** Closes the leak tracker when the pool takes the buffer back without going through release(). */
    void checkedIn() {
        LeakTracker tracker = leakTracker;
        if (tracker != null) {
            leakTracker = null;
            tracker.close();
        }
    }

    @Override
    public void setOnDispose(Runnable onDispose) {
        this.onDispose = onDispose;
    }

    @Override
    public Runnable getOnDispose() {
        return onDispose;
    }

    /**
     * Drops this owner's claim. Identical to {@link #release()} for the single-owner case that every
     * existing call site is, and the reason the two are not one method is that {@code dispose()}
     * predates reference counting and reads as "I am done with this" at hundreds of call sites.
     */
    @Override
    public void dispose() {
        release();
    }

    @Override
    public void destroy() {
        // Deterministic free only in MANUAL mode; AUTO is reclaimed by the GC.
        if (release == Release.MANUAL) {
            cleanable.clean();
        }
    }

    @Override
    public void close() {
        dispose();
    }
}
