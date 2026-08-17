package su.grinev.pool;

/**
 * A pooled object whose lifetime is decided by how many owners still hold it.
 *
 * <p>A pool without this has exactly one rule: whoever took the object gives it back, once. That rule
 * breaks the moment an object is handed to a second consumer — the two either both release it (the
 * object is recycled while one of them still reads it, and the next owner's writes appear in the
 * middle of someone else's data) or neither does (the object is dropped and the pool quietly starts
 * allocating replacements). Both failures are silent, and both have happened here.
 *
 * <p>So: an object handed out by a pool starts at {@code refCnt == 1}. {@link #retain()} adds an
 * owner, {@link #release()} removes one, and the object returns to its pool when the count reaches
 * zero. Releasing below zero is a bug in the caller and throws rather than corrupting the pool.
 *
 * <p>Deliberately <b>not</b> {@code AutoCloseable}: try-with-resources implies a single lexical owner,
 * which is exactly the case that never needed reference counting.
 */
public interface RefCounted {

    /** Owners currently holding this object. Zero means it has been recycled and must not be touched. */
    int refCnt();

    /** Records one more owner. */
    RefCounted retain();

    /**
     * Drops one owner.
     *
     * @return true if this call took the count to zero and the object was recycled
     * @throws IllegalReferenceCountException if the object was already fully released
     */
    boolean release();

    /**
     * Records where the object was last handled, for the leak report. A no-op unless leak detection
     * is running at {@code advanced} or higher — at which point the hint is what turns "a buffer from
     * this pool leaked" into "and the last thing that touched it was the TUN write path".
     */
    RefCounted touch(Object hint);

    /** Equivalent to {@code touch(null)}. */
    default RefCounted touch() {
        return touch(null);
    }
}
