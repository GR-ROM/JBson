package su.grinev.pool;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * High-performance object pool backed by one or more {@link ArrayBlockingQueue} shards.
 *
 * <p><b>Single shard (the default).</b> Under contention a single short-held lock
 * dramatically outperforms a lock-free CAS stack (see PoolBenchmark): contending
 * threads park briefly instead of spinning on multiple contended atomics.
 *
 * <p><b>Sharding.</b> That parking is still parking: with several hot threads on one
 * pool the queue's lock becomes the contention point (visible as
 * {@code AbstractQueuedSynchronizer$ExclusiveNode} allocation in an allocation
 * profile — an uncontended lock never enqueues a waiter node). Passing
 * {@code shards > 1} gives each thread its own queue, chosen once per thread, so
 * threads that never collide on a shard never contend at all.
 *
 * <p><b>Why shards must steal.</b> A pooled buffer is frequently acquired on one thread
 * and released on another — a TUN reader takes it, an I/O thread hands it back. Pure
 * per-thread affinity would therefore drain the producer's shard and overfill the
 * consumer's forever. So {@link #get()} falls back to scanning sibling shards before
 * creating a new object, and {@link #release} falls back to scanning siblings before
 * giving up on a full shard. The fallbacks only run off the fast path, so the
 * uncontended case stays a single {@code poll()} / {@code offer()}.
 *
 * <p><b>What sharding does not remove.</b> The {@code isUse} / {@code totalCreated}
 * counters stay global: they are single atomic adds (no waiter nodes, no parking) and
 * the double-release detection depends on {@code isUse} being exact. Sharding targets
 * the queue lock, not those counters.
 *
 * <p>Concurrency limit: when {@code blocking} is enabled, at most {@code maxSize}
 * objects may be in flight at once — further {@link #get()} calls block (up to
 * {@code timeoutMs}, or indefinitely if {@code timeoutMs <= 0}) until a release.
 * When blocking is disabled there is no limit: {@link #get()} always succeeds,
 * creating a fresh object if every shard is empty (the fast path — no semaphore).
 *
 * <p>{@code maxSize} bounds the idle objects the pool retains. It is divided evenly
 * across shards (rounding up), so a sharded pool may retain up to {@code shards - 1}
 * idle objects more than an unsharded one with the same {@code maxSize}.
 */
public class FastPool<T> implements Trimmable {
    public final String name;
    private final ArrayBlockingQueue<T>[] shardQueues;
    private final int shards;
    private final Supplier<T> supplier;
    private final Consumer<T> destroyer;
    @Getter
    private final int minSize;
    private final int maxSize;
    private final boolean blocking;
    private final int timeoutMs;
    private final Semaphore permits;            // non-null only when blocking — enforces the in-flight limit
    private final AtomicInteger isUse = new AtomicInteger(0);
    private final AtomicLong totalCreated = new AtomicLong(0);
    // Objects dropped by release() because every shard was full, i.e. the pool is over its idle
    // ceiling. Each drop costs twice: the object's memory is garbage until the GC runs, and the next
    // get() has to allocate a replacement — so a pool that is churning shows up as this counter
    // climbing while getCurrentPoolSize() sits pinned at the ceiling. Nothing recorded it before,
    // which made "is this pool sized right?" unanswerable from the outside.
    private final LongAdder droppedOnRelease = new LongAdder();

    // Shard affinity: assigned once per thread, round-robin, so the first `shards` threads to touch
    // the pool land on distinct shards. Resolved with a ThreadLocal rather than (threadId % shards)
    // because thread ids are not dense — a modulo can collide two hot threads onto one shard and
    // silently give back the contention sharding was meant to remove.
    private final AtomicInteger shardCursor = new AtomicInteger(0);
    private final ThreadLocal<Integer> shardIndex;   // assigned in the constructor: needs `shards`

    // Debug: per-buffer checkout tracking to localize double-release / double-handout (buffer-ownership bugs).
    // Enabled with -Dfastpool.track=true (off by default — the identity-set ops add per-get/release lock overhead).
    private static final Logger log = LoggerFactory.getLogger(FastPool.class);
    private static final boolean TRACK = Boolean.getBoolean("fastpool.track");
    private final Set<T> checkedOut = TRACK ? Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>())) : null;

    /** Unsharded (shards = 1) — the historical behaviour. */
    public FastPool(String name, Supplier<T> supplier, Consumer<T> destroyer,
                    int initialSize, int maxSize, boolean blocking, int timeoutMs) {
        this(name, supplier, destroyer, initialSize, maxSize, blocking, timeoutMs, 1);
    }

    /**
     * @param shards number of independent queues; {@code <= 1} means unsharded. Size it to the number
     *               of threads that hit this pool on the hot path — more shards than threads only
     *               spreads the idle objects thinner and makes stealing more likely.
     */
    @SuppressWarnings("unchecked")
    public FastPool(String name, Supplier<T> supplier, Consumer<T> destroyer,
                    int initialSize, int maxSize, boolean blocking, int timeoutMs, int shards) {
        this.name = name;
        this.shards = Math.max(1, shards);
        this.supplier = supplier;
        this.destroyer = destroyer;
        this.maxSize = maxSize;
        this.blocking = blocking;
        this.timeoutMs = timeoutMs;
        this.permits = blocking ? new Semaphore(maxSize) : null;
        this.minSize = initialSize;

        int shardCount = this.shards;
        this.shardIndex = shardCount == 1
                ? ThreadLocal.withInitial(() -> 0)
                : ThreadLocal.withInitial(() -> Math.floorMod(shardCursor.getAndIncrement(), shardCount));

        int perShard = Math.max(1, (Math.max(maxSize, 1) + this.shards - 1) / this.shards);
        this.shardQueues = new ArrayBlockingQueue[this.shards];
        for (int i = 0; i < this.shards; i++) {
            this.shardQueues[i] = new ArrayBlockingQueue<>(perShard);
        }

        // Prefill round-robin so every shard starts warm; a thread's first get() then hits its own
        // shard instead of stealing or allocating.
        for (int i = 0; i < initialSize; i++) {
            shardQueues[i % this.shards].offer(supplier.get());
            totalCreated.incrementAndGet();
        }
    }

    /**
     * Convenience constructor: a non-blocking, effectively-unbounded, unsharded pool with no
     * destroyer (trimmed/dropped objects are left to the GC). {@code maxSize} bounds
     * only the idle queue.
     */
    public FastPool(Supplier<T> supplier, int initialSize, int maxSize) {
        this("fast-pool", supplier, t -> {}, initialSize, maxSize, false, 0, 1);
    }

    /** Number of independent queues backing this pool (1 = unsharded). */
    public int getShards() {
        return shards;
    }

    /**
     * Idle objects held by one shard. Package-private: the split is an implementation detail, but
     * without it a test cannot tell "stole from a sibling" from "allocated a new object", nor assert
     * that shards stay balanced.
     */
    int idleInShard(int shard) {
        return shardQueues[shard].size();
    }

    /** The shard this thread is pinned to — package-private, for tests that need determinism. */
    int shardOfCurrentThread() {
        return shardIndex.get();
    }

    public T get() {
        if (permits != null) {
            acquirePermit();
        }
        isUse.incrementAndGet();
        T item = takeFromShards();
        if (item == null) {
            totalCreated.incrementAndGet();
            item = supplier.get();
        }
        if (checkedOut != null && !checkedOut.add(item)) {
            // Same buffer handed out while already checked out -> it sat in the queue twice,
            // i.e. it was double-released earlier. This is the corruption: two owners, one buffer.
            log.error("FastPool '{}' DOUBLE-HANDOUT: buffer id={} given out while still checked out. get site:",
                    name, System.identityHashCode(item), new Throwable("get"));
        }
        return item;
    }

    /**
     * Own shard first (the fast path: one uncontended {@code poll()}), then siblings. Stealing exists
     * because get and release routinely happen on different threads, which would otherwise drain one
     * shard while another sits full.
     */
    private T takeFromShards() {
        int own = shardIndex.get();
        T item = shardQueues[own].poll();
        if (item != null || shards == 1) {
            return item;
        }
        for (int i = 1; i < shards; i++) {
            item = shardQueues[(own + i) % shards].poll();
            if (item != null) {
                return item;
            }
        }
        return null;
    }

    private void acquirePermit() {
        if (permits.tryAcquire()) {
            return; // fast path: a permit was immediately available (no parking, just a CAS)
        }
        if (timeoutMs > 0) {
            try {
                if (!permits.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
                    throw new IllegalStateException("Pool '" + name + "' timeout after " + timeoutMs + "ms (limit=" + maxSize + ")");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Pool '" + name + "' interrupted while waiting for a free object", e);
            }
        } else {
            permits.acquireUninterruptibly();
        }
    }

    /**
     * Returns an object to the pool. A detected double release (more releases than gets)
     * is logged as a warning and swallowed: the object is NOT re-offered, so it cannot end
     * up idle in the pool twice. Set {@code -Dfastpool.strict=true} to restore the old
     * fail-fast behaviour (throw {@link IllegalStateException}) — useful in tests/CI.
     */
    public void release(T item) {
        if (checkedOut != null && !checkedOut.remove(item)) {
            // Releasing a buffer that is NOT currently checked out -> it was already released
            // (double-release / double-dispose). This stack is the second, offending release site.
            log.error("FastPool '{}' DOUBLE-RELEASE: buffer id={} released but not checked out. release site:",
                    name, System.identityHashCode(item), new Throwable("release"));
        }
        if (isUse.decrementAndGet() < 0) {
            // More releases than gets — a double release (or releasing a non-borrowed object).
            isUse.incrementAndGet();
            log.warn("Double release detected in pool '{}' — object not returned to the pool. Release site:",
                    name, new Throwable("release"));
            // Property read only on this cold error path — never on the release fast path.
            if (Boolean.getBoolean("fastpool.strict")) {
                throw new IllegalStateException("Double release detected in pool '" + name + "'");
            }
            return;
        }
        offerToShards(item);
        if (permits != null) {
            permits.release();
        }
    }

    /**
     * Own shard first, then siblings if it is full. Without the sibling scan a thread that only ever
     * releases (the consumer end of a cross-thread handoff) would fill its shard and then start
     * discarding objects while other shards sat empty.
     */
    private void offerToShards(T item) {
        int own = shardIndex.get();
        if (shardQueues[own].offer(item)) {
            return;
        }
        // The sibling scan is what "shards > 1" buys; unsharded, a full own shard is already the end
        // of the line. Written as a guarded loop rather than an early `|| shards == 1` return so that
        // both cases fall through to the same drop accounting below.
        if (shards > 1) {
            for (int i = 1; i < shards; i++) {
                if (shardQueues[(own + i) % shards].offer(item)) {
                    return;
                }
            }
        }
        // Every shard full: the pool is over its idle ceiling, drop the object on the floor.
        droppedOnRelease.increment();
    }

    /**
     * How many objects {@link #release} has thrown away because the pool was already at its idle
     * ceiling. Steadily non-zero means the ceiling is below the real in-flight demand: the pool is
     * allocating and discarding on the hot path instead of recycling.
     */
    public long getDroppedOnRelease() {
        return droppedOnRelease.sum();
    }

    // --- Trimmable ---

    @Override
    public String getName() {
        return name;
    }

    /** Objects currently in use (checked out) — the demand signal sampled by the optimizer. */
    @Override
    public int getCountInUse() {
        return isUse.get();
    }

    /** Objects currently idle (available) across every shard. */
    @Override
    public int getIdle() {
        int idle = 0;
        for (ArrayBlockingQueue<T> shard : shardQueues) {
            idle += shard.size();
        }
        return idle;
    }

    @Override
    public boolean isTrimmable() {
        return true;
    }

    /**
     * Frees exactly {@code size} idle objects, or nothing at all. Drains across shards (largest
     * first, so trimming evens them out rather than emptying one); if the shards together cannot
     * supply {@code size}, everything drained is put back and the call reports false.
     */
    @Override
    public boolean trim(int size) {
        List<T> items = new ArrayList<>(size);
        // Largest shard first: a partially-drained pool should end up balanced, not lopsided.
        Integer[] order = new Integer[shards];
        for (int i = 0; i < shards; i++) {
            order[i] = i;
        }
        Arrays.sort(order, Comparator.comparingInt((Integer i) -> shardQueues[i].size()).reversed());

        int drained = 0;
        for (int idx : order) {
            if (drained >= size) {
                break;
            }
            drained += shardQueues[idx].drainTo(items, size - drained);
        }
        if (drained < size) {
            items.forEach(this::offerToShards);
            return false;
        }
        items.forEach(destroyer);
        return true;
    }

    // --- BasePool-compatible accessors (read-only) ---

    public int getInFlight() {
        return isUse.get();
    }

    public int getAvailable() {
        return getIdle();
    }

    /** Owned objects: in use + idle in the pool. */
    public int getCurrentPoolSize() {
        return isUse.get() + getIdle();
    }

    public int size() {
        return getIdle();
    }

    public long getTotalCreated() {
        return totalCreated.get();
    }
}
