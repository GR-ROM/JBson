package su.grinev.pool;

import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The pool's steady-state allocation, measured rather than asserted by inspection.
 *
 * <p>This exists because the property is easy to lose and invisible when you do. A pooled buffer's
 * whole purpose is that a packet costs no allocation; the last regression here was a lambda rebuilt
 * on every checkout, which a profiler eventually attributed 8% of all allocations to. Reference
 * counting and leak detection both sit directly on that path, so they get a budget and a number.
 *
 * <p>Measured with {@code ThreadMXBean.getThreadAllocatedBytes}, which counts bytes this thread
 * allocated in TLABs — the same accounting a JFR allocation profile reports, without needing one.
 */
@Timeout(120)
class PoolAllocationBudgetTest {

    private static final int WARMUP = 50_000;
    private static final int MEASURED = 500_000;

    /**
     * Bytes per get/release cycle. Leak detection samples 1 checkout in 128 and allocates a tracker
     * (a PhantomReference subclass plus its registry node) for that one, so the floor is not zero —
     * it is roughly {@code 80 / 128} bytes, under a byte per cycle. The budget is set an order of
     * magnitude above that: comfortably above the sampling noise, far below the cost of allocating
     * anything at all per checkout (the smallest object here would be 16 B/cycle).
     */
    private static final double MAX_BYTES_PER_CYCLE = 8.0;

    @Test
    void aGetReleaseCycleAllocatesEssentiallyNothing() {
        ThreadMXBean threads = threadMxBean();
        assumeTrue(threads != null && threads.isThreadAllocatedMemoryEnabled(),
                "per-thread allocation accounting unavailable on this JVM");

        DisposablePool<DynamicByteBuffer> pool =
                new DisposablePool<>("alloc-budget", () -> new DynamicByteBuffer(1500, true), 4, 8, false, 0);

        for (int i = 0; i < WARMUP; i++) {
            cycle(pool);
        }

        long before = threads.getCurrentThreadAllocatedBytes();
        for (int i = 0; i < MEASURED; i++) {
            cycle(pool);
        }
        long allocated = threads.getCurrentThreadAllocatedBytes() - before;
        double perCycle = (double) allocated / MEASURED;

        System.out.printf("pool get/release: %.3f bytes/cycle over %d cycles (leak detection: %s)%n",
                perCycle, MEASURED, LeakDetector.level());
        assumeProductionLevel();
        assertTrue(perCycle < MAX_BYTES_PER_CYCLE,
                "a pooled checkout must not allocate: " + perCycle + " bytes/cycle");
    }

    /**
     * Retain/release must be allocation-free too — it is an atomic on a field, and the point of
     * counting references rather than wrapping the buffer is that sharing costs nothing to express.
     */
    @Test
    void sharingABufferAllocatesNothing() {
        ThreadMXBean threads = threadMxBean();
        assumeTrue(threads != null && threads.isThreadAllocatedMemoryEnabled(),
                "per-thread allocation accounting unavailable on this JVM");

        DisposablePool<DynamicByteBuffer> pool =
                new DisposablePool<>("alloc-share", () -> new DynamicByteBuffer(1500, true), 4, 8, false, 0);

        for (int i = 0; i < WARMUP; i++) {
            shareCycle(pool);
        }

        long before = threads.getCurrentThreadAllocatedBytes();
        for (int i = 0; i < MEASURED; i++) {
            shareCycle(pool);
        }
        double perCycle = (double) (threads.getCurrentThreadAllocatedBytes() - before) / MEASURED;

        System.out.printf("retain + 2 releases: %.3f bytes/cycle%n", perCycle);
        assumeProductionLevel();
        assertTrue(perCycle < MAX_BYTES_PER_CYCLE,
                "handing a buffer to a second owner must not allocate: " + perCycle + " bytes/cycle");
    }

    private static void cycle(DisposablePool<DynamicByteBuffer> pool) {
        DynamicByteBuffer buffer = pool.get();
        buffer.getBuffer().clear().putInt(0x2026);
        buffer.release();
    }

    private static void shareCycle(DisposablePool<DynamicByteBuffer> pool) {
        DynamicByteBuffer buffer = pool.get();
        buffer.retain();
        buffer.release();
        buffer.release();
    }

    /**
     * The budget applies to the levels a node may actually run at. ADVANCED and PARANOID capture a
     * stack per tracked checkout — measured at ~18 and ~2000 bytes/cycle here — and that is the
     * bargain they exist to offer: they are diagnostic levels, turned on to find a leak that SIMPLE
     * has already reported, not settings to leave on under load. The measurement above still prints
     * at every level, so the comparison stays available.
     */
    private static void assumeProductionLevel() {
        assumeTrue(LeakDetector.level() == LeakDetector.Level.DISABLED
                        || LeakDetector.level() == LeakDetector.Level.SIMPLE,
                "allocation budget applies to DISABLED/SIMPLE; " + LeakDetector.level() + " trades bytes for detail");
    }

    private static ThreadMXBean threadMxBean() {
        return ManagementFactory.getThreadMXBean() instanceof ThreadMXBean sun ? sun : null;
    }
}
