package su.grinev.pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reference counting on pooled buffers, and the leak report that falls out of it.
 *
 * <p>The rule these tests pin is the one a single-owner pool cannot express: a buffer goes back only
 * when the <i>last</i> owner lets go. Without it, handing a buffer to a second consumer has two
 * failure modes and no correct outcome — both release and the buffer is recycled under a live reader,
 * or neither does and the pool silently starts allocating replacements.
 */
@Timeout(60)
class PooledBufferRefCountTest {

    private static DisposablePool<DynamicByteBuffer> pool(String name) {
        return new DisposablePool<>(name, () -> new DynamicByteBuffer(64, true), 1, 8, false, 0);
    }

    @Test
    void aCheckedOutBufferHasExactlyOneOwner() {
        DisposablePool<DynamicByteBuffer> pool = pool("refcount-basic");

        DynamicByteBuffer buffer = pool.get();
        assertEquals(1, buffer.refCnt(), "a buffer leaves the pool with one owner");
        assertEquals(1, pool.getInFlight());

        assertTrue(buffer.release(), "the only owner releasing recycles the buffer");
        assertEquals(0, buffer.refCnt());
        assertEquals(0, pool.getInFlight(), "and the pool has it back");
    }

    @Test
    void aSecondOwnerKeepsTheBufferOutOfThePool() {
        DisposablePool<DynamicByteBuffer> pool = pool("refcount-shared");

        DynamicByteBuffer buffer = pool.get();
        assertSame(buffer, buffer.retain(), "retain returns the buffer so it can be chained");
        assertEquals(2, buffer.refCnt());

        assertFalse(buffer.release(), "one of two owners letting go must not recycle it");
        assertEquals(1, pool.getInFlight(), "the second owner is still reading it");

        assertTrue(buffer.release(), "the last owner letting go recycles it");
        assertEquals(0, pool.getInFlight());
    }

    @Test
    void disposeIsTheSameThingAsReleasingOneOwner() {
        DisposablePool<DynamicByteBuffer> pool = pool("refcount-dispose");

        DynamicByteBuffer buffer = pool.get();
        buffer.retain();
        buffer.dispose();
        assertEquals(1, buffer.refCnt(), "dispose() drops one owner, not all of them");
        assertEquals(1, pool.getInFlight());

        buffer.dispose();
        assertEquals(0, pool.getInFlight());
    }

    /**
     * The failure this replaces: a double release put one buffer into the idle queue twice, two
     * callers were handed the same memory, and the corruption surfaced somewhere else entirely.
     */
    @Test
    void releasingTwiceThrowsInsteadOfCorruptingThePool() {
        DisposablePool<DynamicByteBuffer> pool = pool("refcount-double");

        DynamicByteBuffer buffer = pool.get();
        assertTrue(buffer.release());

        assertThrows(IllegalReferenceCountException.class, buffer::release,
                "releasing an already-recycled buffer must be loud");
        assertEquals(1, pool.getIdle(), "and must not have put it in the queue a second time");
    }

    @Test
    void retainingARecycledBufferThrows() {
        DisposablePool<DynamicByteBuffer> pool = pool("refcount-revive");

        DynamicByteBuffer buffer = pool.get();
        buffer.release();

        assertThrows(IllegalReferenceCountException.class, buffer::retain,
                "the buffer may already belong to someone else");
        assertEquals(0, buffer.refCnt(), "a failed retain must not leave the count raised");
    }

    @Test
    void aRecycledBufferComesBackOutWithAFreshCount() {
        DisposablePool<DynamicByteBuffer> pool = pool("refcount-reuse");

        DynamicByteBuffer first = pool.get();
        first.release();

        DynamicByteBuffer second = pool.get();
        assertSame(first, second, "the pool should have recycled the same instance");
        assertEquals(1, second.refCnt(), "and re-armed it for its new owner");
        assertTrue(second.release());
    }

    /**
     * The whole point of the exercise: a buffer that is taken and dropped is reported, with the pool
     * that lost it named, instead of quietly turning the pool into an allocator.
     */
    @Test
    void aDroppedBufferIsReportedAsALeak() {
        DisposablePool<DynamicByteBuffer> pool =
                new DisposablePool<>("leaky-pool", () -> new DynamicByteBuffer(64, true), 0, 8, false, 0);

        // Leaks are found by sampling — one checkout in 128 is watched, because watching every one
        // would put an allocation and a registry write on the hot path this pool exists to keep clear.
        // So the test leaks the way a real bug does, repeatedly, and asserts the detector catches it
        // within a few hundred occurrences rather than on the first.
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline && pool.getLeakCount() == 0) {
            for (int i = 0; i < 256; i++) {
                pool.get();   // taken, dropped, never released: the bug being detected
            }
            System.gc();
            // A checkout is what drains the reference queue, so the report surfaces on the next one —
            // exactly as in production.
            pool.get().release();
            if (pool.getLeakCount() == 0) {
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        assertTrue(pool.getLeakCount() > 0,
                "buffers collected without being released must be reported by their pool");
    }

    @Test
    void aBufferThatIsReturnedIsNeverReportedAsALeak() {
        DisposablePool<DynamicByteBuffer> pool = pool("tidy-pool");

        for (int i = 0; i < 200; i++) {
            DynamicByteBuffer buffer = pool.get();
            buffer.touch("write path");   // a no-op below ADVANCED, must stay harmless
            buffer.release();
            System.gc();
        }

        assertEquals(0, pool.getLeakCount(), "correct use must never produce a leak report");
    }

    @Test
    void touchIsSafeOnABufferNobodyIsWatching() {
        DynamicByteBuffer unpooled = new DynamicByteBuffer(16, true);

        assertSame(unpooled, unpooled.touch("hint"));
        assertNotSame(null, unpooled.touch());
    }
}
