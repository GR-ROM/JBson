package su.grinev.pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Quarantine mode: a released buffer is destroyed instead of recycled.
 *
 * <p>It exists for the one ownership failure nothing else can see. Normally a disposed buffer goes
 * straight back out to the next caller, so a holder that kept a stale reference writes into a live
 * packet belonging to somebody else — no exception, no counter, just corrupted traffic surfacing
 * somewhere unrelated. Reference counting cannot catch it (the count is a perfectly healthy 1, just
 * not yours), and neither can the leak detector (nothing leaked — it was released, twice over).
 *
 * <p>So the mode removes the reuse: memory that has been released is never handed out again, and a
 * stale write lands on a dead object at the offending line. That is the same bargain a
 * leak-checking allocator offers, and it comes with the same caveat — a pool that never reuses
 * anything is not a pool, so this is something you switch on while hunting, never something you
 * ship. Hence its own JVM: {@code ./gradlew quarantine}.
 */
@Timeout(60)
class PoolQuarantineTest {

    private static DisposablePool<DynamicByteBuffer> pool(String name) {
        return new DisposablePool<>(name, () -> new DynamicByteBuffer(64, true), 0, 8, false, 0);
    }

    /** Every test here is about behaviour that only exists with the mode on. */
    private static void requireQuarantine() {
        assumeTrue(LeakDetector.quarantineEnabled(),
                "run with -Djbson.pool.quarantine=true (./gradlew quarantine)");
    }

    /**
     * The bug this mode exists to catch: someone disposes a buffer and keeps using it. Without
     * quarantine the write silently lands in whatever the pool handed out next.
     */
    @Test
    void writingToABufferAfterReleasingItThrows() {
        requireQuarantine();
        DisposablePool<DynamicByteBuffer> pool = pool("quarantine-use-after-release");

        DynamicByteBuffer buffer = pool.get();
        buffer.getBuffer().clear().putInt(1);
        buffer.release();

        // Through the accessor: a clear explanation of what happened.
        IllegalReferenceCountException viaAccessor =
                assertThrows(IllegalReferenceCountException.class, buffer::getBuffer);
        assertTrue(viaAccessor.getMessage().contains("use after release"), viaAccessor.getMessage());

        // And through the delegating facade, which writes to the backing buffer directly: the
        // zero-length stand-in means the write cannot land anywhere.
        assertThrows(RuntimeException.class, () -> buffer.putInt(2));
    }

    /** The memory is gone for good — no later checkout may be handed it. */
    @Test
    void aQuarantinedBufferIsNeverHandedOutAgain() {
        requireQuarantine();
        DisposablePool<DynamicByteBuffer> pool = pool("quarantine-no-reuse");

        DynamicByteBuffer first = pool.get();
        first.release();

        DynamicByteBuffer second = pool.get();
        assertNotSame(first, second, "a released buffer must not come back while quarantine is on");
        assertEquals(0, pool.getIdle(), "and nothing is idle, because nothing was recycled");
        second.getBuffer().clear().putInt(7);   // the fresh one is perfectly usable
    }

    /**
     * The stale holder must not be able to reach a live buffer's memory. Without quarantine this is
     * exactly the sequence that corrupts traffic: release, the pool re-issues, the old owner writes.
     */
    @Test
    void aStaleHolderCannotReachTheNextOwnersMemory() {
        requireQuarantine();
        DisposablePool<DynamicByteBuffer> pool = pool("quarantine-no-crosstalk");

        DynamicByteBuffer stale = pool.get();
        stale.release();

        DynamicByteBuffer live = pool.get();
        live.getBuffer().clear().putInt(0, 0xCAFE);

        assertThrows(RuntimeException.class, () -> stale.putInt(0, 0xDEAD));
        assertEquals(0xCAFE, live.getBuffer().getInt(0), "the live buffer must be untouched");
    }

    /** Refcount semantics are unchanged by the mode — it decides recycling, not ownership. */
    @Test
    void referenceCountingBehavesTheSame() {
        requireQuarantine();
        DisposablePool<DynamicByteBuffer> pool = pool("quarantine-refcount");

        DynamicByteBuffer buffer = pool.get();
        buffer.retain();
        assertEquals(2, buffer.refCnt());
        buffer.release();
        assertEquals(1, buffer.refCnt(), "a second owner still keeps it alive");
        buffer.getBuffer().clear().putInt(3);   // still usable: it was never released to zero

        buffer.release();
        assertThrows(IllegalReferenceCountException.class, buffer::getBuffer);
    }
}
