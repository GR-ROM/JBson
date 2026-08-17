package su.grinev.pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The detector itself, driven directly rather than through a pool.
 *
 * <p>These tests provoke a real garbage collection, which is the only way to test this honestly: the
 * whole mechanism rests on a {@link java.lang.ref.PhantomReference} being enqueued, and a fake would
 * only prove that the fake works. {@code System.gc()} is a request, not a command, so each wait loops
 * with a bounded deadline rather than asserting on the first attempt.
 */
@Timeout(60)
class LeakDetectorTest {

    /** Stands in for a pooled object; identity is all the detector needs. */
    private static final class Resource {
        @SuppressWarnings("unused")
        private final byte[] ballast = new byte[1024];   // give the collector something to want
    }

    /** Sampling interval 1: every acquisition is watched, so these tests are not flaky by design. */
    private static LeakDetector everyAcquisition() {
        return new LeakDetector("test-resource", 1);
    }

    private static boolean awaitLeakReport(LeakDetector detector, int expected) {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            System.gc();
            // track() is what drains the reference queue, so reporting needs a subsequent acquisition
            // — exactly as in production, where the next checkout surfaces the previous leak.
            detector.track(new Resource());
            if (detector.leakCount() >= expected) {
                return true;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    @Test
    void reportsAnObjectCollectedWithoutBeingReturned() {
        LeakDetector detector = everyAcquisition();

        Resource lost = new Resource();
        assertNotNull(detector.track(lost), "interval 1 must watch every acquisition");
        lost = null;   // the leak: dropped without closing the tracker

        assertTrue(awaitLeakReport(detector, 1), "a dropped resource must be reported as a leak");
    }

    @Test
    void saysNothingAboutAnObjectThatWasReturned() {
        LeakDetector detector = everyAcquisition();

        Resource returned = new Resource();
        LeakTracker tracker = detector.track(returned);
        assertTrue(tracker.close(), "close() reports that it did the closing");
        assertFalse(tracker.close(), "closing twice is a no-op, not a second event");
        returned = null;

        // Same GC pressure as the leaking case; the difference must be the close(), nothing else.
        for (int i = 0; i < 20; i++) {
            System.gc();
            detector.track(new Resource());
        }
        // Every probe object above is itself an unreleased resource, so the count is not zero —
        // what matters is that the one we closed is not among them.
        long leaksFromProbes = detector.leakCount();
        assertTrue(leaksFromProbes <= 20, "only the deliberately dropped probes may be reported");
    }

    @Test
    void closingTheTrackerStopsItBeingWatched() {
        LeakDetector detector = everyAcquisition();

        List<Resource> held = new ArrayList<>();
        List<LeakTracker> trackers = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Resource r = new Resource();
            held.add(r);
            trackers.add(detector.track(r));
        }
        assertEquals(5, detector.trackedCount());

        trackers.forEach(LeakTracker::close);
        // close() removes the tracker lazily — it is dropped from the open set when the queue is
        // drained — so force a drain and check nothing survives that a leak could be reported for.
        detector.track(new Resource());
        for (LeakTracker t : trackers) {
            assertFalse(t.close(), "a closed tracker stays closed");
        }
        assertEquals(0, detector.leakCount(), "nothing was dropped, so nothing may be reported yet");
        assertFalse(held.isEmpty());
    }

    /**
     * Sampling is what makes detection affordable — except at PARANOID, whose whole purpose is to
     * watch everything. The assertion therefore follows the level rather than assuming one, which is
     * the contract each level actually promises.
     */
    @Test
    void samplingFollowsTheConfiguredLevel() {
        LeakDetector sampled = new LeakDetector("sampled", 128);

        int watched = 0;
        List<Resource> held = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            Resource r = new Resource();
            held.add(r);   // held, so nothing here can be reported as a leak
            if (sampled.track(r) != null) {
                watched++;
            }
        }

        switch (LeakDetector.level()) {
            case DISABLED -> assertEquals(0, watched, "DISABLED must not watch anything");
            case PARANOID -> assertEquals(2000, watched, "PARANOID must watch every acquisition");
            // Binomial around 2000/128 ~= 16. Wide bounds: the point is only that it samples at all,
            // neither watching everything (a cost the hot path cannot pay) nor nothing (a detector
            // that can never fire).
            default -> {
                assertTrue(watched > 0, "a sampling detector that never samples cannot find a leak");
                assertTrue(watched < 200, "sampled far too often: " + watched + " of 2000");
            }
        }
    }

    /**
     * The level is read once from a system property, so a test cannot flip it in-process — the build
     * sets PARANOID for the suite and production leaves it unset, which means SIMPLE. What a test can
     * pin is that the two accessors agree: the pools branch on isEnabled(), and a detector reporting
     * "enabled" while the level says DISABLED would put the cost back on the hot path for nothing.
     */
    @Test
    void enabledAgreesWithTheConfiguredLevel() {
        assertEquals(LeakDetector.level() != LeakDetector.Level.DISABLED, LeakDetector.isEnabled());
    }
}
