package su.grinev.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

/**
 * Finds pooled objects that were taken and never given back, at runtime, in production.
 *
 * <p><b>Why this and not a counter.</b> A pool already reports how many objects are in flight, but
 * that number cannot distinguish "busy" from "leaking": a buffer that is taken and dropped keeps the
 * in-use count high forever, and because these buffers are arena-backed the GC eventually frees the
 * native memory anyway. So a leak does not crash, does not throw, and does not even grow memory
 * without bound — it shows up as the pool quietly allocating a replacement per packet, which looks
 * like a healthy pool pinned at its ceiling. That exact failure has cost this project real debugging
 * time (see the note on {@code dropped_on_release_total} in MyVPN's CLAUDE.md).
 *
 * <p><b>How it works.</b> The object is watched through a {@link PhantomReference}. If the GC
 * collects it while its tracker is still open, nobody returned it to the pool, and the tracker —
 * which by then holds the only surviving evidence, the stack that took the object out — is reported.
 * Reports are emitted from {@link #track}, so they surface on the next pool acquisition rather than
 * needing a thread of their own.
 *
 * <p><b>Cost, and why SIMPLE does not capture a stack.</b> Watching every acquisition would put an
 * allocation and a registry write on the hot path, which is the opposite of what a buffer pool is
 * for, so only a sample is watched — one in {@value #DEFAULT_SAMPLING_INTERVAL}. That still leaves
 * the question of what a watched checkout costs, and the answer is dominated by one thing: capturing
 * a stack trace. {@code new Throwable()} walks the stack and is, by a wide margin, the most expensive
 * allocation available to put near a per-packet path — on a data plane that measures its steady-state
 * allocation in kilobytes per second, sampling stacks at 1/128 of a few hundred thousand packets a
 * second is not a rounding error, it is the new dominant term.
 *
 * <p>So SIMPLE allocates a tracker and nothing else (~2 small objects per 128 checkouts, no stack
 * walk) and reports only that a given pool leaked; ADVANCED adds the stack and the hints that say
 * where. Netty splits its levels the same way and for the same reason. The consequence to plan
 * around: finding a leak is a two-step operation — SIMPLE tells you a pool is leaking in production,
 * ADVANCED (on a stand, or briefly) tells you which call site.
 *
 * <p>A leak that happens per packet is caught within a few hundred packets; one that happens once an
 * hour needs {@code PARANOID}.
 *
 * <p>Levels, via {@code -Djbson.leakDetection=<level>}:
 * <ul>
 *   <li>{@code disabled} (default) — no tracking at all; the checks fold away to a constant.</li>
 *   <li>{@code simple} — sample 1/128 and report <i>that</i> a pool is leaking, without capturing a
 *       stack. Cheap enough to switch on in production for a while, but not free: the tracker is a
 *       {@code PhantomReference} in a concurrent set, and at a few hundred thousand checkouts a
 *       second that is hundreds of megabytes an hour on a heap sized in tens of megabytes.</li>
 *   <li>{@code advanced} — sample 1/128, capture the acquisition stack, and record
 *       {@link RefCounted#touch} hints, so the report says <i>where</i> the buffer was taken and what
 *       last handled it. Turn this on once SIMPLE has told you a pool is leaking.</li>
 *   <li>{@code paranoid} — track every acquisition, with hints. For tests and for hunting a leak
 *       that reproduces rarely; far too expensive for a hot path.</li>
 * </ul>
 */
public final class LeakDetector {

    public enum Level { DISABLED, SIMPLE, ADVANCED, PARANOID }

    static final int DEFAULT_SAMPLING_INTERVAL = 128;
    /** Access hints kept per tracked object — enough to see the last few hand-offs, bounded so a
     *  long-lived object cannot turn its own tracker into the leak. */
    private static final int MAX_RECORDS = 8;

    private static final Logger log = LoggerFactory.getLogger(LeakDetector.class);
    private static final Level LEVEL = parseLevel(System.getProperty("jbson.leakDetection"));

    /**
     * Quarantine mode: a released buffer is destroyed instead of recycled, so its memory is never
     * handed to a second owner while a stale reference to it survives.
     *
     * <p>This is a hunting tool, not a setting — a pool that never reuses anything is not a pool, and
     * turning it on deliberately changes the behaviour every other pool test asserts. Off by default,
     * including in test runs; switch it on with {@code -Djbson.pool.quarantine=true} when chasing an
     * ownership bug, exactly as you would reach for a leak-checking allocator rather than shipping one.
     *
     * <p>What it buys: use-after-release stops being survivable. Normally a disposed buffer goes
     * straight back out to the next caller, so a stale writer corrupts a live packet and the damage
     * appears somewhere else entirely. Quarantined, the stale write lands on a dead object and throws
     * at the offending line.
     */
    private static final boolean QUARANTINE = Boolean.getBoolean("jbson.pool.quarantine");

    private final String resourceType;
    private final int samplingInterval;
    private final ReferenceQueue<Object> collected = new ReferenceQueue<>();
    /** Keeps live trackers reachable — a PhantomReference that is itself collected never reports. */
    private final Set<Record> open = ConcurrentHashMap.newKeySet();
    private final LongAdder leaks = new LongAdder();
    /**
     * Acquisition sites already reported, so one bug is one log record.
     *
     * <p>Without this the detector is its own outage. A leak in a per-packet path leaks per packet;
     * at 1/128 sampling and a few hundred thousand packets a second that is hundreds of ERROR lines
     * with stack traces every second, into the same log that once reached 122 MB unrotated and put a
     * container's CPU into {@code dockerd} scanning it. The count keeps rising — that is what a metric
     * should watch — but the text is written once per distinct site. Netty does the same, for the
     * same reason.
     */
    private final Set<String> reportedSites = ConcurrentHashMap.newKeySet();

    public LeakDetector(String resourceType) {
        this(resourceType, DEFAULT_SAMPLING_INTERVAL);
    }

    public LeakDetector(String resourceType, int samplingInterval) {
        this.resourceType = resourceType;
        this.samplingInterval = Math.max(1, samplingInterval);
    }

    /** The level this JVM is running at. Static and final so {@code DISABLED} costs a folded branch. */
    public static Level level() {
        return LEVEL;
    }

    public static boolean isEnabled() {
        return LEVEL != Level.DISABLED;
    }

    /** True when released buffers are destroyed rather than recycled — see the field's note. */
    public static boolean quarantineEnabled() {
        return QUARANTINE;
    }

    /** Leaks reported by this detector since startup. Wire it to a metric — a non-zero value is a bug. */
    public long leakCount() {
        return leaks.sum();
    }

    /** Objects currently being watched. Visible for tests; also a sanity check that close() is wired. */
    int trackedCount() {
        return open.size();
    }

    /**
     * Starts watching {@code resource}, or returns null if this acquisition was not sampled.
     *
     * <p>Also drains whatever the GC has handed back since the last call — reporting from the
     * acquisition path costs nothing when there is nothing to report and avoids owning a thread.
     */
    public LeakTracker track(Object resource) {
        if (LEVEL == Level.DISABLED) {
            return null;
        }
        reportCollected();
        if (LEVEL != Level.PARANOID && ThreadLocalRandom.current().nextInt(samplingInterval) != 0) {
            return null;
        }
        Record record = new Record(this, resource, collected, resourceType, LEVEL != Level.SIMPLE);
        // Note what is NOT here: no stack capture at SIMPLE — see the class comment.
        open.add(record);
        return record;
    }

    /** Drains the reference queue, reporting anything that died with an open tracker. */
    private void reportCollected() {
        for (Reference<?> ref = collected.poll(); ref != null; ref = collected.poll()) {
            Record record = (Record) ref;
            if (open.remove(record)) {
                leaks.increment();
                String detail = record.describe();
                // Bounded: a leak reports its site once, however often it happens. The counter above
                // is the signal for "how bad"; this is the signal for "where".
                if (reportedSites.add(record.siteKey())) {
                    log.error("LEAK: {} was garbage-collected without being returned to its pool. "
                                    + "Whoever took it never released it, so the pool has been allocating a "
                                    + "replacement ever since. Further leaks from this site will only be "
                                    + "counted, not logged.{}",
                            record.resourceType, detail);
                }
            }
        }
    }

    /**
     * One watched object. Extends {@link PhantomReference} rather than holding the object, so the
     * tracker can never be the reason the object stays alive.
     */
    private static final class Record extends PhantomReference<Object> implements LeakTracker {

        private final LeakDetector owner;
        private final String resourceType;
        private final Throwable acquiredAt;
        private final Deque<Object> hints;   // null unless ADVANCED/PARANOID
        private volatile boolean closed;

        Record(LeakDetector owner, Object resource, ReferenceQueue<Object> queue, String resourceType,
               boolean detailed) {
            super(resource, queue);
            this.owner = owner;
            this.resourceType = resourceType;
            // The stack walk is the expensive half of tracking, so SIMPLE does without it.
            this.acquiredAt = detailed ? new Throwable("acquired here") : null;
            this.hints = detailed ? new ArrayDeque<>(MAX_RECORDS) : null;
        }

        @Override
        public void record(Object hint) {
            if (hints == null || closed) {
                return;
            }
            synchronized (hints) {
                if (hints.size() == MAX_RECORDS) {
                    hints.removeFirst();
                }
                hints.addLast(hint == null ? new Throwable("touched here") : hint);
            }
        }

        @Override
        public boolean close() {
            if (closed) {
                return false;
            }
            closed = true;
            clear();          // the object is back in the pool; stop watching it
            // And drop the tracker itself. Leaving it for the reference queue to clean up does not
            // work: a pooled buffer that came back is strongly held by the pool, so it is never
            // collected, so its phantom reference is never enqueued, so the tracker would live as
            // long as the pool — one per checkout, for the life of the process. The leak detector
            // would have been the leak. (PARANOID, which tracks every checkout, is what surfaced it.)
            owner.open.remove(this);
            return true;
        }

        /** Identifies the acquisition site, so repeats of one bug collapse into one report. */
        String siteKey() {
            if (acquiredAt == null) {
                return resourceType;   // no stack: one report per pool is all SIMPLE can distinguish
            }
            StackTraceElement[] stack = acquiredAt.getStackTrace();
            StringBuilder sb = new StringBuilder(64);
            int kept = 0;
            for (StackTraceElement frame : stack) {
                if (frame.getClassName().startsWith("su.grinev.pool.")) {
                    continue;
                }
                sb.append(frame).append('|');
                if (++kept == 6) {   // enough to separate call sites, short enough to stay cheap
                    break;
                }
            }
            return sb.toString();
        }

        String describe() {
            if (acquiredAt == null) {
                // SIMPLE: no stack was captured, because capturing one per sampled checkout is the
                // cost this level exists to avoid. Say so, rather than reporting an empty site.
                return "\n  Run with -Djbson.leakDetection=advanced to capture the call site that took it.";
            }
            StringBuilder sb = new StringBuilder("\n  Taken from the pool at:");
            appendStack(sb, acquiredAt);
            if (hints != null) {
                synchronized (hints) {
                    int i = hints.size();
                    for (Object hint : hints) {
                        sb.append("\n  Handled (").append(i--).append(" before the leak): ");
                        if (hint instanceof Throwable t) {
                            appendStack(sb, t);
                        } else {
                            sb.append(hint);
                        }
                    }
                }
            }
            return sb.toString();
        }

        private static void appendStack(StringBuilder sb, Throwable t) {
            StackTraceElement[] stack = t.getStackTrace();
            // Skip the pool frames: the interesting caller is the one that asked for the object.
            for (StackTraceElement frame : stack) {
                if (frame.getClassName().startsWith("su.grinev.pool.")) {
                    continue;
                }
                sb.append("\n\tat ").append(frame);
            }
        }
    }

    private static Level parseLevel(String value) {
        if (value == null || value.isBlank()) {
            // Off unless asked for. SIMPLE was the default through 0.9.0-39 and cost a production node
            // 518 MB of trackers per three minutes at 20 clients — a third of its ZGC cycles
            // (MyVPN docs/benchmarks/benchmark-2026-08-29-87-jbson39-ab-readv-and-gc-storm.md).
            return Level.DISABLED;
        }
        try {
            return Level.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException unknown) {
            log.warn("Unknown jbson.leakDetection level '{}', falling back to DISABLED", value);
            return Level.DISABLED;
        }
    }

    /**
     * Drops every open tracker without reporting. For tests that deliberately leak and for a pool
     * being torn down, where an unreleased object is the caller's business, not a defect.
     */
    void reset() {
        open.clear();
        reportedSites.clear();
        while (collected.poll() != null) {
            // drain
        }
        leaks.reset();
    }
}
