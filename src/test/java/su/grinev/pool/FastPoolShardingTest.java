package su.grinev.pool;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the sharded mode of {@link FastPool}. The contract that matters is that sharding is
 * invisible from the outside: the {@link Trimmable} signals, the in-use accounting and the
 * double-release detection must behave exactly as they do unsharded, no matter which thread got an
 * object and which thread gave it back.
 */
public class FastPoolShardingTest {

    private static FastPool<Object> sharded(int initialSize, int maxSize, int shards, List<Object> destroyed) {
        return new FastPool<>("test", Object::new, destroyed::add, initialSize, maxSize, false, 0, shards);
    }

    @Test
    void defaultConstructors_areUnsharded() {
        assertEquals(1, new FastPool<>(Object::new, 2, 10).getShards());
        assertEquals(1, new FastPool<>("t", Object::new, o -> {}, 0, 10, false, 0).getShards());
        assertEquals(1, new DisposablePool<>("t", TestDisposable::new, 0, 10, false, 0).getShards());
    }

    @Test
    void shardCount_isClampedToAtLeastOne() {
        assertEquals(1, sharded(0, 10, 0, new ArrayList<>()).getShards());
        assertEquals(1, sharded(0, 10, -4, new ArrayList<>()).getShards());
    }

    @Test
    void prefill_isSpreadAcrossShards() {
        FastPool<Object> pool = sharded(8, 16, 4, new ArrayList<>());
        assertEquals(8, pool.getIdle(), "every prefilled object is idle regardless of shard");
        assertEquals(0, pool.getCountInUse());

        // All eight must be reachable from a single thread, which only works if that thread steals
        // from the shards it does not own.
        List<Object> taken = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            taken.add(pool.get());
        }
        assertEquals(8, pool.getCountInUse());
        assertEquals(0, pool.getIdle(), "prefill drained without allocating");
        assertEquals(8, pool.getTotalCreated(), "stealing reused the prefill instead of creating more");
        taken.forEach(pool::release);
    }

    /** Runs a body on its own thread and waits, so the body gets its own shard assignment. */
    private static void onOwnThread(String name, ThrowingRunnable body) throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                body.run();
            } catch (Throwable e) {
                failure.set(e);
            }
        }, name);
        t.start();
        t.join(TimeUnit.SECONDS.toMillis(10));
        assertFalse(t.isAlive(), "thread " + name + " finished");
        if (failure.get() != null) {
            throw new AssertionError("failure on " + name, failure.get());
        }
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** get() must take from a sibling shard rather than allocate when its own shard is empty. */
    @Test
    void get_stealsFromSiblingShard_ratherThanAllocating() throws Exception {
        FastPool<Object> pool = sharded(0, 16, 4, new ArrayList<>());

        // Producer thread: create one object and park it in ITS shard.
        AtomicInteger producerShard = new AtomicInteger(-1);
        onOwnThread("producer", () -> {
            producerShard.set(pool.shardOfCurrentThread());
            pool.release(pool.get());
        });

        assertEquals(1, pool.getTotalCreated(), "exactly one object exists");
        assertEquals(1, pool.idleInShard(producerShard.get()), "it is idle in the producer's shard");

        // Consumer thread on a different shard: its own shard is empty, so it must steal that object.
        onOwnThread("consumer", () -> {
            assertNotEquals(producerShard.get(), pool.shardOfCurrentThread(), "distinct shards");
            assertEquals(0, pool.idleInShard(pool.shardOfCurrentThread()), "own shard starts empty");
            Object stolen = pool.get();
            assertNotNull(stolen);
            assertEquals(1, pool.getTotalCreated(), "stole instead of allocating a second object");
            assertEquals(0, pool.idleInShard(producerShard.get()), "taken out of the producer's shard");
            pool.release(stolen);
        });
    }

    /** release() must spill into a sibling shard rather than drop the object when its own shard is full. */
    @Test
    void release_overflowsIntoSiblingShard_whenOwnShardIsFull() throws Exception {
        // maxSize 8 over 4 shards -> 2 slots per shard.
        FastPool<Object> pool = sharded(0, 8, 4, new ArrayList<>());

        onOwnThread("releaser", () -> {
            int own = pool.shardOfCurrentThread();
            List<Object> taken = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                taken.add(pool.get());
            }
            taken.forEach(pool::release);

            assertEquals(2, pool.idleInShard(own), "own shard filled to its per-shard capacity");
            assertEquals(5, pool.getIdle(), "the other three were kept in sibling shards, not dropped");
        });
    }

    /**
     * End-to-end check of the asymmetric case sharding could get badly wrong: one thread only takes,
     * another only gives back — exactly the TUN-reader / IO-thread handoff. With pure per-thread
     * affinity (neither stealing nor spillover) the taker's shard drains and it allocates a fresh
     * object on every get, forever.
     *
     * <p>This is deliberately an integration check, not a discriminating one: either rebalancing
     * mechanism on its own keeps objects circulating here, so removing just one does not break it.
     * The individual mechanisms are pinned by {@link #get_stealsFromSiblingShard_ratherThanAllocating}
     * and {@link #release_overflowsIntoSiblingShard_whenOwnShardIsFull}.
     */
    @Test
    void asymmetricProducerConsumer_reusesObjectsInsteadOfAllocatingUnbounded() throws Exception {
        FastPool<Object> pool = sharded(0, 64, 4, new ArrayList<>());
        int rounds = 2000;

        // Hand-off queue: taker pulls from the pool, giver returns to the pool, on distinct threads.
        // Bounded on purpose. With an unbounded queue the taker simply runs ahead of the giver and
        // the object count measures pipeline depth, not shard behaviour. Capped at 8, at most ~9
        // objects can be in flight, so `totalCreated` becomes a direct read on whether stealing
        // works: circulating -> stays near the bound, starving -> climbs toward `rounds`.
        java.util.concurrent.BlockingQueue<Object> handoff = new java.util.concurrent.ArrayBlockingQueue<>(8);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        Thread giver = new Thread(() -> {
            try {
                for (int i = 0; i < rounds; i++) {
                    pool.release(handoff.take());
                }
            } catch (Throwable e) {
                failure.set(e);
            } finally {
                done.countDown();
            }
        }, "giver");
        giver.start();

        onOwnThread("taker", () -> {
            for (int i = 0; i < rounds; i++) {
                handoff.put(pool.get());
            }
        });

        assertTrue(done.await(30, TimeUnit.SECONDS), "giver drained the handoff");
        assertNull(failure.get());
        assertEquals(0, pool.getCountInUse(), "in-use accounting survives cross-thread release");
        // The whole point: objects must circulate. Without stealing the taker's shard is empty on
        // every get (the giver returns to its own shard) and this climbs toward `rounds`.
        assertTrue(pool.getTotalCreated() < 64,
                "objects circulate between shards instead of being allocated per get, created="
                        + pool.getTotalCreated() + " over " + rounds + " rounds");
        assertTrue(pool.getIdle() > 0, "objects came back to the pool rather than being dropped");
    }

    @Test
    void concurrentGetRelease_keepsInUseExact() throws Exception {
        FastPool<Object> pool = sharded(16, 64, 4, new ArrayList<>());
        int threads = 6;
        int perThread = 2000;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        for (int t = 0; t < threads; t++) {
            new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        Object o = pool.get();
                        pool.release(o);
                    }
                } catch (Throwable e) {
                    failure.set(e);
                } finally {
                    done.countDown();
                }
            }, "worker").start();
        }

        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "all workers finished");
        assertNull(failure.get());
        assertEquals(0, pool.getCountInUse(), "every get was matched by a release");
        assertTrue(pool.getIdle() <= 64 + 4, "idle stays within maxSize plus per-shard rounding");
    }

    /**
     * The first N threads to touch an N-shard pool must land on N distinct shards — that is the whole
     * mechanism by which hot threads stop sharing a lock.
     */
    @Test
    void firstThreads_landOnDistinctShards() throws Exception {
        FastPool<Object> pool = sharded(0, 16, 4, new ArrayList<>());
        Set<Integer> assigned = ConcurrentHashMap.newKeySet();

        for (int i = 0; i < 4; i++) {
            onOwnThread("hot-" + i, () -> assigned.add(pool.shardOfCurrentThread()));
        }
        assertEquals(Set.of(0, 1, 2, 3), assigned, "four threads, four distinct shards");
    }

    @Test
    void unshardedPool_pinsEveryThreadToShardZero() throws Exception {
        FastPool<Object> pool = sharded(0, 16, 1, new ArrayList<>());
        for (int i = 0; i < 3; i++) {
            onOwnThread("t-" + i, () -> assertEquals(0, pool.shardOfCurrentThread()));
        }
    }

    /** A lopsided pool must come out balanced, not with one shard emptied and the rest untouched. */
    @Test
    void trim_evensOutLopsidedShards() throws Exception {
        List<Object> destroyed = new ArrayList<>();
        FastPool<Object> pool = sharded(0, 32, 4, destroyed);

        // Pile 8 objects into a single shard by taking and releasing them all on one thread.
        AtomicInteger loadedShard = new AtomicInteger(-1);
        onOwnThread("loader", () -> {
            loadedShard.set(pool.shardOfCurrentThread());
            List<Object> taken = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                taken.add(pool.get());
            }
            taken.forEach(pool::release);
        });
        assertEquals(8, pool.idleInShard(loadedShard.get()), "all 8 idle in one shard");

        assertTrue(pool.trim(4), "4 of 8 idle objects freed");
        assertEquals(4, destroyed.size());
        assertEquals(4, pool.getIdle());
        assertEquals(4, pool.idleInShard(loadedShard.get()), "drained from the fullest shard");
    }

    @Test
    void trim_drainsAcrossShards_andBalancesThem() {
        List<Object> destroyed = new ArrayList<>();
        FastPool<Object> pool = sharded(12, 16, 4, destroyed);
        assertEquals(12, pool.getIdle());

        assertTrue(pool.trim(8), "8 of 12 idle objects are available across shards");
        assertEquals(8, destroyed.size(), "trimmed objects went to the destroyer");
        assertEquals(4, pool.getIdle());
    }

    @Test
    void trim_returnsFalse_andRestoresObjects_whenShardsCannotSupplyEnough() {
        List<Object> destroyed = new ArrayList<>();
        FastPool<Object> pool = sharded(6, 16, 4, destroyed);

        assertFalse(pool.trim(10), "only 6 idle, cannot free 10");
        assertTrue(destroyed.isEmpty(), "nothing destroyed on the failed path");
        assertEquals(6, pool.getIdle(), "everything drained was put back");
    }

    @Test
    void doubleRelease_isStillDetected_whenSharded() {
        FastPool<Object> pool = sharded(0, 10, 4, new ArrayList<>());
        Object a = pool.get();
        pool.release(a);
        assertEquals(0, pool.getCountInUse());

        pool.release(a);   // second release — swallowed, must not corrupt the accounting
        assertEquals(0, pool.getCountInUse(), "in-use never goes negative");
    }

    @Test
    void blockingSharded_enforcesTheInFlightLimit() {
        FastPool<Object> pool = new FastPool<>("blocking", Object::new, o -> {}, 0, 2, true, 50, 4);
        Object a = pool.get();
        Object b = pool.get();
        assertEquals(2, pool.getCountInUse());

        assertThrows(IllegalStateException.class, pool::get, "third checkout exceeds maxSize in flight");

        pool.release(a);
        pool.release(b);
        assertEquals(0, pool.getCountInUse());
    }

    private static class TestDisposable implements Disposable {
        private Runnable onDispose;

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
            if (onDispose != null) {
                onDispose.run();
            }
        }

        @Override
        public void destroy() {
        }

        @Override
        public void close() {
        }
    }
}
