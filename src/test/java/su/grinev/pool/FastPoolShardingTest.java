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

    /**
     * The reason shards steal: in the real data path a buffer is acquired on one thread and released
     * on another. Pure affinity would drain the producer's shard forever while the consumer's filled.
     */
    @Test
    void crossThreadHandoff_doesNotStarveTheProducerShard() throws Exception {
        FastPool<Object> pool = sharded(4, 16, 4, new ArrayList<>());
        int rounds = 500;

        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        // Producer takes on this thread, consumer gives back on another — the shards must rebalance.
        Thread consumer = new Thread(() -> {
            try {
                for (int i = 0; i < rounds; i++) {
                    Object o = pool.get();
                    pool.release(o);
                }
            } catch (Throwable t) {
                failure.set(t);
            } finally {
                done.countDown();
            }
        }, "consumer");
        consumer.start();

        List<Object> held = new ArrayList<>();
        for (int i = 0; i < rounds; i++) {
            held.add(pool.get());
        }
        held.forEach(pool::release);

        assertTrue(done.await(10, TimeUnit.SECONDS), "consumer finished");
        assertNull(failure.get(), "no failure on the consumer thread");
        assertEquals(0, pool.getCountInUse(), "in-use accounting survives cross-thread release");
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

    @Test
    void distinctThreads_landOnDistinctShards() throws Exception {
        FastPool<Object> pool = sharded(0, 16, 4, new ArrayList<>());
        Set<Object> firstObjects = ConcurrentHashMap.newKeySet();
        AtomicInteger created = new AtomicInteger();

        // Four threads, four shards, every shard empty: each thread must allocate its own object
        // rather than contend. Objects are distinct, which is only observable because no thread
        // found anything to steal.
        CountDownLatch done = new CountDownLatch(4);
        for (int i = 0; i < 4; i++) {
            new Thread(() -> {
                Object o = pool.get();
                firstObjects.add(o);
                created.incrementAndGet();
                done.countDown();
            }).start();
        }
        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertEquals(4, firstObjects.size(), "each thread got its own object");
        assertEquals(4, pool.getCountInUse());
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
