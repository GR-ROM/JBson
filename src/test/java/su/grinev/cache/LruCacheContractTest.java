package su.grinev.cache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One contract, two implementations: {@link LruCache} (single lock, exact LRU order) and
 * {@link CowLruCache} (lock-free reads, recency by per-entry stamp). They differ in how they pay for
 * concurrency, not in what they promise, so the behavioural tests run against both.
 */
class LruCacheContractTest {

    static Stream<Object[]> implementations() {
        return Stream.of(
                new Object[]{"LruCache", (IntFunction<Map<String, Integer>>) LruCache::new},
                new Object[]{"CowLruCache", (IntFunction<Map<String, Integer>>) CowLruCache::new}
        );
    }

    /**
     * Both caches record recency with a clock or an access order that only advances between distinct
     * operations; spinning until {@link System#nanoTime()} moves keeps the stamp-based implementation
     * from tying two entries and evicting an arbitrary one.
     */
    private static void tick() {
        long start = System.nanoTime();
        while (System.nanoTime() == start) {
            Thread.onSpinWait();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void storesAndReturnsValues(String name, IntFunction<Map<String, Integer>> factory) {
        Map<String, Integer> cache = factory.apply(4);

        assertNull(cache.put("a", 1), "put must report no previous mapping");
        assertEquals(1, cache.get("a"));
        assertEquals(1, cache.put("a", 2), "put must report the value it replaced");
        assertEquals(2, cache.get("a"));
        assertEquals(1, cache.size());
        assertTrue(cache.containsKey("a"));
        assertNull(cache.get("absent"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void neverGrowsPastCapacity(String name, IntFunction<Map<String, Integer>> factory) {
        Map<String, Integer> cache = factory.apply(3);

        for (int i = 0; i < 50; i++) {
            cache.put("k" + i, i);
            tick();
            assertTrue(cache.size() <= 3, "capacity exceeded at insert " + i + ": " + cache.size());
        }
        assertEquals(3, cache.size());
    }

    /** The point of an LRU: a READ protects an entry that insertion order would have evicted. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void readingAnEntryProtectsItFromEviction(String name, IntFunction<Map<String, Integer>> factory) {
        Map<String, Integer> cache = factory.apply(3);
        cache.put("a", 1);
        tick();
        cache.put("b", 2);
        tick();
        cache.put("c", 3);
        tick();

        cache.get("a");        // "a" is now the most recently used, "b" the least
        tick();
        cache.put("d", 4);     // evicts the least recently used

        assertEquals(1, cache.get("a"), "the entry that was just read must survive");
        assertNull(cache.get("b"), "the least recently used entry must be the one evicted");
        assertEquals(3, cache.get("c"));
        assertEquals(4, cache.get("d"));
    }

    /** Replacing an existing key must not evict anything — the table is not growing. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void replacingAValueEvictsNothing(String name, IntFunction<Map<String, Integer>> factory) {
        Map<String, Integer> cache = factory.apply(2);
        cache.put("a", 1);
        tick();
        cache.put("b", 2);
        tick();

        cache.put("a", 11);

        assertEquals(2, cache.size());
        assertEquals(11, cache.get("a"));
        assertEquals(2, cache.get("b"));
    }

    /** A stored null could not be told apart from a miss, so it is refused outright. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void nullValuesAreNotStored(String name, IntFunction<Map<String, Integer>> factory) {
        Map<String, Integer> cache = factory.apply(4);

        assertNull(cache.put("a", null));
        assertFalse(cache.containsKey("a"));
        assertEquals(0, cache.size());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void removeAndClear(String name, IntFunction<Map<String, Integer>> factory) {
        Map<String, Integer> cache = factory.apply(4);
        cache.put("a", 1);
        cache.put("b", 2);

        assertEquals(1, cache.remove("a"));
        assertNull(cache.remove("a"), "removing twice reports nothing the second time");
        assertEquals(1, cache.size());

        cache.clear();
        assertEquals(0, cache.size());
        assertTrue(cache.isEmpty());
        assertNull(cache.get("b"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void viewsAreSnapshotsAndDoNotWriteThrough(String name, IntFunction<Map<String, Integer>> factory) {
        Map<String, Integer> cache = factory.apply(4);
        cache.put("a", 1);

        Map<String, Integer> asMap = Map.copyOf(cache);
        assertEquals(Map.of("a", 1), asMap);

        var keys = cache.keySet();
        cache.put("b", 2);
        assertEquals(1, keys.size(), "the view was a snapshot: a later put must not appear in it");
        assertThrows(UnsupportedOperationException.class, () -> cache.keySet().add("c"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void capacityMustBePositive(String name, IntFunction<Map<String, Integer>> factory) {
        assertThrows(IllegalArgumentException.class, () -> factory.apply(0));
        assertThrows(IllegalArgumentException.class, () -> factory.apply(-1));
    }

    /**
     * Concurrency smoke test: readers and writers hammer the cache at once. It asserts the invariants
     * that a broken lock discipline breaks — no exception escapes, and the size bound holds throughout
     * — rather than a specific interleaving.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void staysConsistentUnderConcurrentUse(String name, IntFunction<Map<String, Integer>> factory) throws Exception {
        Map<String, Integer> cache = factory.apply(16);
        int threads = 8;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicBoolean overCapacity = new AtomicBoolean();
        AtomicInteger failures = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            int id = t;
            Thread.ofVirtual().start(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 2_000; i++) {
                        cache.put("k" + ((id * 31 + i) % 64), i);
                        cache.get("k" + (i % 64));
                        if (cache.size() > 16) {
                            overCapacity.set(true);
                        }
                    }
                } catch (Throwable e) {
                    failures.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "workers did not finish — likely a lock never released");
        assertEquals(0, failures.get(), "no operation may throw under concurrent use");
        assertFalse(overCapacity.get(), "the capacity bound must hold under concurrent writers");
        assertTrue(cache.size() <= 16);
    }

    /** COW-specific: a snapshot taken before a write keeps reading the old table, without blocking. */
    @Test
    void cowReadersSeeAConsistentSnapshot() {
        CowLruCache<String, Integer> cache = new CowLruCache<>(8);
        cache.put("a", 1);

        var before = cache.entrySet();
        cache.put("b", 2);
        cache.remove("a");

        assertEquals(1, before.size(), "the entry set handed out earlier must not change under the reader");
        assertEquals(Map.entry("a", 1), before.iterator().next());
        assertNull(cache.get("a"));
        assertEquals(2, cache.get("b"));
    }
}
