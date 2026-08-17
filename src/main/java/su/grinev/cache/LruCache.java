package su.grinev.cache;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A bounded, thread-safe LRU map: the least recently USED entry (read or written) is evicted once the
 * cache is full.
 *
 * <p>Guarded by one plain lock rather than a {@link java.util.concurrent.locks.ReadWriteLock} on
 * purpose — recording recency mutates the order on every {@code get}, so there is no read-only
 * operation to share a read lock between, and a read lock would only add cost while giving no
 * concurrency. That single lock is also the reason this belongs nowhere near the per-packet data
 * plane (a caller with a per-packet hot path must keep this off it): every hit serialises
 * every caller. It is meant for control-plane-rate lookups.
 *
 * <p>{@code null} values are not stored: {@link #get} returning null means "absent", so a stored null
 * would be indistinguishable from a miss and would make the caller recompute anyway.
 *
 * <p>The collection views ({@link #keySet}, {@link #values}, {@link #entrySet}) are immutable
 * SNAPSHOTS taken under the lock, not live views — iterating a live view of a structure another
 * thread reorders on read would need the caller to hold the lock, which the {@link Map} contract
 * gives it no way to do.
 */
public class LruCache<K, V> implements Map<K, V> {

    private static final int DEFAULT_CAPACITY = 10_000;

    private final int capacity;
    private final ReentrantLock lock = new ReentrantLock();
    private final LinkedHashMap<K, V> cache;

    public LruCache() {
        this(DEFAULT_CAPACITY);
    }

    public LruCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("LruCache capacity must be positive, got " + capacity);
        }
        this.capacity = capacity;
        // accessOrder=true is what makes this an LRU rather than an insertion-ordered map: get()
        // moves the entry to the end, so the eldest entry is genuinely the least recently used.
        this.cache = new LinkedHashMap<>(Math.min(capacity, 1024), 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > LruCache.this.capacity;
            }
        };
    }

    /** The cache's fixed upper bound on entries. */
    public int capacity() {
        return capacity;
    }

    @Override
    public int size() {
        lock.lock();
        try {
            return cache.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        lock.lock();
        try {
            return cache.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        lock.lock();
        try {
            return cache.containsKey(key);   // deliberately does NOT count as a use: no reordering
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean containsValue(Object value) {
        lock.lock();
        try {
            return cache.containsValue(value);
        } finally {
            lock.unlock();
        }
    }

    /** Returns the value and marks it most recently used, or null when absent. */
    @Override
    public V get(Object key) {
        lock.lock();
        try {
            return cache.get(key);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Stores the mapping and marks it most recently used, evicting the eldest entry if that pushes the
     * cache past its capacity. A null value is ignored (see the class note).
     *
     * @return the value previously mapped to {@code key}, or null if there was none
     */
    @Override
    public V put(K key, V value) {
        if (value == null) {
            return null;
        }
        lock.lock();
        try {
            return cache.put(key, value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public V remove(Object key) {
        lock.lock();
        try {
            return cache.remove(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        lock.lock();
        try {
            for (Entry<? extends K, ? extends V> e : m.entrySet()) {
                if (e.getValue() != null) {
                    cache.put(e.getKey(), e.getValue());
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            cache.clear();
        } finally {
            lock.unlock();
        }
    }

    /** Immutable snapshot, eldest (least recently used) first. */
    @Override
    public Set<K> keySet() {
        lock.lock();
        try {
            return Collections.unmodifiableSet(new LinkedHashSet<>(cache.keySet()));
        } finally {
            lock.unlock();
        }
    }

    /** Immutable snapshot, eldest (least recently used) first. */
    @Override
    public Collection<V> values() {
        lock.lock();
        try {
            return Collections.unmodifiableList(new ArrayList<>(cache.values()));
        } finally {
            lock.unlock();
        }
    }

    /** Immutable snapshot, eldest (least recently used) first. */
    @Override
    public Set<Entry<K, V>> entrySet() {
        lock.lock();
        try {
            // Copied into detached Map.entry pairs: LinkedHashMap's own Entry objects are views into
            // the map, so keeping them would hand out handles that mutate (or setValue into) it.
            Set<Entry<K, V>> snapshot = new LinkedHashSet<>(cache.size());
            for (Entry<K, V> e : cache.entrySet()) {
                snapshot.add(Map.entry(e.getKey(), e.getValue()));
            }
            return Collections.unmodifiableSet(snapshot);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        lock.lock();
        try {
            return "LruCache{size=" + cache.size() + ", capacity=" + capacity + "}";
        } finally {
            lock.unlock();
        }
    }
}
