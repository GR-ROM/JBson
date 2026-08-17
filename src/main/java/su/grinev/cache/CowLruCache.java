package su.grinev.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Copy-on-write counterpart to {@link LruCache}: <b>reads take no lock at all</b>, writes copy the
 * whole map and publish it through one volatile store.
 *
 * <p>Which of the two to use follows from the read/write mix, not from taste:
 * <ul>
 *   <li>{@link LruCache} — one lock for everything. Cheap writes, but every read serialises against
 *       every other. Right when writes are frequent, or the cache is not on a concurrent path.
 *   <li>this one — reads scale with cores and never block; a write is O(size) copying plus an
 *       allocation of the whole table. Right when the key set is read constantly and changes rarely.
 *       Wrong for a write-heavy or high-churn cache: a steady miss rate turns every miss into a full
 *       copy, so a cache that never warms up degenerates into copying the table per lookup.
 * </ul>
 *
 * <p><b>Recency without mutating the map.</b> A textbook LRU reorders itself on read, which under
 * copy-on-write would mean copying the table on every read — the opposite of the point. So recency
 * lives in the entry instead: each read stamps its own node, and eviction scans for the oldest stamp.
 * The stamp is written without synchronisation and read without it, which is deliberate — a lost or
 * stale stamp can only make eviction pick a slightly different victim, and no correctness (or
 * visibility of the VALUE, which is final and published safely with the map) depends on it.
 *
 * <p>Published maps are never mutated again: a writer builds the successor privately and swaps the
 * reference, so a reader iterating an old snapshot sees a consistent, if momentarily stale, table.
 *
 * <p>{@code null} values are not stored — {@link #get} returning null means "absent", and a stored
 * null would be indistinguishable from a miss.
 */
public class CowLruCache<K, V> implements Map<K, V> {

    private static final int DEFAULT_CAPACITY = 10_000;

    private final int capacity;
    // Writers serialise here; readers never touch it. A lock rather than a CAS retry loop because a
    // write already copies the whole table — losing that race and redoing it would be the expensive
    // part, not the contention.
    private final ReentrantLock writeLock = new ReentrantLock();
    // The published table. Immutable BY CONVENTION: whoever builds it stops touching it before the
    // store below makes it visible.
    private volatile Map<K, Node<V>> snapshot = Map.of();

    /** Value plus its recency stamp; the stamp is the only mutable state and only a heuristic. */
    private static final class Node<V> {
        final V value;
        long lastAccessNanos;

        Node(V value) {
            this.value = value;
            this.lastAccessNanos = System.nanoTime();
        }
    }

    public CowLruCache() {
        this(DEFAULT_CAPACITY);
    }

    public CowLruCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("CowLruCache capacity must be positive, got " + capacity);
        }
        this.capacity = capacity;
    }

    /** The cache's fixed upper bound on entries. */
    public int capacity() {
        return capacity;
    }

    @Override
    public int size() {
        return snapshot.size();
    }

    @Override
    public boolean isEmpty() {
        return snapshot.isEmpty();
    }

    /** Does not count as a use: no recency stamp is written. */
    @Override
    public boolean containsKey(Object key) {
        return snapshot.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        for (Node<V> node : snapshot.values()) {
            if (node.value.equals(value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Lock-free lookup. Marks the entry most recently used, or returns null when absent.
     *
     * <p>One volatile read of the table, one hash lookup, one plain store. Nothing here can block, so
     * concurrent readers never see each other.
     */
    @Override
    public V get(Object key) {
        Node<V> node = snapshot.get(key);
        if (node == null) {
            return null;
        }
        node.lastAccessNanos = System.nanoTime();
        return node.value;
    }

    /**
     * Stores the mapping, evicting the least recently used entry if that would exceed capacity.
     * Copies the whole table. A null value is ignored (see the class note).
     *
     * @return the value previously mapped to {@code key}, or null if there was none
     */
    @Override
    public V put(K key, V value) {
        if (value == null) {
            return null;
        }
        writeLock.lock();
        try {
            Map<K, Node<V>> current = snapshot;
            Node<V> previous = current.get(key);
            Map<K, Node<V>> next = new HashMap<>(current);
            // Only a NEW key can push the table over capacity; replacing a value cannot.
            if (previous == null && next.size() >= capacity) {
                evictLeastRecentlyUsed(next);
            }
            next.put(key, new Node<>(value));
            snapshot = next;
            return previous == null ? null : previous.value;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public V remove(Object key) {
        writeLock.lock();
        try {
            Map<K, Node<V>> current = snapshot;
            if (!current.containsKey(key)) {
                return null;   // nothing to do: do not pay for a copy
            }
            Map<K, Node<V>> next = new HashMap<>(current);
            Node<V> removed = next.remove(key);
            snapshot = next;
            return removed == null ? null : removed.value;
        } finally {
            writeLock.unlock();
        }
    }

    /** One copy for the whole batch, rather than one per entry. */
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        writeLock.lock();
        try {
            Map<K, Node<V>> next = new HashMap<>(snapshot);
            for (Entry<? extends K, ? extends V> e : m.entrySet()) {
                if (e.getValue() == null) {
                    continue;
                }
                if (!next.containsKey(e.getKey()) && next.size() >= capacity) {
                    evictLeastRecentlyUsed(next);
                }
                next.put(e.getKey(), new Node<>(e.getValue()));
            }
            snapshot = next;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void clear() {
        writeLock.lock();
        try {
            snapshot = Map.of();
        } finally {
            writeLock.unlock();
        }
    }

    /** Immutable snapshot of the keys; unordered, like the {@link HashMap} underneath. */
    @Override
    public Set<K> keySet() {
        return Collections.unmodifiableSet(new LinkedHashSet<>(snapshot.keySet()));
    }

    /** Immutable snapshot of the values; unordered. */
    @Override
    public Collection<V> values() {
        Map<K, Node<V>> current = snapshot;
        List<V> values = new ArrayList<>(current.size());
        for (Node<V> node : current.values()) {
            values.add(node.value);
        }
        return Collections.unmodifiableList(values);
    }

    /** Immutable snapshot of detached entries; unordered. */
    @Override
    public Set<Entry<K, V>> entrySet() {
        Map<K, Node<V>> current = snapshot;
        Set<Entry<K, V>> entries = new LinkedHashSet<>(current.size());
        for (Entry<K, Node<V>> e : current.entrySet()) {
            entries.add(Map.entry(e.getKey(), e.getValue().value));
        }
        return Collections.unmodifiableSet(entries);
    }

    /**
     * Drops the entry with the oldest stamp. O(size), but so is the copy it runs inside, so it costs
     * the write nothing asymptotically. Called with the write lock held, on the private successor map.
     */
    private void evictLeastRecentlyUsed(Map<K, Node<V>> map) {
        K victim = null;
        long oldest = Long.MAX_VALUE;
        for (Entry<K, Node<V>> e : map.entrySet()) {
            long stamp = e.getValue().lastAccessNanos;
            if (stamp < oldest) {
                oldest = stamp;
                victim = e.getKey();
            }
        }
        if (victim != null) {
            map.remove(victim);
        }
    }

    @Override
    public String toString() {
        return "CowLruCache{size=" + snapshot.size() + ", capacity=" + capacity + "}";
    }
}
