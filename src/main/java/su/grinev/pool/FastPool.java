package su.grinev.pool;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * High-performance lock-free object pool.
 * Uses CAS operations for get/release to avoid synchronized blocks.
 */
public class FastPool<T> {
    private final ConcurrentLinkedDeque<T> pool;
    private final Supplier<T> supplier;
    private final AtomicInteger size;
    private final int maxSize;

    public FastPool(Supplier<T> supplier, int initialSize, int maxSize) {
        this.pool = new ConcurrentLinkedDeque<>();
        this.supplier = supplier;
        this.size = new AtomicInteger(0);
        this.maxSize = maxSize;

        // Pre-populate pool
        for (int i = 0; i < initialSize; i++) {
            pool.addLast(supplier.get());
            size.incrementAndGet();
        }
    }

    public T get() {
        T item = pool.pollLast();
        if (item != null) {
            return item;
        }
        // Pool empty, create new
        return supplier.get();
    }

    public void release(T item) {
        if (size.get() < maxSize) {
            pool.addLast(item);
            size.incrementAndGet();
        }
        // If over max size, just discard (let GC handle it)
    }

    public int size() {
        return pool.size();
    }
}
