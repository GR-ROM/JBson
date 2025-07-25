package su.grinev.bson;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class Pool<T> {

    private final List<T> pool;
    private final int initialSize;
    private final Supplier<T> supplier;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final int limit;
    private volatile boolean isWaiting;

    public Pool(int initialSize, int limit, Supplier<T> supplier) {
        this.initialSize = initialSize;
        this.supplier = supplier;
        this.limit = limit;
        this.pool = new ArrayList<>(initialSize);
        isWaiting = false;
        supply(initialSize, supplier);
    }

    private void supply(int initialSize, Supplier<T> supplier) {
        for (int i = 0; i < initialSize; i++) {
            pool.addLast(supplier.get());
        }
    }

    public T get() {
        if (counter.get() >= limit) {
            synchronized (pool) {
                while (counter.get() >= limit) {
                    try {
                        isWaiting = true;
                        pool.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } else {
            counter.incrementAndGet();
        }
        if (pool.isEmpty()) {
            supply(initialSize, supplier);
        }
        return pool.removeLast();
    }

    public void release(T t) {
        pool.addLast(t);
        if (counter.decrementAndGet() < limit && isWaiting) {
            synchronized (pool) {
                isWaiting = false;
                pool.notify();
            }
        }
    }
}
