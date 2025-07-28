package su.grinev.pool;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class Pool<T> {

    protected final List<T> pool;
    protected final int initialSize;
    protected final Supplier<T> supplier;
    protected final AtomicInteger counter = new AtomicInteger(0);
    protected final int limit;
    protected volatile boolean isWaiting;

    public Pool(int initialSize, int limit, Supplier<T> supplier) {
        this.initialSize = initialSize;
        this.supplier = supplier;
        this.limit = limit;
        this.pool = new LinkedList<>();
        isWaiting = false;
        supply(initialSize);
    }

    protected void supply(int initialSize) {
        for (int i = 0; i < initialSize; i++) {
            T obj = supplier.get();
            pool.addLast(obj);
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
            supply(initialSize);
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
