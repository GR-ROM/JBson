package su.grinev.pool;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class BasePool<T> {

    protected final AtomicInteger counter = new AtomicInteger(0);
    protected final ConcurrentLinkedDeque<T> pool;
    protected int limit;
    protected int initalSize;
    protected volatile boolean isWaiting;

    public BasePool(int initialSize, int limit) {
        this.pool = new ConcurrentLinkedDeque<>();
        this.limit = limit;
        this.initalSize = initialSize;
        this.isWaiting = false;
    }

    protected abstract T supply();

    public T get() {
        synchronized (pool) {
            if (counter.get() >= limit) {
                while (counter.get() >= limit) {
                    try {
                        isWaiting = true;
                        pool.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            } else {
                counter.incrementAndGet();
            }
            if (pool.isEmpty()) {
                pool.add(supply());
            }
            return pool.removeLast();
        }
    }

    public void release(T t) {
        pool.addLast(t);
        if (counter.decrementAndGet() < limit && isWaiting) {
            synchronized (pool) {
                isWaiting = false;
                pool.notifyAll();
            }
        }
    }
}
