package su.grinev.pool;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class BasePool<T> {
    protected final AtomicInteger counter = new AtomicInteger(0);
    protected final AtomicInteger currentPoolSize;
    protected final ConcurrentLinkedDeque<T> pool;
    protected int limit;
    protected int initalSize;
    protected final AtomicBoolean isWaiting;
    protected final int timeoutMs;
    protected final boolean blocking;
    public final String name;

    public BasePool(String name, AtomicInteger currentPoolSize, int initialSize, int limit, int timeoutMs, boolean blocking) {
        this.name = name;
        this.pool = new ConcurrentLinkedDeque<>();
        this.currentPoolSize = currentPoolSize;
        this.limit = limit;
        this.initalSize = initialSize;
        this.isWaiting = new AtomicBoolean(false);
        this.timeoutMs = timeoutMs;
        this.blocking = blocking;
    }

    protected abstract T supply();

    public T get() {
        synchronized (pool) {
            if (counter.get() >= limit) {
                if (blocking) {
                    while (counter.get() >= limit) {
                        try {
                            isWaiting.set(true);
                            if (timeoutMs > 0) {
                                pool.wait(timeoutMs);
                                if (counter.get() >= limit) {
                                    throw new RuntimeException("Pool is full");
                                }
                            } else {
                                pool.wait();
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    }
                } else {
                    throw new RuntimeException("Pool is full");
                }
            }
            counter.incrementAndGet();
            if (pool.isEmpty()) {
                pool.add(supply());
            }
            return pool.removeLast();
        }
    }

    public void release(T t) {
        synchronized (pool) {
            pool.addLast(t);
            counter.decrementAndGet();
            if (isWaiting.compareAndSet(true, false)) {
                pool.notifyAll();
            }
        }
    }
}
