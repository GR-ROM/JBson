package su.grinev.pool;

import lombok.Getter;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public abstract class BasePool<T> {
    protected final AtomicInteger counter = new AtomicInteger(0);
    @Getter
    protected final AtomicInteger currentPoolSize;
    protected final ConcurrentLinkedDeque<T> pool;
    protected final ConcurrentLinkedDeque<Thread> waiters;
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
        this.waiters = new ConcurrentLinkedDeque<>();
    }

    protected abstract T supply();

    public T get() {
        int spins = 3;

        while (true) {
            int cur = counter.get();
            if (cur < limit && counter.compareAndSet(cur, cur + 1)) {
                T obj = pool.pollLast();
                return obj != null ? obj : supply();
            }

            if (!blocking) {
                throw new IllegalStateException("Pool overflow");
            }

            if (--spins > 0) {
                Thread.onSpinWait();
                continue;
            }

            Thread me = Thread.currentThread();
            waiters.add(me);

            cur = counter.get();
            if (cur < limit && counter.compareAndSet(cur, cur + 1)) {
                waiters.remove(me);
                T obj = pool.pollLast();
                return obj != null ? obj : supply();
            }

            LockSupport.park(this);
            spins = 3;
        }
    }

    public void release(T t) {
        pool.addLast(t);

        int c = counter.decrementAndGet();
        if (c < 0) {
            counter.incrementAndGet();
           // throw new IllegalStateException("Double release");
        }

        Thread w = waiters.poll();
        if (w != null) {
            LockSupport.unpark(w);
        }
    }
}
