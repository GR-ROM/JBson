package su.grinev.pool;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class PoolFactory {

    private int maxPoolSize;
    private int minPoolSize;
    private int outOfPoolTimeout;
    private boolean blocking;
    /** Default shard count for pools this factory creates; 1 = unsharded (see {@link FastPool}). */
    private int shards = 1;
    private final Map<String, Trimmable> pools = new ConcurrentHashMap<>();
    private final AtomicInteger poolCounter = new AtomicInteger(0);

    public PoolFactory(int maxPoolSize, int minPoolSize, int outOfPoolTimeout, boolean blocking) {
        this(maxPoolSize, minPoolSize, outOfPoolTimeout, blocking, 1);
    }

    public PoolFactory(int maxPoolSize, int minPoolSize, int outOfPoolTimeout, boolean blocking, int shards) {
        this.maxPoolSize = maxPoolSize;
        this.minPoolSize = minPoolSize;
        this.outOfPoolTimeout = outOfPoolTimeout;
        this.blocking = blocking;
        this.shards = Math.max(1, shards);
    }

    public PoolFactory() {

    }

    public Map<String, Trimmable> getPools() {
        return new HashMap<>(pools);
    }

    public <T> FastPool<T> getPool(Supplier<T> supplier) {
        String name = "Pool-" + poolCounter.getAndIncrement();
        return getPool(name, supplier);
    }

    public <T> FastPool<T> getPool(String name, Supplier<T> supplier) {
        return getPool(name, supplier, minPoolSize);
    }

    /** Create a pool with an explicit prefill (warm baseline) instead of the factory's default minPoolSize. */
    public <T> FastPool<T> getPool(String name, Supplier<T> supplier, int initialSize) {
        return getPool(name, supplier, initialSize, maxPoolSize);
    }

    /**
     * Create a pool with an explicit prefill and an explicit per-pool ceiling (overriding the factory's
     * default maxPoolSize). Use a small ceiling for large buffers and a large one for small buffers so a
     * full pool stays within the memory budget.
     */
    public <T> FastPool<T> getPool(String name, Supplier<T> supplier, int initialSize, int maxSize) {
        return getPool(name, supplier, initialSize, maxSize, shards);
    }

    /**
     * Create a pool with an explicit shard count, overriding the factory default. Shard only the
     * pools that several hot threads hit — an unshared pool gains nothing and just spreads its idle
     * objects thinner.
     */
    public <T> FastPool<T> getPool(String name, Supplier<T> supplier, int initialSize, int maxSize, int shards) {
        FastPool<T> pool = new FastPool<>(name, supplier, item -> {}, initialSize, maxSize, blocking, outOfPoolTimeout, shards);
        pools.put(name, pool);
        return pool;
    }

    public <T> FastPool<T> getFastPool(Supplier<T> supplier) {
        return getPool(supplier);
    }

    public <T> FastPool<T> getFastPool(String name, Supplier<T> supplier) {
        return getPool(name, supplier);
    }

    public <T extends Disposable> DisposablePool<T> getDisposablePool(Supplier<T> supplier) {
        String name = "DisposablePool-" + poolCounter.getAndIncrement();
        return getDisposablePool(name, supplier);
    }

    public <T extends Disposable> DisposablePool<T> getDisposablePool(String name, Supplier<T> supplier) {
        return getDisposablePool(name, supplier, minPoolSize);
    }

    /** Create a disposable pool with an explicit prefill (warm baseline) instead of the factory's default minPoolSize. */
    public <T extends Disposable> DisposablePool<T> getDisposablePool(String name, Supplier<T> supplier, int initialSize) {
        return getDisposablePool(name, supplier, initialSize, maxPoolSize);
    }

    /** Create a disposable pool with an explicit prefill and an explicit per-pool ceiling (see {@link #getPool(String, Supplier, int, int)}). */
    public <T extends Disposable> DisposablePool<T> getDisposablePool(String name, Supplier<T> supplier, int initialSize, int maxSize) {
        return getDisposablePool(name, supplier, initialSize, maxSize, shards);
    }

    /** Create a disposable pool with an explicit shard count (see {@link #getPool(String, Supplier, int, int, int)}). */
    public <T extends Disposable> DisposablePool<T> getDisposablePool(String name, Supplier<T> supplier, int initialSize, int maxSize, int shards) {
        DisposablePool<T> disposablePool = new DisposablePool<>(name, supplier, initialSize, maxSize, blocking, outOfPoolTimeout, shards);
        pools.put(name, disposablePool);
        return disposablePool;
    }

    public static class Builder {

        private final PoolFactory instance = new PoolFactory();

        public static Builder builder() {
            return new Builder();
        }

        public Builder setMaxPoolSize(int maxPoolSize) {
            instance.maxPoolSize = maxPoolSize;
            return this;
        }

        public Builder setMinPoolSize(int minPoolSize) {
            instance.minPoolSize = minPoolSize;
            return this;
        }

        public Builder setOutOfPoolTimeout(int outOfPoolTimeout) {
            instance.outOfPoolTimeout = outOfPoolTimeout;
            return this;
        }

        public Builder setBlocking(boolean blocking) {
            instance.blocking = blocking;
            return this;
        }

        /**
         * Default shard count for every pool this factory creates (1 = unsharded). Size it to the
         * number of threads that hit a pool on the hot path; individual pools can override it via
         * {@link PoolFactory#getPool(String, Supplier, int, int, int)}.
         */
        public Builder setShards(int shards) {
            instance.shards = Math.max(1, shards);
            return this;
        }

        public PoolFactory build() {
            return instance;
        }
    }
}
