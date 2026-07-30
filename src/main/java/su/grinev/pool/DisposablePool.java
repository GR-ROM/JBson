package su.grinev.pool;

import java.util.function.Supplier;

/**
 * A {@link FastPool} of {@link Disposable} objects.
 *
 * Borrowed objects get an {@code onDispose} hook so {@code dispose()} returns
 * them to the pool. Idle objects removed by {@link #trim(int)} are permanently
 * released via {@link Disposable#destroy()} (e.g. closing an arena), so the pool
 * shrinks its real footprint during long idle periods.
 */
public class DisposablePool<T extends Disposable> extends FastPool<T> {

    /** Unsharded (shards = 1) — the historical behaviour. */
    public DisposablePool(String name, Supplier<T> supplier,
                          int initialSize, int maxSize, boolean blocking, int timeoutMs) {
        this(name, supplier, initialSize, maxSize, blocking, timeoutMs, 1);
    }

    /** @param shards see {@link FastPool#FastPool(String, Supplier, java.util.function.Consumer, int, int, boolean, int, int)}. */
    public DisposablePool(String name, Supplier<T> supplier,
                          int initialSize, int maxSize, boolean blocking, int timeoutMs, int shards) {
        super(name, supplier, Disposable::destroy, initialSize, maxSize, blocking, timeoutMs, shards);
    }

    @Override
    public T get() {
        T t = super.get();
        // Set the release closure once per buffer, not per checkout: dispose() runs onDispose but never
        // clears it, and the buffer always returns to this same pool, so the captured lambda stays valid
        // for the buffer's whole pool lifetime. Re-creating it every get() was a measurable per-packet
        // allocation on the hot path (JFR run3: DisposablePool$$Lambda, ~8% of sampled allocations).
        if (t.getOnDispose() == null) {
            t.setOnDispose(() -> release(t));
        }
        return t;
    }
}
