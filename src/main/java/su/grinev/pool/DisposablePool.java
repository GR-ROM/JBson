package su.grinev.pool;

import java.util.function.Supplier;

public class DisposablePool<T extends Disposable> extends BasePool<T> {
    private final Supplier<T> supplier;

    public DisposablePool(int initialSize, int limit, Supplier<T> supplier) {
        super(initialSize, limit);
        this.supplier = supplier;
        for (int i = 0; i < limit; i++) {
            pool.add(supply());
        }
    }

    protected T supply() {
        T t = supplier.get();
        t.setOnDispose(() -> release(t));
        return t;
    }
}