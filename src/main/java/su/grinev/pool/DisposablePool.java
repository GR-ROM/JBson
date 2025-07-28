package su.grinev.pool;

import java.util.function.Supplier;

public class DisposablePool<T extends Disposable> extends BasePool<T> {
    private final Supplier<T> supplier;

    public DisposablePool(int initialSize, int limit, Supplier<T> supplier) {
        super(initialSize, limit);
        this.supplier = supplier;
        supply(initialSize);
    }

    protected void supply(int initialSize) {
        for (int i = 0; i < initialSize; i++) {
            T obj = supplier.get();
            obj.setOnDispose(() -> super.release(obj));
            pool.addLast(obj);
        }
    }
}