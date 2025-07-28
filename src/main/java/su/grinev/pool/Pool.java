package su.grinev.pool;

import java.util.function.Supplier;

public class Pool<T> extends BasePool<T> {

    private final Supplier<T> supplier;

    public Pool(int initialSize, int limit, Supplier<T> supplier) {
        super(initialSize, limit);
        this.supplier = supplier;
        isWaiting = false;
        supply(initialSize);
    }

    @Override
    protected void supply(int initialSize) {
        for (int i = 0; i < initialSize; i++) {
            T obj = supplier.get();
            pool.addLast(obj);
        }
    }
}
