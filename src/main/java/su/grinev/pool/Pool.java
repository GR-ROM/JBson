package su.grinev.pool;

import java.util.function.Supplier;

public class Pool<T> extends BasePool<T> {
    private final Supplier<T> supplier;

    public Pool(int initialSize, int limit, Supplier<T> supplier) {
        super(initialSize, limit);
        this.supplier = supplier;

        for (int i = 0; i < initialSize; i++) {
            pool.add(supply());
        }
    }

    @Override
    protected T supply() {
        return supplier.get();
    }
}
