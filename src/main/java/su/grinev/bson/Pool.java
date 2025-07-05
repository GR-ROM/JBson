package su.grinev.bson;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

public class Pool<T> {

    private final List<T> pool = new LinkedList<>();
    private final int initialSize;
    private final Supplier<T> supplier;

    public Pool(int initialSize, Supplier<T> supplier) {
        this.initialSize = initialSize;
        this.supplier = supplier;
        supply(initialSize, supplier);
    }

    private void supply(int initialSize, Supplier<T> supplier) {
        for (int i = 0; i < initialSize; i++) {
            pool.addLast(supplier.get());
        }
    }

    public T get() {
        if (pool.isEmpty()) {
            supply(initialSize, supplier);
        }
        return pool.removeFirst();
    }

    public void release(T t) {
        pool.addLast(t);
    }
}
