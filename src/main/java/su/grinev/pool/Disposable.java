package su.grinev.pool;

public interface Disposable {

    void setOnDispose(Runnable onDispose);
    void dispose();

}
