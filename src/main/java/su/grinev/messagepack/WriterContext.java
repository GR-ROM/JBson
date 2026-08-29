package su.grinev.messagepack;

import java.util.Iterator;
import java.util.Map;

/**
 * One frame of the writer's explicit container stack: the iterator over the map or list currently
 * being written. Frames live in a per-thread array ({@link MessagePackWriter}) and are reused
 * across calls, so {@link #clear()} drops the iterator once the container is finished — otherwise a
 * frame would pin the caller's document until the next serialize on that thread.
 */
public class WriterContext {

    public Iterator<Map.Entry<Object, Object>> objectMap;
    public Iterator<Object> array;
    public boolean isArray;

    public WriterContext initMap(Iterator<Map.Entry<Object, Object>> objectMap) {
        this.objectMap = objectMap;
        this.array = null;
        this.isArray = false;
        return this;
    }

    public WriterContext initList(Iterator<Object> arrayMap) {
        this.objectMap = null;
        this.array = arrayMap;
        this.isArray = true;
        return this;
    }

    public void clear() {
        this.objectMap = null;
        this.array = null;
    }
}
