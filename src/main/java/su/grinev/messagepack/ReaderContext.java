package su.grinev.messagepack;

import java.util.List;
import java.util.Map;

/**
 * One frame of the reader's explicit container stack: the map or list being filled and how many
 * entries it still owes. Frames live in a per-thread array ({@link MessagePackReader}) and are
 * reused across calls, so {@link #clear()} drops the container once it is complete — otherwise a
 * frame would pin the decoded document until the next deserialize on that thread.
 */
public class ReaderContext {

    public Map<Object, Object> objectMap;
    /** Same object as {@link #objectMap} when it is a {@link CompactMap}, so int keys can be put unboxed. */
    public CompactMap compact;
    public List<Object> array;
    public boolean isArray;
    public int remaining;

    public ReaderContext initMap(Map<Object, Object> objectMap, int size) {
        this.objectMap = objectMap;
        this.compact = objectMap instanceof CompactMap cm ? cm : null;
        this.array = null;
        this.isArray = false;
        this.remaining = size;
        return this;
    }

    public ReaderContext initArray(List<Object> objectList, int size) {
        this.objectMap = null;
        this.compact = null;
        this.array = objectList;
        this.isArray = true;
        this.remaining = size;
        return this;
    }

    public void clear() {
        this.objectMap = null;
        this.compact = null;
        this.array = null;
    }
}
