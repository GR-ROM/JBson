package su.grinev.pool;

/**
 * The handle a {@link LeakDetector} gives back for one watched object.
 *
 * <p>Closing it says "this object was returned to its pool, stop watching". An object collected by
 * the GC with its tracker still open is a leak, and the tracker is what carries the evidence.
 */
public interface LeakTracker {

    /**
     * Notes where the object was just handled, so a later leak report can say what it was doing
     * before it was lost. Only kept at {@code advanced} and above.
     */
    void record(Object hint);

    /**
     * Stops watching the object.
     *
     * @return true if this call closed it, false if it was already closed
     */
    boolean close();
}
