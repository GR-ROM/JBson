package su.grinev.pool;

/**
 * Thrown when an object is used or released after its reference count has already reached zero — i.e.
 * after it has gone back to the pool and possibly to another owner.
 *
 * <p>Unchecked and thrown eagerly on purpose. The alternative, which this codebase has lived with, is
 * that a double release puts one buffer into the idle queue twice, two threads are handed the same
 * memory, and the symptom appears later as corrupted traffic or a direct-memory sawtooth — arbitrarily
 * far from the call that caused it.
 */
public class IllegalReferenceCountException extends IllegalStateException {

    public IllegalReferenceCountException(int refCnt, int decrement) {
        super("refCnt: " + refCnt + ", decrement: " + decrement);
    }

    public IllegalReferenceCountException(String message) {
        super(message);
    }
}
