package su.grinev.messagepack;

import java.util.*;

/**
 * Zero-allocation map for small integer keys (0..15) plus a discriminator slot (1488) — the shape
 * of a tagged protocol document. Uses flat arrays: no HashMap$Node allocation on put/get, and an
 * {@code int}-keyed {@link #putInt(int, Object)} that never boxes.
 *
 * <p>Any other key (an int tag of 16 or more, a negative int, a string) goes to a lazily allocated
 * overflow {@link HashMap}, so the map is general; only the common case is free. The overflow map
 * is kept (emptied, not dropped) across {@link #clear()} so a pooled instance stays allocation-free
 * once it has seen such a key. Implements Map<Object, Object> for compatibility with BinaryDocument.
 */
public final class CompactMap implements Map<Object, Object> {

    private static final int SMALL_LIMIT = 16;
    private static final int DISCRIMINATOR_KEY = 1488;
    private static final Integer DISCRIMINATOR_KEY_BOXED = DISCRIMINATOR_KEY;

    private final Object[] values = new Object[SMALL_LIMIT];
    private long presentBits;
    private Object discriminatorValue;
    private boolean discriminatorPresent;
    private HashMap<Object, Object> overflow;   // null until a key outside the fast slots shows up
    private int size;

    /**
     * {@code int}-keyed put: the fast slots without boxing; other ints go to the overflow map.
     * Named rather than overloaded because {@code put(int, Object)} vs {@code put(Object, Object)} is
     * ambiguous for a call like {@code put(0, 1)} once boxing is in play.
     */
    public Object putInt(int key, Object value) {
        if (key >= 0 && key < SMALL_LIMIT) {
            Object old = values[key];
            values[key] = value;
            long bit = 1L << key;
            if ((presentBits & bit) == 0) {
                presentBits |= bit;
                size++;
            }
            return old;
        }
        if (key == DISCRIMINATOR_KEY) {
            Object old = discriminatorValue;
            discriminatorValue = value;
            if (!discriminatorPresent) {
                discriminatorPresent = true;
                size++;
            }
            return old;
        }
        return putOverflow(key, value);
    }

    @Override
    public Object put(Object key, Object value) {
        if (key instanceof Integer k) {
            return putInt(k.intValue(), value);
        }
        if (key == null) {
            throw new UnsupportedOperationException("null key");
        }
        return putOverflow(key, value);
    }

    private Object putOverflow(Object key, Object value) {
        if (overflow == null) {
            overflow = new HashMap<>();
        }
        int before = overflow.size();
        Object old = overflow.put(key, value);
        if (overflow.size() != before) {
            size++;
        }
        return old;
    }

    public Object get(int key) {
        if (key >= 0 && key < SMALL_LIMIT) {
            return (presentBits & (1L << key)) != 0 ? values[key] : null;
        }
        if (key == DISCRIMINATOR_KEY) {
            return discriminatorPresent ? discriminatorValue : null;
        }
        return overflow == null ? null : overflow.get(key);
    }

    @Override
    public Object get(Object key) {
        if (key instanceof Integer k) {
            return get(k.intValue());
        }
        return overflow == null || key == null ? null : overflow.get(key);
    }

    @Override
    public boolean containsKey(Object key) {
        if (key instanceof Integer k) {
            int i = k;
            if (i >= 0 && i < SMALL_LIMIT) return (presentBits & (1L << i)) != 0;
            if (i == DISCRIMINATOR_KEY) return discriminatorPresent;
        }
        return overflow != null && key != null && overflow.containsKey(key);
    }

    @Override
    public Object remove(Object key) {
        if (key instanceof Integer k) {
            int i = k;
            if (i >= 0 && i < SMALL_LIMIT) {
                long bit = 1L << i;
                if ((presentBits & bit) != 0) {
                    Object old = values[i];
                    values[i] = null;
                    presentBits &= ~bit;
                    size--;
                    return old;
                }
                return null;
            }
            if (i == DISCRIMINATOR_KEY) {
                if (discriminatorPresent) {
                    Object old = discriminatorValue;
                    discriminatorValue = null;
                    discriminatorPresent = false;
                    size--;
                    return old;
                }
                return null;
            }
        }
        if (overflow == null || key == null || !overflow.containsKey(key)) {
            return null;
        }
        size--;
        return overflow.remove(key);
    }

    @Override
    public void clear() {
        if (presentBits != 0) {
            long bits = presentBits;
            while (bits != 0) {
                int i = Long.numberOfTrailingZeros(bits);
                values[i] = null;
                bits &= bits - 1;
            }
            presentBits = 0;
        }
        discriminatorValue = null;
        discriminatorPresent = false;
        if (overflow != null && !overflow.isEmpty()) {
            overflow.clear();
        }
        size = 0;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    private boolean hasOverflow() {
        return overflow != null && !overflow.isEmpty();
    }

    @Override
    public boolean containsValue(Object value) {
        long bits = presentBits;
        while (bits != 0) {
            int i = Long.numberOfTrailingZeros(bits);
            if (Objects.equals(values[i], value)) return true;
            bits &= bits - 1;
        }
        if (discriminatorPresent && Objects.equals(discriminatorValue, value)) return true;
        return hasOverflow() && overflow.containsValue(value);
    }

    @Override
    public void putAll(Map<?, ?> m) {
        for (Entry<?, ?> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public Set<Object> keySet() {
        Set<Object> keys = new LinkedHashSet<>(size);
        long bits = presentBits;
        while (bits != 0) {
            keys.add(Long.numberOfTrailingZeros(bits));
            bits &= bits - 1;
        }
        if (discriminatorPresent) keys.add(DISCRIMINATOR_KEY);
        if (hasOverflow()) keys.addAll(overflow.keySet());
        return keys;
    }

    @Override
    public Collection<Object> values() {
        List<Object> vals = new ArrayList<>(size);
        long bits = presentBits;
        while (bits != 0) {
            vals.add(values[Long.numberOfTrailingZeros(bits)]);
            bits &= bits - 1;
        }
        if (discriminatorPresent) vals.add(discriminatorValue);
        if (hasOverflow()) vals.addAll(overflow.values());
        return vals;
    }

    /**
     * Returns a reusable iterator over entries. Zero allocation unless the map has overflow keys
     * (then one HashMap iterator for them). NOT thread-safe — single consumer only.
     */
    public EntryIterator entryIterator() {
        reusableIterator.reset();
        return reusableIterator;
    }

    private final EntryIterator reusableIterator = new EntryIterator();
    private final ReusableEntry reusableEntry = new ReusableEntry();

    @Override
    public Set<Entry<Object, Object>> entrySet() {
        Set<Entry<Object, Object>> entries = new LinkedHashSet<>(size);
        long bits = presentBits;
        while (bits != 0) {
            int i = Long.numberOfTrailingZeros(bits);
            entries.add(new AbstractMap.SimpleImmutableEntry<>(i, values[i]));
            bits &= bits - 1;
        }
        if (discriminatorPresent) {
            entries.add(new AbstractMap.SimpleImmutableEntry<>(DISCRIMINATOR_KEY, discriminatorValue));
        }
        if (hasOverflow()) {
            for (Entry<Object, Object> e : overflow.entrySet()) {
                entries.add(new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue()));
            }
        }
        return entries;
    }

    final class ReusableEntry implements Entry<Object, Object> {
        int key;
        Object value;

        @Override public Object getKey() {
            // keys 0-15: Integer.valueOf returns JVM-cached instances (range -128..127)
            // key 1488: use pre-cached boxed constant to avoid allocation
            return key == DISCRIMINATOR_KEY ? DISCRIMINATOR_KEY_BOXED : Integer.valueOf(key);
        }
        @Override public Object getValue() { return value; }
        @Override public Object setValue(Object value) { throw new UnsupportedOperationException(); }
    }

    public final class EntryIterator implements Iterator<Entry<Object, Object>> {
        private long remainingBits;
        private boolean discriminatorRemaining;
        private Iterator<Entry<Object, Object>> overflowIterator;

        void reset() {
            remainingBits = presentBits;
            discriminatorRemaining = discriminatorPresent;
            overflowIterator = hasOverflow() ? overflow.entrySet().iterator() : null;
        }

        @Override
        public boolean hasNext() {
            return remainingBits != 0 || discriminatorRemaining
                    || (overflowIterator != null && overflowIterator.hasNext());
        }

        @Override
        public Entry<Object, Object> next() {
            if (remainingBits != 0) {
                int i = Long.numberOfTrailingZeros(remainingBits);
                remainingBits &= remainingBits - 1;
                reusableEntry.key = i;
                reusableEntry.value = values[i];
                return reusableEntry;
            }
            if (discriminatorRemaining) {
                discriminatorRemaining = false;
                reusableEntry.key = DISCRIMINATOR_KEY;
                reusableEntry.value = discriminatorValue;
                return reusableEntry;
            }
            if (overflowIterator != null && overflowIterator.hasNext()) {
                return overflowIterator.next();
            }
            throw new NoSuchElementException();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        long bits = presentBits;
        while (bits != 0) {
            int i = Long.numberOfTrailingZeros(bits);
            if (!first) sb.append(", ");
            sb.append(i).append("=").append(values[i]);
            first = false;
            bits &= bits - 1;
        }
        if (discriminatorPresent) {
            if (!first) sb.append(", ");
            sb.append(DISCRIMINATOR_KEY).append("=").append(discriminatorValue);
            first = false;
        }
        if (hasOverflow()) {
            for (Entry<Object, Object> e : overflow.entrySet()) {
                if (!first) sb.append(", ");
                sb.append(e.getKey()).append("=").append(e.getValue());
                first = false;
            }
        }
        sb.append("}");
        return sb.toString();
    }
}
