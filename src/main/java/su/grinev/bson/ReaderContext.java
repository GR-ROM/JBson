package su.grinev.bson;

public class ReaderContext {
    private int pos;
    private String key;
    private Object value;

    public ReaderContext(int pos) {
        this.pos = pos;
    }

    public int getPos() {
        return pos;
    }

    public Object getValue() {
        return value;
    }

    public ReaderContext setPos(int pos) {
        this.pos = pos;
        return this;
    }

    public ReaderContext setValue(Object value) {
        this.value = value;
        return this;
    }

    public ReaderContext setKey(String key) {
        this.key = key;
        return this;
    }

    public String getKey() {
        return key;
    }
}
