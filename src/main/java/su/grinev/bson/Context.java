package su.grinev.bson;

public class Context {
    private int pos;
    private String key;
    private Object value;

    public Context(int pos) {
        this.pos = pos;
    }

    public int getPos() {
        return pos;
    }

    public Object getValue() {
        return value;
    }

    public Context setPos(int pos) {
        this.pos = pos;
        return this;
    }

    public Context setValue(Object value) {
        this.value = value;
        return this;
    }

    public Context setKey(String key) {
        this.key = key;
        return this;
    }

    public String getKey() {
        return key;
    }
}
