package su.grinev.bson;

public class Context {
    private int pos;
    private Object value;

    public Context(int pos, Object value) {
        this.pos = pos;
        this.value = value;
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
}
