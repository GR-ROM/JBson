package su.grinev;

public interface Binder {

    void bind(KeyTypeValue keyTypeValue);


    record KeyTypeValue(String key, String type, Object value) {};
}
