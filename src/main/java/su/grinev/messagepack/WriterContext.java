package su.grinev.messagepack;

import java.util.Iterator;
import java.util.Map;

public class WriterContext {

    public Iterator<Map.Entry<Object, Object>> objectMap;
    public Iterator<Object> array;

    public WriterContext initMap(Iterator<Map.Entry<Object, Object>> objectMap) {
        this.objectMap = objectMap;
        return this;
    }

    public WriterContext initList(Iterator<Object> arrayMap) {
        this.array = arrayMap;
        return this;
    }

    public void reset() {
        objectMap = null;
        array = null;
    }
}
