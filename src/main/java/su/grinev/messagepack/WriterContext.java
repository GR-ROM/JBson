package su.grinev.messagepack;

import java.util.Iterator;
import java.util.Map;

public class WriterContext {

    public Iterator<Map.Entry<String, Object>> objectMap;

    public WriterContext init(Iterator<Map.Entry<String, Object>> objectMap) {
        this.objectMap = objectMap;
        return this;
    }

    public void reset() {
        this.objectMap = null;
    }
}
