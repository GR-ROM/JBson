package su.grinev.messagepack;

import java.util.Map;

public class ReaderContext {

    public Map<String, Object> objectMap;
    public int size;
    public int index;

    public ReaderContext init(Map<String, Object> objectMap, int size) {
        this.objectMap = objectMap;
        this.size = size;
        this.index = 0;
        return this;
    }

    public void reset() {
        this.objectMap = null;
        this.size = 0;
        this.index = 0;
    }
}
