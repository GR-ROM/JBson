package su.grinev.messagepack;

import java.util.List;
import java.util.Map;

public class ReaderContext {

    public Map<Integer, Object> objectMap;
    public List<Object> objectList;
    public boolean isArray;
    public int size;
    public int index;

    public ReaderContext initMap(Map<Integer, Object> objectMap, int size) {
        this.objectMap = objectMap;
        this.objectList = null;
        this.isArray = false;
        this.size = size;
        this.index = 0;
        return this;
    }

    public ReaderContext initArray(List<Object> objectList, int size) {
        this.objectMap = null;
        this.objectList = objectList;
        this.isArray = true;
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
