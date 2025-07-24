package su.grinev.bson;

import lombok.Getter;

import java.util.*;

@Getter
public class Document {

    private final Map<String, Object> documentMap;
    private final int length;

    public Document(Map<String, Object> documentMap, int length) {
        this.documentMap = documentMap;
        this.length = length;
    }

    public Document(Map<String, Object> documentMap) {
        this.documentMap = documentMap;
        this.length = 0;
    }

    public Object get(String key) {
        List<String> path = Arrays.stream(key.split("\\.")).toList();

        Iterator<String> i = path.iterator();
        Map<String, Object> map = documentMap;
        while (i.hasNext()) {
            Object o = map.get(i.next());
            if (i.hasNext()) {
                map = (Map<String, Object>) o;
            } else {
                return o;
            }
        }

        return null;
    }

}
