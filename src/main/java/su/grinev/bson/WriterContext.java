package su.grinev.bson;

import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;

@Setter
@Accessors(chain = true)
@NoArgsConstructor
public final class WriterContext {
    int idx;
    int lengthPos = 0;
    int startPos = 0;
    Map.Entry<String, Object>[] mapEntries;;

    public static WriterContext fillForDocument(
            WriterContext writerContext,
            int lengthPos,
            Map<String, Object> value
            ) {
        return writerContext
                .setLengthPos(lengthPos)
                .setIdx(0)
                .setMapEntries(objectToMapEntries(value));
    }

    public static WriterContext fillForArray(
            WriterContext writerContext,
            int lengthPos,
            List<Object> value
    ) {
        return writerContext
                .setLengthPos(lengthPos)
                .setIdx(0)
                .setMapEntries(listToMapEntries(value));
    }

    private static Map.Entry<String, Object>[] objectToMapEntries(Map<String, Object> value) {
        Map.Entry<String, Object>[] mapEntries = new Map.Entry[value.size()];
        int i = 0;
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            mapEntries[i++] =   Map.entry(entry.getKey(), entry.getValue() == null ? new NullObject() : entry.getValue());
        }

        return mapEntries;
    }

    private static Map.Entry<String, Object>[] listToMapEntries(List<Object> value) {
        Map.Entry<String, Object>[] mapEntries = new Map.Entry[value.size()];
        int i = 0;
        for (Object object : value) {
            mapEntries[i] = Map.entry(Integer.toString(i), object == null ? new NullObject() : object);
            i++;
        }

        return mapEntries;
    }

    public static class NullObject {

    }
}