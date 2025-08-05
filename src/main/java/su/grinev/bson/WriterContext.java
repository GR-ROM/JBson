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
    boolean isNestedObjectPending;
    List<Map.Entry<String, Object>> mapEntries;
    List<Object> listEntries;

    public static WriterContext fillForDocument(
            WriterContext writerContext,
            int lengthPos,
            Map<String, Object> value
            ) {
        return writerContext
                .setNestedObjectPending(false)
                .setLengthPos(lengthPos)
                .setIdx(0)
                .setMapEntries(value.entrySet().stream().toList())
                .setListEntries(null);
    }

    public static WriterContext fillForArray(
            WriterContext writerContext,
            int lengthPos,
            List<Object> value
    ) {
        return writerContext
                .setNestedObjectPending(false)
                .setLengthPos(lengthPos)
                .setIdx(0)
                .setMapEntries(null)
                .setListEntries(value);
    }
}