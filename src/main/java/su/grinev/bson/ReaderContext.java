package su.grinev.bson;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@Accessors(chain=true)
@NoArgsConstructor
public class ReaderContext {
    private int length;
    private Object value;
    private boolean isNestedObjectPending;

    public Object getValue() {
        return value;
    }
}
