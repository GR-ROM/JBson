package su.grinev.bson;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain=true)
public class ReaderContext {
    private int pos;
    private Object value;
    private boolean isNestedObjectPending;

    public ReaderContext() {
        this.pos = 0;
    }

    public int getPos() {
        return pos;
    }

    public Object getValue() {
        return value;
    }
}
