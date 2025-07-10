package su.grinev.test;

import annotation.BsonType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class VpnRequest<T> {
    private String ver;
    private String type;
    private Map<String, String> opts;
    private List<String> reserved;
    @BsonType(discriminator = "type")
    private T data;
}


