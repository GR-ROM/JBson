package su.grinev.test;

import annotation.BsonType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VpnResponseDto<T> {

    private String protocolVersion;
    private Status status;
    private Instant timestamp;
    @BsonType(discriminator = "_dataType")
    private T data;

    public static <T> VpnResponseDto<T> wrap(Status status, T data) {
        return VpnResponseDto.<T>builder()
                .protocolVersion("0.1")
                .status(status)
                .data(data)
                .timestamp(Instant.now())
                .build();
    }
}


