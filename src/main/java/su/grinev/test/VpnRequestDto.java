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
public class VpnRequestDto<T> {

    private String protocolVersion;
    private Command command;
    private Instant timestamp;

    @BsonType(discriminator = "_dataType")
    private T data;

    public static <T> VpnRequestDto<T> wrap(Command command, T data) {
        return VpnRequestDto.<T>builder()
                .protocolVersion("0.1")
                .command(command)
                .data(data)
                .timestamp(Instant.now())
                .build();
    }
}

