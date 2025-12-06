package su.grinev.test;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VpnForwardPacketDto {
    private ByteBuffer packet;
}