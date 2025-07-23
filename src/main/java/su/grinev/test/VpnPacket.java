package su.grinev.test;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.math.BigDecimal;

@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class VpnPacket {
    private BigDecimal bigDecimal;
    private String encoding;
    private byte[] packet;
}
