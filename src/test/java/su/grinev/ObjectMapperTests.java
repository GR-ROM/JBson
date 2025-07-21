package su.grinev;

import org.junit.jupiter.api.Test;
import su.grinev.test.TestTest;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ObjectMapperTests {

    @Test
    public void serializeAndDeserializeObjectTest() {
        ObjectMapper objectMapper = new ObjectMapper();

        byte[] b1 = new byte[100];
        byte[] b2 = new byte[100];

        for (int i = 0; i < 100; i++) {
            b1[i] = (byte) i;
            b2[i] = (byte) i;
        }

        TestTest nested = new TestTest();
        nested.setB(b1);

        ByteBuffer b = objectMapper.serialize(nested);

        TestTest deserialized = objectMapper.deserialize(b, TestTest.class);

        assertEquals(nested, deserialized);
    }
}
