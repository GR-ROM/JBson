package su.grinev.test;

import annotation.BsonType;
import lombok.Data;

@Data
public class TestTest {
    private int i;
    private double d;
    private String s;
    private byte[] b;

    @BsonType(discriminator = "_nestedType")
    private TestTest nestedTestTest;
}