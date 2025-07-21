package su.grinev.test;

import lombok.Data;

@Data
public class TestTest {
    private int i;
    private double d;
    private String s;
    private byte[] b;
    private TestTest nestedTestTest;
}