package ru.yandex.clickhouse.util;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import org.testng.annotations.Test;

public class UtilsTest {
    @Test
    public void testUnsignedLeb128() throws Exception {
        DataInputStream input = prepareStream(new byte[] { 0 });
        assertEquals(Utils.readUnsignedLeb128(input), 0);

        input = prepareStream(new byte[] { -27, -114, 38 });
        assertEquals(Utils.readUnsignedLeb128(input), 624485);

        input = prepareStream(new byte[] { -128, -62, -41, 47 });
        assertEquals(Utils.readUnsignedLeb128(input), 100000000);
    }

    private DataInputStream prepareStream(byte[] input) {
        return new DataInputStream(new ByteArrayInputStream(input));
    }
}
