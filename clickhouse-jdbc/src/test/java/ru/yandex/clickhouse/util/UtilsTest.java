package ru.yandex.clickhouse.util;

import static org.testng.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.testng.annotations.Test;

public class UtilsTest {
    @Test(groups = "unit")
    public void testUnsignedLeb128() throws Exception {
        DataInputStream input = prepareStream(new byte[] { 0 });
        assertEquals(Utils.readUnsignedLeb128(input), 0);

        input = prepareStream(new byte[] { -27, -114, 38 });
        assertEquals(Utils.readUnsignedLeb128(input), 624485);

        input = prepareStream(new byte[] { -128, -62, -41, 47 });
        assertEquals(Utils.readUnsignedLeb128(input), 100000000);
    }

    @Test(groups = "unit")
    public void testString() {
        Charset charset = Charset.forName("ISO-8859-15");
        byte[] b1 = new byte[] { (byte) 127, (byte) 128 };
        String s = new String(b1, charset);
        byte[] b2 = s.getBytes(charset);
        assertEquals(b2, b1);
    }

    @Test(groups = "unit")
    public void testVarInt() {
        ByteBuffer buffer;
        for (int i : new int[] { 0, 128, 255, 65535, 1023 * 1024 }) {
            buffer = ByteBuffer.allocate(8);
            Utils.writeVarInt(i, buffer);
            Utils.writeVarInt(0 - i, buffer);
            buffer = (ByteBuffer) ((Buffer) buffer.flip());
            assertEquals(Utils.readVarInt(buffer), i);
            assertEquals(Utils.readVarInt(buffer), 0 - i);
        }
    }

    private DataInputStream prepareStream(byte[] input) {
        return new DataInputStream(new ByteArrayInputStream(input));
    }
}
