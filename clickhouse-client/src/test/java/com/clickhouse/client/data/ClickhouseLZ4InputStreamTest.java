package com.clickhouse.client.data;

import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ClickhouseLZ4InputStreamTest {
    @Test(groups = { "unit" })
    public void testLZ4Stream() throws IOException {
        StringBuilder sb = new StringBuilder();
        byte[] result = null;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ClickHouseLZ4OutputStream outputStream = new ClickHouseLZ4OutputStream(baos, 1024 * 1024)) {
            for (int i = 0; i < 100000; i++) {
                outputStream.write(("test" + i).getBytes());
                sb.append("test").append(i);
            }
            outputStream.flush();
            result = baos.toByteArray();
            // System.out.println(result.length);
            Assert.assertTrue(result.length < sb.length() / 2);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(result);
                ClickHouseLZ4InputStream is = new ClickHouseLZ4InputStream(bais)) {
            byte[] buf = new byte[20000000];
            int read = is.read(buf);
            // System.out.println(read);
            Assert.assertEquals(new String(buf, 0, read), sb.toString());
        }
    }
}
