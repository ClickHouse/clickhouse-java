package ru.yandex.clickhouse;

import org.testng.Assert;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.response.ClickHouseLZ4Stream;
import ru.yandex.clickhouse.util.ClickHouseLZ4OutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ClickhouseLZ4StreamTest {
    @Test(groups = "unit")
    public void testLZ4Stream() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ClickHouseLZ4OutputStream outputStream = new ClickHouseLZ4OutputStream(baos,  1024*1024);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            outputStream.write(("test"+i).getBytes());
            sb.append("test").append(i);
        }
        outputStream.flush();
        byte[] result = baos.toByteArray();
        // System.out.println(result.length);
        Assert.assertTrue(result.length < sb.length()/2);
        ByteArrayInputStream bais = new ByteArrayInputStream(result);
        ClickHouseLZ4Stream is = new ClickHouseLZ4Stream(bais);
        byte[] buf = new byte[20000000];
        int read = is.read(buf);
        // System.out.println(read);
        Assert.assertEquals(new String(buf, 0, read), sb.toString());
    }
}
