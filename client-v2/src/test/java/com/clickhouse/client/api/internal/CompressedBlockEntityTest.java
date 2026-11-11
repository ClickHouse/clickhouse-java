package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.CompressionMethod;
import net.jpountz.lz4.LZ4Factory;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class CompressedBlockEntityTest {

    private static final String TEXT = "Code: 115. DB::Exception: Setting unknown_setting is neither a builtin setting";

    @DataProvider(name = "httpCompressedResponseBodies")
    public static Object[][] httpCompressedResponseBodies() throws IOException {
        ByteArrayOutputStream lz4Frame = new ByteArrayOutputStream();
        try (OutputStream out = new FramedLZ4CompressorOutputStream(lz4Frame)) {
            out.write(TEXT.getBytes(StandardCharsets.UTF_8));
        }
        return new Object[][] {
                // response body, expected content
                {lz4Frame.toByteArray(), TEXT},
                {TEXT.getBytes(StandardCharsets.UTF_8), TEXT}, // body that the server did not compress
                {"Co".getBytes(StandardCharsets.UTF_8), "Co"}, // shorter than the LZ4 frame magic number
                {new byte[0], ""},
        };
    }

    @Test(dataProvider = "httpCompressedResponseBodies")
    public void testHttpCompressedResponseContent(byte[] body, String expectedContent) throws IOException {
        CompressedBlockEntity entity = new CompressedBlockEntity(new ByteArrayEntity(body, ContentType.TEXT_PLAIN),
                true, true, false, 8192, true, LZ4Factory.fastestInstance(), CompressionMethod.LZ4);

        ByteArrayOutputStream content = new ByteArrayOutputStream();
        try (InputStream in = entity.getContent()) {
            byte[] buffer = new byte[64];
            int n;
            while ((n = in.read(buffer)) != -1) {
                content.write(buffer, 0, n);
            }
        }
        Assert.assertEquals(new String(content.toByteArray(), StandardCharsets.UTF_8), expectedContent);
    }
}
