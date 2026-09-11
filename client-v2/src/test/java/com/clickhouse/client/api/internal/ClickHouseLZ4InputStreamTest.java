package com.clickhouse.client.api.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import net.jpountz.lz4.LZ4Factory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseLZ4InputStreamTest {

    @Test
    public void returnsDecodedBlockBeforeFailingOnNextHeader() throws Exception {
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        try (ClickHouseLZ4OutputStream output = new ClickHouseLZ4OutputStream(encoded,
                LZ4Factory.fastestJavaInstance().fastCompressor(), 8192)) {
            output.write(new byte[] {1, 2, 3});
        }
        encoded.write(new byte[10]);
        try (ClickHouseLZ4InputStream input = new ClickHouseLZ4InputStream(
                new ByteArrayInputStream(encoded.toByteArray()),
                LZ4Factory.fastestJavaInstance().fastDecompressor(), 8192)) {
            byte[] buffer = new byte[64];
            Assert.assertEquals(input.read(buffer), 3);
            Assert.assertEquals(java.util.Arrays.copyOf(buffer, 3), new byte[] {1, 2, 3});
            Assert.expectThrows(IOException.class, () -> input.read(buffer));
        }
    }

    @Test
    public void reportsActualByteCountsForTruncatedHeader() {
        byte[] truncatedHeader = new byte[10];
        ClickHouseLZ4InputStream input = new ClickHouseLZ4InputStream(
                new ByteArrayInputStream(truncatedHeader),
                LZ4Factory.fastestJavaInstance().fastDecompressor(),
                8192);

        IOException exception = Assert.expectThrows(IOException.class,
                () -> input.read(new byte[1], 0, 1));

        Assert.assertEquals(exception.getMessage(), "Incomplete read: 10 of 25");
    }
}
