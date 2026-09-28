package com.clickhouse.client.api.internal;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import net.jpountz.lz4.LZ4Factory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CompressedBlockInputStreamTest {

    @Test
    public void reportsActualByteCountsForTruncatedHeader() {
        byte[] truncatedHeader = new byte[10];
        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(truncatedHeader),
                LZ4Factory.fastestJavaInstance().fastDecompressor(),
                8192);

        IOException exception = Assert.expectThrows(IOException.class,
                () -> input.read(new byte[1], 0, 1));

        Assert.assertEquals(exception.getMessage(), "Incomplete read: 10 of 25");
    }
}
