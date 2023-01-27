package com.clickhouse.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHousePassThruStreamTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        final InputStream in = new ByteArrayInputStream(new byte[0]);
        final OutputStream out = new ByteArrayOutputStream();

        ClickHousePassThruStream stream = ClickHousePassThruStream.of(in, null, null);
        Assert.assertNotEquals(stream.getInputStream(), ClickHouseInputStream.empty());
        Assert.assertEquals(stream.getOutputStream(), ClickHouseOutputStream.empty());
        Assert.assertFalse(stream.hasCompression());
        Assert.assertTrue(stream.hasInput());
        Assert.assertFalse(stream.hasOutput());
        Assert.assertFalse(stream.isCompressed());
        Assert.assertFalse(stream.hasFormat());
        Assert.assertNull(stream.getCompressionAlgorithm());
        Assert.assertEquals(stream.getCompressionLevel(), ClickHouseDataConfig.DEFAULT_COMPRESS_LEVEL);
        Assert.assertNull(stream.getFormat());

        stream = ClickHousePassThruStream.of(out, null, -1, null);
        Assert.assertEquals(stream.getInputStream(), ClickHouseInputStream.empty());
        Assert.assertNotEquals(stream.getOutputStream(), ClickHouseOutputStream.empty());
        Assert.assertFalse(stream.hasCompression());
        Assert.assertFalse(stream.hasInput());
        Assert.assertTrue(stream.hasOutput());
        Assert.assertFalse(stream.isCompressed());
        Assert.assertFalse(stream.hasFormat());
        Assert.assertNull(stream.getCompressionAlgorithm());
        Assert.assertEquals(stream.getCompressionLevel(), -1);
        Assert.assertNull(stream.getFormat());

        stream = ClickHousePassThruStream.of(in, out, ClickHouseCompression.ZSTD, 22, ClickHouseFormat.JSON);
        Assert.assertNotEquals(stream.getInputStream(), ClickHouseInputStream.empty());
        Assert.assertNotEquals(stream.getOutputStream(), ClickHouseOutputStream.empty());
        Assert.assertTrue(stream.hasCompression());
        Assert.assertTrue(stream.hasInput());
        Assert.assertTrue(stream.hasOutput());
        Assert.assertTrue(stream.isCompressed());
        Assert.assertTrue(stream.hasFormat());
        Assert.assertEquals(stream.getCompressionAlgorithm(), ClickHouseCompression.ZSTD);
        Assert.assertEquals(stream.getCompressionLevel(), 22);
        Assert.assertEquals(stream.getFormat(), ClickHouseFormat.JSON);

        Assert.assertTrue(ClickHousePassThruStream.of(null, null, null, -1, null) == ClickHousePassThruStream.NULL);
        Assert.assertTrue(ClickHousePassThruStream.of(null, null, ClickHouseCompression.NONE, -1,
                null) != ClickHousePassThruStream.NULL);
    }
}
