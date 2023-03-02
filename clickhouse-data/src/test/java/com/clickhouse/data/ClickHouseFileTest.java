package com.clickhouse.data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseFileTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        final InputStream in = new ByteArrayInputStream(new byte[0]);

        String nonExistingFile = Paths
                .get(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString() + ".csv.lz4").toFile()
                .getAbsolutePath();
        ClickHouseFile file = ClickHouseFile.of(nonExistingFile);
        Assert.assertFalse(file.hasInput());
        Assert.assertTrue(file.hasOutput());
        Assert.assertTrue(file.hasCompression());
        Assert.assertEquals(file.getCompressionAlgorithm(), ClickHouseCompression.LZ4);
        Assert.assertEquals(file.getCompressionLevel(), ClickHouseDataConfig.DEFAULT_COMPRESS_LEVEL);
        Assert.assertTrue(file.hasFormat());
        Assert.assertEquals(file.getFormat(), ClickHouseFormat.CSV);
        Assert.assertEquals(file.getFile().getAbsolutePath(), nonExistingFile);

        file = ClickHouseFile.of(in, null, 0, ClickHouseFormat.CSV);
        Assert.assertTrue(file.hasInput());
        Assert.assertTrue(file.hasOutput());
        Assert.assertFalse(file.hasCompression());
        Assert.assertNull(file.getCompressionAlgorithm());
        Assert.assertEquals(file.getCompressionLevel(), 0);
        Assert.assertTrue(file.hasFormat());
        Assert.assertEquals(file.getFormat(), ClickHouseFormat.CSV);
        Assert.assertTrue(file.getFile().exists());

        file = ClickHouseFile.of(ClickHouseInputStream.of(in, ClickHouseCompression.ZSTD), ClickHouseCompression.LZ4,
                2, null);
        Assert.assertTrue(file.hasInput());
        Assert.assertTrue(file.hasOutput());
        Assert.assertTrue(file.hasCompression());
        Assert.assertEquals(file.getCompressionAlgorithm(), ClickHouseCompression.LZ4);
        Assert.assertEquals(file.getCompressionLevel(), 2);
        Assert.assertFalse(file.hasFormat());
        Assert.assertNull(file.getFormat());
        Assert.assertTrue(file.getFile().exists());

        file = ClickHouseFile.of(
                ClickHouseInputStream.of(
                        ClickHousePassThruStream.of(in, ClickHouseCompression.ZSTD, 9, ClickHouseFormat.TSV), 0, null),
                ClickHouseCompression.LZ4, 3, ClickHouseFormat.CSV);
        Assert.assertTrue(file.hasInput());
        Assert.assertTrue(file.hasOutput());
        Assert.assertTrue(file.hasCompression());
        Assert.assertEquals(file.getCompressionAlgorithm(), ClickHouseCompression.ZSTD);
        Assert.assertEquals(file.getCompressionLevel(), 9);
        Assert.assertTrue(file.hasFormat());
        Assert.assertEquals(file.getFormat(), ClickHouseFormat.TSV);
        Assert.assertTrue(file.getFile().exists());

        file = ClickHouseFile
                .of(ClickHousePassThruStream.of(in, ClickHouseCompression.BROTLI, 7, ClickHouseFormat.RowBinary));
        Assert.assertTrue(file.hasInput());
        Assert.assertTrue(file.hasOutput());
        Assert.assertTrue(file.hasCompression());
        Assert.assertEquals(file.getCompressionAlgorithm(), ClickHouseCompression.BROTLI);
        Assert.assertEquals(file.getCompressionLevel(), 7);
        Assert.assertTrue(file.hasFormat());
        Assert.assertEquals(file.getFormat(), ClickHouseFormat.RowBinary);
        Assert.assertTrue(file.getFile().exists());
    }
}
