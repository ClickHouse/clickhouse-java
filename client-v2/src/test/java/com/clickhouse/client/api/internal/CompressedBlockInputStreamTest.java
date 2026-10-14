package com.clickhouse.client.api.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.data.ClickHouseByteUtils;
import com.clickhouse.data.ClickHouseCityHash;
import com.github.luben.zstd.Zstd;
import net.jpountz.lz4.LZ4Factory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CompressedBlockInputStreamTest {

    private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();

    @Test
    public void reportsActualByteCountsForTruncatedHeader() {
        byte[] truncatedHeader = new byte[10];
        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(truncatedHeader),
                LZ4_FACTORY.fastDecompressor(),
                8192);

        IOException exception = Assert.expectThrows(IOException.class,
                () -> input.read(new byte[1], 0, 1));

        Assert.assertEquals(exception.getMessage(), "Incomplete read: 10 of 25");
    }

    @DataProvider(name = "compressionAlgorithms")
    public static Object[][] compressionAlgorithms() {
        return new Object[][] {
                { CompressedBlockInputStream.MAGIC_LZ4 },
                { CompressedBlockInputStream.MAGIC_ZSTD_3 },
                { CompressedBlockInputStream.MAGIC_NONE }
        };
    }

    @Test(dataProvider = "compressionAlgorithms")
    public void testBlockDecompression(byte magic) throws IOException {
        byte[] payload = "Hello ClickHouse, testing LZ4, ZSTD and uncompressed blocks!".getBytes(StandardCharsets.UTF_8);
        byte[] block = createBlock(magic, payload);

        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(block),
                LZ4_FACTORY.fastDecompressor(),
                1024);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[11];
        int n;
        while ((n = input.read(buf, 0, buf.length)) != -1) {
            out.write(buf, 0, n);
        }

        Assert.assertEquals(out.toByteArray(), payload);
    }

    @Test(dataProvider = "compressionAlgorithms")
    public void testReadByteByByte(byte magic) throws IOException {
        byte[] payload = generateData(256);
        byte[] block = createBlock(magic, payload);

        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(block),
                LZ4_FACTORY.fastDecompressor(),
                1024);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int b;
        while ((b = input.read()) != -1) {
            out.write(b);
        }

        Assert.assertEquals(out.toByteArray(), payload);
    }

    @Test(dataProvider = "compressionAlgorithms")
    public void testMultiBlockStream(byte magic) throws IOException {
        byte[] part1 = generateData(500);
        byte[] part2 = generateData(700);

        ByteArrayOutputStream combined = new ByteArrayOutputStream();
        combined.write(createBlock(magic, part1));
        combined.write(createBlock(magic, part2));

        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(combined.toByteArray()),
                LZ4_FACTORY.fastDecompressor(),
                256);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[64];
        int n;
        while ((n = input.read(buf, 0, buf.length)) != -1) {
            out.write(buf, 0, n);
        }

        byte[] expected = new byte[part1.length + part2.length];
        System.arraycopy(part1, 0, expected, 0, part1.length);
        System.arraycopy(part2, 0, expected, part1.length, part2.length);

        Assert.assertEquals(out.toByteArray(), expected);
    }

    @Test
    public void testRoundTripWithOutputStream() throws IOException {
        byte[] payload = generateData(4096);

        // LZ4
        ByteArrayOutputStream lz4Out = new ByteArrayOutputStream();
        try (CompressedBlockOutputStream out = new CompressedBlockOutputStream.LZ4OutputStream(
                lz4Out, LZ4_FACTORY.fastCompressor(), 1024)) {
            out.write(payload);
        }
        try (CompressedBlockInputStream in = new CompressedBlockInputStream(
                new ByteArrayInputStream(lz4Out.toByteArray()), LZ4_FACTORY.fastDecompressor(), 512)) {
            ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
            byte[] buf = new byte[128];
            int n;
            while ((n = in.read(buf)) != -1) {
                decompressed.write(buf, 0, n);
            }
            Assert.assertEquals(decompressed.toByteArray(), payload);
        }

        // ZSTD
        ByteArrayOutputStream zstdOut = new ByteArrayOutputStream();
        try (CompressedBlockOutputStream out = new CompressedBlockOutputStream.ZSTDOutputStream(zstdOut, 1024)) {
            out.write(payload);
        }
        try (CompressedBlockInputStream in = new CompressedBlockInputStream(
                new ByteArrayInputStream(zstdOut.toByteArray()), LZ4_FACTORY.fastDecompressor(), 512)) {
            ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
            byte[] buf = new byte[128];
            int n;
            while ((n = in.read(buf)) != -1) {
                decompressed.write(buf, 0, n);
            }
            Assert.assertEquals(decompressed.toByteArray(), payload);
        }
    }

    @Test
    public void testInvalidMagicByte() {
        byte[] payload = generateData(32);
        byte[] block = createBlock((byte) 0x7F, payload);

        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(block),
                LZ4_FACTORY.fastDecompressor(),
                1024);

        ClientException ex = Assert.expectThrows(ClientException.class,
                () -> input.read(new byte[1], 0, 1));
        Assert.assertTrue(ex.getMessage().contains("Invalid LZ4 magic byte"));
    }

    @Test
    public void testChecksumMismatch() {
        byte[] payload = generateData(64);
        byte[] block = createBlock(CompressedBlockInputStream.MAGIC_LZ4, payload);
        block[block.length - 1] ^= 0xFF;

        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(block),
                LZ4_FACTORY.fastDecompressor(),
                1024);

        ClientException ex = Assert.expectThrows(ClientException.class,
                () -> input.read(new byte[1], 0, 1));
        Assert.assertTrue(ex.getMessage().contains("checksum mismatch"));
    }

    @Test
    public void testUnexpectedEndOfStreamInBlock() {
        byte[] payload = generateData(64);
        byte[] block = createBlock(CompressedBlockInputStream.MAGIC_LZ4, payload);
        byte[] truncated = Arrays.copyOf(block, 25); // header only, 0 bytes of payload

        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(truncated),
                LZ4_FACTORY.fastDecompressor(),
                1024);

        EOFException ex = Assert.expectThrows(EOFException.class, () -> input.read(new byte[1], 0, 1));
        Assert.assertEquals(ex.getMessage(), "Unexpected end of stream");
    }

    @Test
    public void testIncompleteReadInBlockPayload() {
        byte[] payload = generateData(64);
        byte[] block = createBlock(CompressedBlockInputStream.MAGIC_LZ4, payload);
        byte[] truncated = Arrays.copyOf(block, block.length - 5);

        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(truncated),
                LZ4_FACTORY.fastDecompressor(),
                1024);

        IOException ex = Assert.expectThrows(IOException.class, () -> input.read(new byte[1], 0, 1));
        Assert.assertTrue(ex.getMessage().startsWith("Incomplete read:"));
    }

    private static byte[] generateData(int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = (byte) ('A' + (i % 26));
        }
        return data;
    }

    private static byte[] createBlock(byte magic, byte[] payload) {
        int uncompressedSize = payload.length;
        byte[] compressedPayload;
        if (magic == CompressedBlockInputStream.MAGIC_LZ4) {
            compressedPayload = LZ4_FACTORY.fastCompressor().compress(payload);
        } else if (magic == CompressedBlockInputStream.MAGIC_ZSTD_3) {
            compressedPayload = Zstd.compress(payload, 3);
        } else if (magic == CompressedBlockInputStream.MAGIC_NONE) {
            compressedPayload = payload;
        } else {
            compressedPayload = payload;
        }

        int compressedSizeWithHeader = compressedPayload.length + 9;
        byte[] block = new byte[compressedSizeWithHeader];
        block[0] = magic;
        CompressedBlockInputStream.setInt32(block, 1, compressedSizeWithHeader);
        CompressedBlockInputStream.setInt32(block, 5, uncompressedSize);
        System.arraycopy(compressedPayload, 0, block, 9, compressedPayload.length);

        long[] checksum = ClickHouseCityHash.cityHash128(block, 0, compressedSizeWithHeader);

        byte[] rawBlock = new byte[16 + compressedSizeWithHeader];
        ClickHouseByteUtils.setInt64(rawBlock, 0, checksum[0]);
        ClickHouseByteUtils.setInt64(rawBlock, 8, checksum[1]);
        System.arraycopy(block, 0, rawBlock, 16, compressedSizeWithHeader);
        return rawBlock;
    }
}
