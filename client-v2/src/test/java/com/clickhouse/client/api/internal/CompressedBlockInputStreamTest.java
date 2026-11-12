package com.clickhouse.client.api.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.DataTransferException;
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

        DataTransferException exception = Assert.expectThrows(DataTransferException.class,
                () -> input.read(new byte[1], 0, 1));

        Assert.assertEquals(exception.getMessage(), "Incomplete read: 10 of 25");
    }

    @Test
    public void eofExceptionBeforeAnyByteIsATransferFailure() {
        EOFException eof = new EOFException("end");
        CompressedBlockInputStream input = new CompressedBlockInputStream(
                streamThatThrows(eof, new byte[0]),
                LZ4_FACTORY.fastDecompressor(),
                64);

        DataTransferException ex = Assert.expectThrows(DataTransferException.class,
                () -> input.read(new byte[1], 0, 1));
        Assert.assertEquals(ex.getMessage(), "Incomplete read: 0 of 25");
        Assert.assertSame(ex.getCause(), eof);
    }

    @Test
    public void eofExceptionAfterABlockIsATransferFailure() throws IOException {
        byte[] payload = "abc".getBytes(StandardCharsets.UTF_8);
        byte[] block = createBlock(CompressedBlockInputStream.MAGIC_NONE, payload);
        EOFException eof = new EOFException("end");
        CompressedBlockInputStream input = new CompressedBlockInputStream(
                streamThatThrows(eof, block),
                LZ4_FACTORY.fastDecompressor(),
                64);

        byte[] read = new byte[payload.length];
        Assert.assertEquals(input.read(read), payload.length);
        Assert.assertEquals(read, payload);
        DataTransferException ex = Assert.expectThrows(DataTransferException.class,
                () -> input.read(new byte[1], 0, 1));
        Assert.assertEquals(ex.getMessage(), "Incomplete read: 0 of 25");
        Assert.assertSame(ex.getCause(), eof);
    }

    @Test
    public void eofExceptionAfterAPartialHeaderIsAnIncompleteRead() {
        byte[] prefix = new byte[10];
        EOFException eof = new EOFException("end");
        CompressedBlockInputStream input = new CompressedBlockInputStream(
                streamThatThrows(eof, prefix),
                LZ4_FACTORY.fastDecompressor(),
                64);

        DataTransferException ex = Assert.expectThrows(DataTransferException.class,
                () -> input.read(new byte[1], 0, 1));
        Assert.assertEquals(ex.getMessage(), "Incomplete read: 10 of 25");
        Assert.assertSame(ex.getCause(), eof);
    }

    @Test
    public void eofExceptionAtBlockPayloadIsATransferFailure() {
        byte[] block = createBlock(CompressedBlockInputStream.MAGIC_NONE, generateData(32));
        byte[] header = Arrays.copyOf(block, CompressedBlockInputStream.HEADER_LENGTH);
        CompressedBlockInputStream input = new CompressedBlockInputStream(
                streamThatThrows(new EOFException("end"), header),
                LZ4_FACTORY.fastDecompressor(),
                64);

        DataTransferException ex = Assert.expectThrows(DataTransferException.class,
                () -> input.read(new byte[1], 0, 1));
        Assert.assertEquals(ex.getMessage(), "Incomplete read: 0 of 32");
        Assert.assertTrue(ex.getCause() instanceof EOFException);
    }

    @Test
    public void wrapsSourceReadFailure() {
        InputStream failing = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("socket closed");
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                throw new IOException("socket closed");
            }
        };
        CompressedBlockInputStream input = new CompressedBlockInputStream(
                failing, LZ4_FACTORY.fastDecompressor(), 64);

        DataTransferException ex = Assert.expectThrows(DataTransferException.class,
                () -> input.read(new byte[1], 0, 1));
        Assert.assertEquals(ex.getMessage(), "Incomplete read: 0 of 25");
        Assert.assertEquals(ex.getCause().getMessage(), "socket closed");
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

    @Test(dataProvider = "compressionAlgorithms")
    public void testChecksumMismatch(byte magic) {
        byte[] payload = generateData(64);
        byte[] block = createBlock(magic, payload);
        block[block.length - 1] ^= 0xFF;

        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(block),
                LZ4_FACTORY.fastDecompressor(),
                1024);

        ClientException ex = Assert.expectThrows(ClientException.class,
                () -> input.read(new byte[1], 0, 1));
        Assert.assertTrue(ex.getMessage().contains("checksum mismatch"));
    }

    @Test(dataProvider = "compressionAlgorithms")
    public void testUnexpectedEndOfStreamInBlock(byte magic) {
        byte[] payload = generateData(64);
        byte[] block = createBlock(magic, payload);
        byte[] truncated = Arrays.copyOf(block, 25); // header only, 0 bytes of payload

        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(truncated),
                LZ4_FACTORY.fastDecompressor(),
                1024);

        DataTransferException ex = Assert.expectThrows(DataTransferException.class, () -> input.read(new byte[1], 0, 1));
        Assert.assertEquals(ex.getMessage(), "Unexpected end of stream");
    }

    @Test(dataProvider = "compressionAlgorithms")
    public void testIncompleteReadInBlockPayload(byte magic) {
        byte[] payload = generateData(64);
        byte[] block = createBlock(magic, payload);
        byte[] truncated = Arrays.copyOf(block, block.length - 5);

        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(truncated),
                LZ4_FACTORY.fastDecompressor(),
                1024);

        DataTransferException ex = Assert.expectThrows(DataTransferException.class, () -> input.read(new byte[1], 0, 1));
        Assert.assertTrue(ex.getMessage().startsWith("Incomplete read:"));
    }

    @Test
    public void testConstructorAndReadBoundsValidation() throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        Assert.expectThrows(NullPointerException.class,
                () -> new CompressedBlockInputStream(null, LZ4_FACTORY.fastDecompressor(), 1024));
        Assert.expectThrows(NullPointerException.class,
                () -> new CompressedBlockInputStream(bais, null, 1024));
        Assert.expectThrows(IllegalArgumentException.class,
                () -> new CompressedBlockInputStream(bais, LZ4_FACTORY.fastDecompressor(), 0));
        Assert.expectThrows(IllegalArgumentException.class,
                () -> new CompressedBlockInputStream(bais, LZ4_FACTORY.fastDecompressor(), -1));
        Assert.expectThrows(IllegalArgumentException.class,
                () -> new CompressedBlockInputStream(
                        bais, LZ4_FACTORY.fastDecompressor(), 64, 8));
        Assert.expectThrows(IllegalArgumentException.class,
                () -> new CompressedBlockInputStream(
                        bais, LZ4_FACTORY.fastDecompressor(), 64,
                        CompressedBlockInputStream.MAX_COMPRESSED_BLOCK_SIZE + 1));

        try (CompressedBlockInputStream in = new CompressedBlockInputStream(bais, LZ4_FACTORY.fastDecompressor(), 64)) {
            byte[] buf = new byte[10];
            Assert.expectThrows(NullPointerException.class, () -> in.read(null, 0, 1));
            Assert.expectThrows(IndexOutOfBoundsException.class, () -> in.read(buf, -1, 1));
            Assert.expectThrows(IndexOutOfBoundsException.class, () -> in.read(buf, 0, -1));
            Assert.expectThrows(IndexOutOfBoundsException.class, () -> in.read(buf, 5, 6));
            Assert.assertEquals(in.read(buf, 0, 0), 0);
        }
    }

    @DataProvider(name = "invalidBlockHeaders")
    public static Object[][] invalidBlockHeaders() {
        return new Object[][] {
                { CompressedBlockInputStream.MAGIC_LZ4, 0, 64, "invalid compressed block size" },
                { CompressedBlockInputStream.MAGIC_LZ4, 8, 64, "invalid compressed block size" },
                { CompressedBlockInputStream.MAGIC_ZSTD_3, -10, 64, "invalid compressed block size" },
                { CompressedBlockInputStream.MAGIC_LZ4, 25, -1, "invalid uncompressed block size" },
                { CompressedBlockInputStream.MAGIC_LZ4,
                        CompressedBlockInputStream.MAX_COMPRESSED_BLOCK_SIZE + 1, 64,
                        "invalid compressed block size" },
                { CompressedBlockInputStream.MAGIC_LZ4, 25,
                        CompressedBlockInputStream.MAX_UNCOMPRESSED_BLOCK_SIZE + 1,
                        "invalid uncompressed block size" },
        };
    }

    @Test(dataProvider = "invalidBlockHeaders")
    public void testCorruptedHeaderSizes(byte magic, int compressedSizeWithHeader, int uncompressedSize,
                                         String expectedMessagePart) {
        byte[] header = new byte[CompressedBlockInputStream.HEADER_LENGTH];
        header[CompressedBlockInputStream.MAGIC_NUM_POS] = magic;
        CompressedBlockInputStream.setInt32(header, 17, compressedSizeWithHeader);
        CompressedBlockInputStream.setInt32(header, 21, uncompressedSize);

        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(header),
                LZ4_FACTORY.fastDecompressor(),
                1024);

        DataTransferException ex = Assert.expectThrows(DataTransferException.class,
                () -> input.read(new byte[1], 0, 1));
        Assert.assertTrue(ex.getMessage().contains(expectedMessagePart), "Unexpected message: " + ex.getMessage());
    }

    @Test
    public void testConfiguredBlockSizeLimit() {
        int maxBlockSize = 1024;
        byte[] header = new byte[CompressedBlockInputStream.HEADER_LENGTH];
        header[CompressedBlockInputStream.MAGIC_NUM_POS] = CompressedBlockInputStream.MAGIC_LZ4;
        CompressedBlockInputStream.setInt32(header, 17, maxBlockSize + 1);
        CompressedBlockInputStream.setInt32(header, 21, 64);
        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(header), LZ4_FACTORY.fastDecompressor(), 64, maxBlockSize);

        DataTransferException ex = Assert.expectThrows(DataTransferException.class,
                () -> input.read(new byte[1], 0, 1));

        Assert.assertTrue(ex.getMessage().contains("exceeds configured limit"), ex.getMessage());
    }

    @Test
    public void testUncompressedBlockSizeMismatch() {
        byte[] payload = generateData(32);
        // Header claims uncompressedSize is 64, while payload (and remaining) is 32
        byte[] block = createBlockWithCustomUncompressedSize(CompressedBlockInputStream.MAGIC_NONE, payload, 64);

        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(block),
                LZ4_FACTORY.fastDecompressor(),
                1024);

        DataTransferException ex = Assert.expectThrows(DataTransferException.class,
                () -> input.read(new byte[1], 0, 1));
        Assert.assertTrue(ex.getMessage().contains("uncompressed block size mismatch"),
                "Unexpected message: " + ex.getMessage());
    }

    @Test
    public void testZstdDecompressedSizeMismatch() {
        byte[] payload = generateData(32);
        byte[] compressedPayload = Zstd.compress(payload, 3);
        // Header claims uncompressedSize is 64, while ZSTD frame decompresses to 32 bytes
        byte[] block = createBlockWithCustomUncompressedSize(CompressedBlockInputStream.MAGIC_ZSTD_3, compressedPayload, 64);

        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(block),
                LZ4_FACTORY.fastDecompressor(),
                1024);

        DataTransferException ex = Assert.expectThrows(DataTransferException.class,
                () -> input.read(new byte[1], 0, 1));
        Assert.assertTrue(ex.getMessage().contains("ZSTD decompressed size mismatch"),
                "Unexpected message: " + ex.getMessage());
    }

    @Test
    public void testLz4CompressedSizeMismatch() {
        byte[] payload = generateData(32);
        byte[] compressedPayload = LZ4_FACTORY.fastCompressor().compress(payload);
        compressedPayload = Arrays.copyOf(compressedPayload, compressedPayload.length + 1);
        byte[] block = createBlockWithCustomUncompressedSize(
                CompressedBlockInputStream.MAGIC_LZ4, compressedPayload, payload.length);

        CompressedBlockInputStream input = new CompressedBlockInputStream(
                new ByteArrayInputStream(block),
                LZ4_FACTORY.fastDecompressor(),
                1024);

        DataTransferException ex = Assert.expectThrows(DataTransferException.class,
                () -> input.read(new byte[1], 0, 1));
        Assert.assertTrue(ex.getMessage().contains("LZ4 compressed size mismatch"),
                "Unexpected message: " + ex.getMessage());
    }

    private static InputStream streamThatThrows(EOFException eof, byte[] prefix) {
        return new InputStream() {
            private int position;

            @Override
            public int read() throws IOException {
                byte[] one = new byte[1];
                int n = read(one, 0, 1);
                return n < 0 ? -1 : one[0] & 0xFF;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (position >= prefix.length) {
                    throw eof;
                }
                int n = Math.min(len, prefix.length - position);
                System.arraycopy(prefix, position, b, off, n);
                position += n;
                return n;
            }
        };
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

        return createBlockWithCustomUncompressedSize(magic, compressedPayload, uncompressedSize);
    }

    private static byte[] createBlockWithCustomUncompressedSize(byte magic, byte[] compressedPayload, int uncompressedSize) {
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
