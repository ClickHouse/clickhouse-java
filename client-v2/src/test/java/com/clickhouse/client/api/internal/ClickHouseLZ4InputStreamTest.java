package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.data.ClickHouseCityHash;
import com.github.luben.zstd.Zstd;
import net.jpountz.lz4.LZ4Factory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ClickHouseLZ4InputStreamTest {

    private static final int BUFFER_SIZE = 8192;

    @Test(groups = {"unit"}, dataProvider = "compressionMethodProvider")
    public void testReadsBlockOfEveryCompressionMethod(byte method) throws IOException {
        byte[] payload = payload();

        Assert.assertEquals(readFully(frame(method, payload), BUFFER_SIZE), payload);
    }

    @Test(groups = {"unit"}, dataProvider = "compressionMethodProvider")
    public void testReadsBlockLargerThanInitialBuffer(byte method) throws IOException {
        byte[] payload = payload();

        Assert.assertEquals(readFully(frame(method, payload), 16), payload);
    }

    @DataProvider(name = "compressionMethodProvider")
    public Object[][] compressionMethodProvider() {
        return new Object[][]{
                {ClickHouseLZ4InputStream.MAGIC},
                {ClickHouseLZ4InputStream.MAGIC_ZSTD},
                {ClickHouseLZ4InputStream.MAGIC_NONE},
        };
    }

    @Test(groups = {"unit"})
    public void testReadsConsecutiveBlocksOfDifferentMethods() throws IOException {
        byte[] first = "first block\n".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second block\n".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(frame(ClickHouseLZ4InputStream.MAGIC_ZSTD, first));
        stream.write(frame(ClickHouseLZ4InputStream.MAGIC, second));

        byte[] expected = new byte[first.length + second.length];
        System.arraycopy(first, 0, expected, 0, first.length);
        System.arraycopy(second, 0, expected, first.length, second.length);
        Assert.assertEquals(readFully(stream.toByteArray(), BUFFER_SIZE), expected);
    }

    @Test(groups = {"unit"})
    public void testRejectsUnknownCompressionMethod() {
        byte[] frame = frame(ClickHouseLZ4InputStream.MAGIC_ZSTD, payload());
        frame[16] = (byte) 0x42;

        ClientException e = Assert.expectThrows(ClientException.class, () -> readFully(frame, BUFFER_SIZE));
        Assert.assertTrue(e.getMessage().contains("Invalid compression method byte"), e.getMessage());
    }

    @Test(groups = {"unit"})
    public void testRejectsBlockOfImpossibleSize() {
        byte[] frame = frame(ClickHouseLZ4InputStream.MAGIC_ZSTD, payload());
        ClickHouseLZ4InputStream.setInt32(frame, 17, 4);

        ClientException e = Assert.expectThrows(ClientException.class, () -> readFully(frame, BUFFER_SIZE));
        Assert.assertTrue(e.getMessage().contains("Corrupted stream"), e.getMessage());
    }

    @Test(groups = {"unit"})
    public void testKeepsUnframedBodyReadableThroughHeaderBuffer() throws IOException {
        byte[] body = "Code: 62. DB::Exception: Syntax error: failed at position 1\n"
                .getBytes(StandardCharsets.UTF_8);

        try (ClickHouseLZ4InputStream in = new ClickHouseLZ4InputStream(new ByteArrayInputStream(body),
                LZ4Factory.fastestInstance().fastDecompressor(), BUFFER_SIZE)) {
            Assert.expectThrows(ClientException.class, () -> in.read(new byte[64]));
            Assert.assertEquals(in.getHeaderBuffer(),
                    Arrays.copyOf(body, ClickHouseLZ4InputStream.HEADER_LENGTH));
        }
    }

    @Test(groups = {"unit"})
    public void testRejectsCorruptedBlock() {
        byte[] frame = frame(ClickHouseLZ4InputStream.MAGIC_ZSTD, payload());
        frame[frame.length - 1] ^= 0xFF;

        ClientException e = Assert.expectThrows(ClientException.class, () -> readFully(frame, BUFFER_SIZE));
        Assert.assertTrue(e.getMessage().contains("checksum mismatch"), e.getMessage());
    }

    private static byte[] payload() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append(i).append('\t').append("value-").append(i).append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] readFully(byte[] frames, int bufferSize) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (InputStream in = new ClickHouseLZ4InputStream(new ByteArrayInputStream(frames),
                LZ4Factory.fastestInstance().fastDecompressor(), bufferSize)) {
            byte[] chunk = new byte[64];
            int read;
            while ((read = in.read(chunk)) != -1) {
                out.write(chunk, 0, read);
            }
        }
        return out.toByteArray();
    }

    /**
     * Builds a block the way the server frames the output of the HTTP {@code compress=1}
     * interface: 16 bytes of CityHash128 checksum over the block, then a 9 byte header holding the
     * compression method, the compressed size (header included) and the uncompressed size, then
     * the compressed data.
     */
    private static byte[] frame(byte method, byte[] data) {
        byte[] compressed;
        switch (method) {
            case ClickHouseLZ4InputStream.MAGIC:
                byte[] lz4 = new byte[LZ4Factory.fastestInstance().fastCompressor().maxCompressedLength(data.length)];
                int lz4Length = LZ4Factory.fastestInstance().fastCompressor()
                        .compress(data, 0, data.length, lz4, 0, lz4.length);
                compressed = new byte[lz4Length];
                System.arraycopy(lz4, 0, compressed, 0, lz4Length);
                break;
            case ClickHouseLZ4InputStream.MAGIC_ZSTD:
                compressed = Zstd.compress(data, 3);
                break;
            default:
                compressed = data;
                break;
        }

        byte[] block = new byte[9 + compressed.length];
        block[0] = method;
        ClickHouseLZ4InputStream.setInt32(block, 1, block.length);
        ClickHouseLZ4InputStream.setInt32(block, 5, data.length);
        System.arraycopy(compressed, 0, block, 9, compressed.length);

        long[] checksum = ClickHouseCityHash.cityHash128(block, 0, block.length);
        byte[] frame = new byte[16 + block.length];
        setInt64(frame, 0, checksum[0]);
        setInt64(frame, 8, checksum[1]);
        System.arraycopy(block, 0, frame, 16, block.length);
        return frame;
    }

    private static void setInt64(byte[] bytes, int offset, long value) {
        for (int i = 0; i < 8; i++) {
            bytes[offset + i] = (byte) (0xFF & (value >> (8 * i)));
        }
    }
}
