package com.clickhouse.client.api.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.clickhouse.client.api.DataTransferException;
import net.jpountz.lz4.LZ4Factory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CompressedBlockOutputStreamTest {

    private static final LZ4Factory LZ4_FASTEST = LZ4Factory.fastestInstance();
    private static final LZ4Factory LZ4_JAVA = LZ4Factory.fastestJavaInstance();

    private static final int BUFFER_SIZE = CompressedBlockOutputStream.UNCOMPRESSED_BUFF_SIZE;

    @DataProvider(name = "streamsAndPayloads")
    public static Object[][] streamsAndPayloads() {
        Function<OutputStream, CompressedBlockOutputStream> lz4Fastest =
                out -> new CompressedBlockOutputStream.LZ4OutputStream(out, LZ4_FASTEST.fastCompressor(), BUFFER_SIZE);
        Function<OutputStream, CompressedBlockOutputStream> lz4Java =
                out -> new CompressedBlockOutputStream.LZ4OutputStream(out, LZ4_JAVA.fastCompressor(), BUFFER_SIZE);
        Function<OutputStream, CompressedBlockOutputStream> zstd =
                out -> new CompressedBlockOutputStream.ZSTDOutputStream(out, BUFFER_SIZE);

        int[] sizes = new int[] {
                1,
                BUFFER_SIZE - 1,
                BUFFER_SIZE,
                BUFFER_SIZE + 1,
                3 * BUFFER_SIZE,
                3 * BUFFER_SIZE + 12345
        };

        Object[][] codecs = new Object[][] {
                { "LZ4-fastest", lz4Fastest },
                { "LZ4-java", lz4Java },
                { "ZSTD", zstd }
        };

        Object[][] rows = new Object[codecs.length * (sizes.length + 2)][];
        int idx = 0;
        for (Object[] codec : codecs) {
            String name = (String) codec[0];
            @SuppressWarnings("unchecked")
            Function<OutputStream, CompressedBlockOutputStream> factory =
                    (Function<OutputStream, CompressedBlockOutputStream>) codec[1];
            for (int size : sizes) {
                rows[idx++] = new Object[] { name, "incompressible-" + size, factory, incompressibleData(size) };
            }
            rows[idx++] = new Object[] { name, "compressible-full", factory, compressibleData(BUFFER_SIZE) };
            rows[idx++] = new Object[] { name, "mixed-blocks", factory, mixedBlocksData(BUFFER_SIZE) };
        }
        return rows;
    }

    @Test(dataProvider = "streamsAndPayloads")
    public void testFullBlockRoundTrip(String method, String dataKind,
                                       Function<OutputStream, CompressedBlockOutputStream> streamFactory,
                                       byte[] payload) throws IOException {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (CompressedBlockOutputStream out = streamFactory.apply(compressed)) {
            out.write(payload);
        }

        Assert.assertEquals(decompressAll(compressed.toByteArray(), BUFFER_SIZE), payload,
                method + " " + dataKind + " round trip");
    }

    @DataProvider(name = "smallBufferSizes")
    public static Object[][] smallBufferSizes() {
        BiFunction<OutputStream, Integer, CompressedBlockOutputStream> lz4 =
                (out, size) -> new CompressedBlockOutputStream.LZ4OutputStream(out, LZ4_FASTEST.fastCompressor(), size);
        BiFunction<OutputStream, Integer, CompressedBlockOutputStream> zstd =
                CompressedBlockOutputStream.ZSTDOutputStream::new;

        int[] bufferSizes = new int[] { 1, 16, 64, 1000 };
        Object[][] rows = new Object[bufferSizes.length * 2][];
        int idx = 0;
        for (int bufSize : bufferSizes) {
            rows[idx++] = new Object[] { "LZ4", lz4, bufSize };
            rows[idx++] = new Object[] { "ZSTD", zstd, bufSize };
        }
        return rows;
    }

    @Test(dataProvider = "smallBufferSizes")
    public void testSmallBufferSizesWithIncompressibleData(
            String method,
            BiFunction<OutputStream, Integer, CompressedBlockOutputStream> streamFactory,
            int bufSize) throws IOException {
        byte[] payload = incompressibleData(bufSize * 4 + 7);
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (CompressedBlockOutputStream out = streamFactory.apply(compressed, bufSize)) {
            out.write(payload);
        }

        Assert.assertEquals(decompressAll(compressed.toByteArray(), Math.max(16, bufSize)), payload,
                method + " bufferSize=" + bufSize);
    }

    @DataProvider(name = "codecFactories")
    public static Object[][] codecFactories() {
        BiFunction<OutputStream, Integer, CompressedBlockOutputStream> lz4 =
                (out, size) -> new CompressedBlockOutputStream.LZ4OutputStream(out, LZ4_FASTEST.fastCompressor(), size);
        BiFunction<OutputStream, Integer, CompressedBlockOutputStream> zstd =
                CompressedBlockOutputStream.ZSTDOutputStream::new;
        return new Object[][] {
                { "LZ4", lz4 },
                { "ZSTD", zstd }
        };
    }

    @Test(dataProvider = "codecFactories")
    public void testVariedWritePatternsAndFlushes(
            String method,
            BiFunction<OutputStream, Integer, CompressedBlockOutputStream> streamFactory) throws IOException {
        int bufSize = 256;
        byte[] payload = incompressibleData(bufSize * 5 + 37);
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();

        try (CompressedBlockOutputStream out = streamFactory.apply(compressed, bufSize)) {
            int pos = 0;
            // 1. Single-byte writes across the first block boundary
            for (int i = 0; i < bufSize + 10; i++) {
                out.write(payload[pos++] & 0xFF);
            }
            // 2. 1-byte array write
            out.write(new byte[] { payload[pos++] });
            // 3. Explicit flush of partial incompressible block
            out.flush();
            // 4. Empty write (no-op) and redundant flush on empty buffer (no-op)
            out.write(payload, pos, 0);
            out.flush();
            // 5. Chunked write with non-zero offset straddling buffer boundary
            int chunk = bufSize + 50;
            out.write(payload, pos, chunk);
            pos += chunk;
            // 6. Write remaining bytes (larger than bufferSize) at once
            out.write(payload, pos, payload.length - pos);
        }

        Assert.assertEquals(decompressAll(compressed.toByteArray(), 128), payload, method + " varied writes");
    }

    @Test
    public void testConstructorValidation() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        Assert.expectThrows(NullPointerException.class,
                () -> new CompressedBlockOutputStream.LZ4OutputStream(null, LZ4_FASTEST.fastCompressor(), 1024));
        Assert.expectThrows(NullPointerException.class,
                () -> new CompressedBlockOutputStream.LZ4OutputStream(baos, null, 1024));
        Assert.expectThrows(IllegalArgumentException.class,
                () -> new CompressedBlockOutputStream.LZ4OutputStream(baos, LZ4_FASTEST.fastCompressor(), 0));
        Assert.expectThrows(IllegalArgumentException.class,
                () -> new CompressedBlockOutputStream.LZ4OutputStream(baos, LZ4_FASTEST.fastCompressor(), -1));
        Assert.expectThrows(IllegalArgumentException.class,
                () -> new CompressedBlockOutputStream.LZ4OutputStream(
                        baos, LZ4_FASTEST.fastCompressor(),
                        CompressedBlockInputStream.MAX_UNCOMPRESSED_BLOCK_SIZE + 1));

        Assert.expectThrows(NullPointerException.class,
                () -> new CompressedBlockOutputStream.ZSTDOutputStream(null, 1024));
        Assert.expectThrows(IllegalArgumentException.class,
                () -> new CompressedBlockOutputStream.ZSTDOutputStream(baos, 0));
        Assert.expectThrows(IllegalArgumentException.class,
                () -> new CompressedBlockOutputStream.ZSTDOutputStream(baos, -1));
        Assert.expectThrows(IllegalArgumentException.class,
                () -> new CompressedBlockOutputStream.ZSTDOutputStream(baos, Integer.MAX_VALUE));
        Assert.expectThrows(IllegalArgumentException.class,
                () -> new CompressedBlockOutputStream.ZSTDOutputStream(
                        baos, CompressedBlockInputStream.MAX_UNCOMPRESSED_BLOCK_SIZE + 1));
    }

    @Test(dataProvider = "codecFactories")
    public void testWriteBoundsValidation(
            String method,
            BiFunction<OutputStream, Integer, CompressedBlockOutputStream> streamFactory) throws IOException {
        try (CompressedBlockOutputStream out = streamFactory.apply(new ByteArrayOutputStream(), 64)) {
            byte[] buf = new byte[10];
            Assert.expectThrows(NullPointerException.class, () -> out.write(null, 0, 1));
            Assert.expectThrows(IndexOutOfBoundsException.class, () -> out.write(buf, -1, 1));
            Assert.expectThrows(IndexOutOfBoundsException.class, () -> out.write(buf, 0, -1));
            Assert.expectThrows(IndexOutOfBoundsException.class, () -> out.write(buf, 5, 6));
        }
    }

    @Test(dataProvider = "codecFactories")
    public void testLifecycleAndCloseGuarantees(
            String method,
            BiFunction<OutputStream, Integer, CompressedBlockOutputStream> streamFactory) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CompressedBlockOutputStream out = streamFactory.apply(baos, 64);
        out.write(new byte[] { 1, 2, 3 });
        out.close();
        // Idempotent close
        out.close();

        Assert.expectThrows(IOException.class, () -> out.write(1));
        Assert.expectThrows(IOException.class, () -> out.write(new byte[] { 1, 2 }, 0, 2));
        Assert.expectThrows(IOException.class, out::flush);

        // Verify underlying stream is closed even when flush throws
        AtomicBoolean innerClosed = new AtomicBoolean(false);
        OutputStream failingOut = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("write failed");
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                throw new IOException("write failed");
            }

            @Override
            public void close() throws IOException {
                innerClosed.set(true);
                throw new IOException("close failed");
            }
        };

        CompressedBlockOutputStream failingStream = streamFactory.apply(failingOut, 64);
        failingStream.write(1);
        IOException failure = Assert.expectThrows(IOException.class, failingStream::close);
        Assert.assertEquals(failure.getMessage(), "write failed");
        Assert.assertEquals(failure.getSuppressed().length, 1);
        Assert.assertEquals(failure.getSuppressed()[0].getMessage(), "close failed");
        Assert.assertTrue(innerClosed.get(), "Underlying stream should be closed even if flush fails");
    }

    @Test(dataProvider = "codecFactories")
    public void testCloseDoesNotSuppressFailureOnItself(
            String method,
            BiFunction<OutputStream, Integer, CompressedBlockOutputStream> streamFactory) throws IOException {
        IOException sharedFailure = new IOException("shared failure");
        OutputStream failingOut = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw sharedFailure;
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                throw sharedFailure;
            }

            @Override
            public void close() throws IOException {
                throw sharedFailure;
            }
        };
        CompressedBlockOutputStream stream = streamFactory.apply(failingOut, 64);
        stream.write(1);

        IOException thrown = Assert.expectThrows(IOException.class, stream::close);

        Assert.assertSame(thrown, sharedFailure, method);
        Assert.assertEquals(thrown.getSuppressed().length, 0, method);
    }

    @Test
    public void testInvalidCompressedSizeFromCompressorThrowsDataTransferException() throws IOException {
        CompressedBlockOutputStream bogusStream = new CompressedBlockOutputStream(new ByteArrayOutputStream(), 64, 64) {
            @Override
            protected byte getMagicNumber() {
                return CompressedBlockInputStream.MAGIC_LZ4;
            }

            @Override
            protected int compressData(ByteBuffer inBuffer, int uncompressedLen, ByteBuffer compressedBuffer,
                                       ByteBuffer buffer) {
                return 0;
            }
        };
        bogusStream.write(1);
        DataTransferException ex = Assert.expectThrows(DataTransferException.class, bogusStream::flush);
        Assert.assertTrue(ex.getMessage().contains("Invalid compressed block size"));
    }

    private static byte[] decompressAll(byte[] compressedBytes, int bufferSize) throws IOException {
        try (CompressedBlockInputStream in = new CompressedBlockInputStream(
                new ByteArrayInputStream(compressedBytes), LZ4_FASTEST.fastDecompressor(), bufferSize)) {
            ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) != -1) {
                decompressed.write(buf, 0, n);
            }
            return decompressed.toByteArray();
        }
    }

    private static byte[] incompressibleData(int length) {
        byte[] data = new byte[length];
        new Random(42).nextBytes(data);
        return data;
    }

    private static byte[] compressibleData(int length) {
        byte[] pattern = "ClickHouse compressed block test line. ".getBytes(StandardCharsets.US_ASCII);
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = pattern[i % pattern.length];
        }
        return data;
    }

    private static byte[] mixedBlocksData(int blockSize) {
        byte[] comp1 = compressibleData(blockSize);
        byte[] incomp = incompressibleData(blockSize);
        byte[] comp2 = compressibleData(blockSize / 2 + 13);
        byte[] combined = new byte[comp1.length + incomp.length + comp2.length];
        System.arraycopy(comp1, 0, combined, 0, comp1.length);
        System.arraycopy(incomp, 0, combined, comp1.length, incomp.length);
        System.arraycopy(comp2, 0, combined, comp1.length + incomp.length, comp2.length);
        return combined;
    }
}
