package com.clickhouse.data.stream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.net.URL;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class InputStreamImplTest {
    static class CustomReader {
        private final byte[] delimiters;

        CustomReader(byte... delimiters) {
            this.delimiters = delimiters;
        }

        int read(byte[] bytes, int position, int limit) {
            int count = 0;
            for (int i = position; i < limit; i++) {
                byte b = bytes[i];
                count++;
                for (byte d : delimiters) {
                    if (b == d) {
                        return count;
                    }
                }
            }
            return -1;
        }
    }

    protected byte[] toByteArray(int... bytes) {
        byte[] b = ClickHouseByteBuffer.EMPTY_BYTES;
        if (bytes != null) {
            b = new byte[bytes.length];
            for (int i = 0; i < b.length; i++) {
                b[i] = (byte) (0xFF & bytes[i]);
            }
        }
        return b;
    }

    ClickHouseInputStream generateInputStream(Boolean closed, int[] bytes) {
        if (closed || (bytes != null && bytes.length == 0)) {
            return new IterableByteArrayInputStream(Collections.emptyList(), null);
        } else if (bytes == null) {
            return new IterableByteArrayInputStream(null, null);
        } else {
            return new IterableByteArrayInputStream(Arrays.asList(new byte[][] { toByteArray(bytes) }), null);
        }
    }

    @DataProvider(name = "streamProvider")
    private Object[][] getInputStreamProvider() {
        return new Object[][] {
                new BiFunction[] {
                        (c, b) -> {
                            boolean closed = (boolean) c;
                            int[] bytes = (int[]) b;
                            IterableByteArrayInputStream in;
                            if (bytes == null) {
                                in = new IterableByteArrayInputStream(null, null);
                            } else if (bytes.length == 0) {
                                in = new IterableByteArrayInputStream(Collections.emptyList(), null);
                            } else {
                                in = new IterableByteArrayInputStream(
                                        Arrays.asList(new byte[][] { toByteArray(bytes) }), null);
                            }
                            if (closed) {
                                try {
                                    in.close();
                                } catch (IOException e) {
                                    // ignore
                                }
                            }
                            return in;
                        },
                }
        };
    }

    private File generateTempFile(int... bytes) {
        try {
            File f = Files.createTempFile("test_", "_input_stream").toFile();
            if (bytes != null && bytes.length > 0) {
                try (FileOutputStream out = new FileOutputStream(f)) {
                    for (int b : bytes) {
                        out.write(b);
                    }
                    out.flush();
                }
            }
            return f;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private URL generateTempUrl(int... bytes) {
        try {
            return generateTempFile(bytes).toURI().toURL();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DataProvider(name = "emptyInputStreamProvider")
    private Object[][] getEmptyInputStreams() {
        return new Object[][] {
                // empty input stream
                { EmptyInputStream.INSTANCE },
                { ClickHouseInputStream.empty() },
                // custom objects
                { ClickHouseInputStream.of((Object[]) null, Object.class, o -> new byte[] { 1, 2, 3 }, null) },
                { ClickHouseInputStream.of((Iterable<Object>) null, Object.class, o -> new byte[] { 1, 2, 3 }, null) },
                { ClickHouseInputStream.of(new Object[0], Object.class, o -> new byte[] { 1, 2, 3 }, null) },
                { ClickHouseInputStream.of(Collections.emptyList(), Object.class, o -> new byte[] { 1, 2, 3 }, null) },
                { ClickHouseInputStream.of(Arrays.asList(1, 2, 3, 4, 5), Integer.class, i -> new byte[0], null) },
                // bytes
                { ClickHouseInputStream.of(new byte[0][]) },
                { ClickHouseInputStream.of(new byte[0]) },
                { ClickHouseInputStream.of((byte[]) null) },
                { ClickHouseInputStream.of(new byte[0], null) },
                { ClickHouseInputStream.of(null, new byte[0]) },
                { ClickHouseInputStream.of(new byte[0], new byte[0]) },
                { ClickHouseInputStream.of((byte[]) null, (byte[]) null) },
                // byte buffer
                { ClickHouseInputStream.of(new ByteBuffer[0]) },
                { ClickHouseInputStream.of(ByteBuffer.wrap(new byte[0])) },
                { ClickHouseInputStream.of((ByteBuffer) null) },
                { ClickHouseInputStream.of(ByteBuffer.wrap(new byte[0]), null) },
                { ClickHouseInputStream.of(null, ByteBuffer.wrap(new byte[0])) },
                { ClickHouseInputStream.of(ByteBuffer.wrap(new byte[0]), ByteBuffer.wrap(new byte[0])) },
                { ClickHouseInputStream.of((ByteBuffer) null, (ByteBuffer) null) },
                // files
                { ClickHouseInputStream.of(new File[0]) },
                { ClickHouseInputStream.of(generateTempFile()) },
                { ClickHouseInputStream.of((File) null) },
                { ClickHouseInputStream.of(generateTempFile(), null) },
                { ClickHouseInputStream.of(null, generateTempFile()) },
                { ClickHouseInputStream.of(generateTempFile(), generateTempFile()) },
                { ClickHouseInputStream.of((File) null, (File) null) },
                // input streams
                { ClickHouseInputStream.of(new ByteArrayInputStream[0]) },
                { ClickHouseInputStream.of(new ByteArrayInputStream(new byte[0])) },
                { ClickHouseInputStream.of((ByteArrayInputStream) null) },
                { ClickHouseInputStream.of(new ByteArrayInputStream(new byte[0])) },
                { ClickHouseInputStream.of(null, new ByteArrayInputStream(new byte[0])) },
                { ClickHouseInputStream.of(new ByteArrayInputStream(new byte[0]),
                        new ByteArrayInputStream(new byte[0])) },
                { ClickHouseInputStream.of((ByteArrayInputStream) null, (ByteArrayInputStream) null) },
                { ClickHouseInputStream.wrap(new ByteArrayInputStream(new byte[] { 1, 2, 3 }), 2048, 0L, null) },
                // strings
                { ClickHouseInputStream.of(new String[0]) },
                { ClickHouseInputStream.of("") },
                { ClickHouseInputStream.of((String) null) },
                { ClickHouseInputStream.of("", null) },
                { ClickHouseInputStream.of(null, "") },
                { ClickHouseInputStream.of("", "") },
                { ClickHouseInputStream.of((String) null, (String) null) },
                // urls
                { ClickHouseInputStream.of(new URL[0]) },
                { ClickHouseInputStream.of(generateTempUrl()) },
                { ClickHouseInputStream.of((URL) null) },
                { ClickHouseInputStream.of(generateTempUrl(), null) },
                { ClickHouseInputStream.of(null, generateTempUrl()) },
                { ClickHouseInputStream.of(generateTempUrl(), generateTempUrl()) },
                { ClickHouseInputStream.of((URL) null, (URL) null) },
                // blocking and non-blocking
                { new BlockingInputStream(
                        new LinkedBlockingQueue<>(Collections.singletonList(ClickHouseByteBuffer.EMPTY_BUFFER)), 0,
                        null) },
                { new NonBlockingInputStream(
                        AdaptiveQueue.create(CapacityPolicy.linearDynamicCapacity(0, 0, 0),
                                ClickHouseByteBuffer.EMPTY_BUFFER),
                        0, null) }
        };
    }

    @DataProvider(name = "inputStreamProvider")
    private Object[][] getInputStreamsWithData() {
        return new Object[][] {
                // "efghip" -> 0x65 0x66 0x67 0x68 0x69 0x70

                // custom objects
                { ClickHouseInputStream.of(new BigInteger[] { BigInteger.ZERO, BigInteger.ONE }, BigInteger.class,
                        b -> b == BigInteger.ZERO ? new byte[] { 0x65, 0x66 } : new byte[] { 0x67, 0x68, 0x69, 0x70 },
                        null) },
                { ClickHouseInputStream.of(Collections.singletonList(new Object()), Object.class,
                        o -> new byte[] { 0x65, 0x66, 0x67, 0x68, 0x69, 0x70 },
                        null) },
                // bytes
                { ClickHouseInputStream
                        .of(new byte[][] { { 0x65, 0x66, 0x67, 0x68, 0x69, 0x70 } }) },
                { ClickHouseInputStream
                        .of(new byte[][] { { 0x65 }, { 0x66 }, { 0x67 }, { 0x68 }, { 0x69 }, { 0x70 } }) },
                { ClickHouseInputStream
                        .of(new byte[][] { { 0x65 }, null, { 0x66, 0x67, 0x68 }, new byte[0], { 0x69, 0x70 } }) },
                // byte buffer
                { ClickHouseInputStream
                        .of(new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 0x65, 0x66, 0x67, 0x68, 0x69, 0x70 }) }) },
                { ClickHouseInputStream
                        .of(new ByteBuffer[] { ByteBuffer
                                .wrap(new byte[] { 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x70, 0x71 }, 1, 6) }) },
                { ClickHouseInputStream
                        .of(new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 0x65 }),
                                ByteBuffer.wrap(new byte[] { 0x66 }), ByteBuffer.wrap(new byte[] { 0x67 }),
                                ByteBuffer.wrap(new byte[] { 0x68 }), ByteBuffer.wrap(new byte[] { 0x69 }),
                                ByteBuffer.wrap(new byte[] { 0x70 }) }) },
                { ClickHouseInputStream
                        .of(new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 0x64, 0x65, 0x66 }, 1, 1),
                                ByteBuffer.wrap(new byte[] { 0x64, 0x65, 0x66, 0x67 }, 2, 2),
                                ByteBuffer.wrap(new byte[] { 0x68, 0x69, 0x70, 0x71 }, 0, 3) }) },
                // files
                { ClickHouseInputStream.of(generateTempFile(0x65, 0x66, 0x67, 0x68, 0x69, 0x70)) },
                { ClickHouseInputStream.of(generateTempFile(0x65), generateTempFile(0x66), generateTempFile(0x67),
                        generateTempFile(0x68), generateTempFile(0x69), generateTempFile(0x70)) },
                { ClickHouseInputStream.of(generateTempFile(0x65), generateTempFile(0x66, 0x67),
                        generateTempFile(0x68, 0x69, 0x70)) },
                // input streams
                { ClickHouseInputStream
                        .of(new InputStream[] {
                                new ByteArrayInputStream(new byte[] { 0x65, 0x66, 0x67, 0x68, 0x69, 0x70 }) }) },
                { ClickHouseInputStream
                        .of(new InputStream[] { new ByteArrayInputStream(new byte[] { 0x65 }),
                                new ByteArrayInputStream(new byte[] { 0x66 }),
                                new ByteArrayInputStream(new byte[] { 0x67 }),
                                new ByteArrayInputStream(new byte[] { 0x68 }),
                                new ByteArrayInputStream(new byte[] { 0x69 }),
                                new ByteArrayInputStream(new byte[] { 0x70 }) }) },
                { ClickHouseInputStream
                        .of(new InputStream[] { new ByteArrayInputStream(new byte[] { 0x65 }), null,
                                new ByteArrayInputStream(new byte[] { 0x66, 0x67, 0x68 }), null,
                                new ByteArrayInputStream(new byte[] { 0x69, 0x70 }) }) },
                { ClickHouseInputStream.wrap(
                        new ByteArrayInputStream(new byte[] { 1, 2, 0x65, 0x66, 0x67, 0x68, 0x69, 0x70, 1, 2 }, 2, 6),
                        2048, 6L, null) },
                // strings
                { ClickHouseInputStream.of("efghip") },
                { ClickHouseInputStream.of("e", "fg", "hip") },
                { ClickHouseInputStream.of("", "efg", "h", "ip", "") },
                // urls
                { ClickHouseInputStream.of(generateTempUrl(0x65, 0x66, 0x67, 0x68, 0x69, 0x70)) },
                { ClickHouseInputStream.of(generateTempUrl(0x65), generateTempUrl(0x66), generateTempUrl(0x67),
                        generateTempUrl(0x68), generateTempUrl(0x69), generateTempUrl(0x70)) },
                { ClickHouseInputStream.of(generateTempUrl(0x65, 0x66, 0x67), generateTempUrl(0x68, 0x69),
                        generateTempUrl(0x70)) },
                // blocking and non-blocking
                { new BlockingInputStream(
                        new LinkedBlockingQueue<>(
                                Arrays.asList(ByteBuffer.wrap(new byte[] { 0x65, 0x66, 0x67, 0x68, 0x69, 0x70 }),
                                        ClickHouseByteBuffer.EMPTY_BUFFER)),
                        0, null) },
                { new BlockingInputStream(
                        new LinkedBlockingQueue<>(Arrays.asList(ByteBuffer.wrap(new byte[] { 0x65 }),
                                ByteBuffer.wrap(new byte[] { 0x66 }), ByteBuffer.wrap(new byte[] { 0x67 }),
                                ByteBuffer.wrap(new byte[] { 0x68 }), ByteBuffer.wrap(new byte[] { 0x69 }),
                                ByteBuffer.wrap(new byte[] { 0x70 }), ClickHouseByteBuffer.EMPTY_BUFFER)),
                        0, null) },
                { new BlockingInputStream(
                        new LinkedBlockingQueue<>(Arrays.asList(ByteBuffer.wrap(new byte[] { 0x65, 0x66 }),
                                ByteBuffer.wrap(new byte[] { 0x67 }),
                                ByteBuffer.wrap(new byte[] { 0x68, 0x69 }),
                                ByteBuffer.wrap(new byte[] { 0x70 }), ClickHouseByteBuffer.EMPTY_BUFFER)),
                        0, null) },
                { new NonBlockingInputStream(AdaptiveQueue.create(CapacityPolicy.linearDynamicCapacity(0, 0, 0),
                        Arrays.asList(ByteBuffer.wrap(new byte[] { 0x65, 0x66, 0x67, 0x68, 0x69, 0x70 }),
                                ClickHouseByteBuffer.EMPTY_BUFFER)),
                        0, null) },
                { new NonBlockingInputStream(
                        AdaptiveQueue.create(CapacityPolicy.linearDynamicCapacity(0, 0, 0),
                                Arrays.asList(ByteBuffer.wrap(new byte[] { 0x65 }),
                                        ByteBuffer.wrap(new byte[] { 0x66 }), ByteBuffer.wrap(new byte[] { 0x67 }),
                                        ByteBuffer.wrap(new byte[] { 0x68 }), ByteBuffer.wrap(new byte[] { 0x69 }),
                                        ByteBuffer.wrap(new byte[] { 0x70 }),
                                        ClickHouseByteBuffer.EMPTY_BUFFER)),
                        0, null) },
                { new NonBlockingInputStream(
                        AdaptiveQueue.create(CapacityPolicy.linearDynamicCapacity(0, 0, 0),
                                Arrays.asList(ByteBuffer.wrap(new byte[] { 0x65, 0x66 }),
                                        ByteBuffer.wrap(new byte[] { 0x67 }),
                                        ByteBuffer.wrap(new byte[] { 0x68, 0x69 }),
                                        ByteBuffer.wrap(new byte[] { 0x70 }), ClickHouseByteBuffer.EMPTY_BUFFER)),
                        0, null) },
        };

    }

    @DataProvider(name = "streamWithData")
    private Object[][] getInputStreamWithData() {
        return new Object[][] {
                new Object[] { new WrappedInputStream(null,
                        new ByteArrayInputStream(new byte[] { -1, 1, 2, 3, 4, 5, 6 }, 1, 5), 1, null) },
                new Object[] {
                        new IterableByteArrayInputStream(
                                Arrays.asList(new byte[] { 1 }, new byte[] { 2, 3, 4 }, new byte[] { 5 }),
                                null)
                },
                new Object[] {
                        new IterableByteArrayInputStream(
                                Arrays.asList(null, new byte[0], new byte[] { 1, 2, 3 }, new byte[0], null,
                                        new byte[] { 4, 5 }, null,
                                        new byte[0], null),
                                null)
                },
                new Object[] { new IterableByteBufferInputStream(
                        Arrays.asList(null, ByteBuffer.allocateDirect(0),
                                ByteBuffer.wrap(new byte[] { -1, 1, 2, 3, -4 }, 1, 3),
                                ByteBuffer.allocate(0), null, ByteBuffer.wrap(new byte[] { 4, 5 }), null,
                                ByteBuffer.allocate(0), null),
                        null) },
                new Object[] {
                        new RestrictedInputStream(null, new ByteArrayInputStream(new byte[] { 1, 2, 3, 4, 5 }), 2048,
                                5L, null) },
        };
    }

    @Test(dataProvider = "emptyInputStreamProvider", groups = { "unit" })
    public void testEmptyInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        Assert.assertEquals(in.read(), -1);
        Assert.assertEquals(in.read(new byte[0], 0, 0), 0);
        Assert.assertEquals(in.read(new byte[2], 1, 1), -1);
        Assert.assertEquals(in.read(new byte[0]), 0);
        Assert.assertEquals(in.read(new byte[1]), -1);
        Assert.assertEquals(in.skip(0L), 0L);
        Assert.assertEquals(in.skip(1L), 0L);
        Assert.assertEquals(in.skip(Long.MAX_VALUE), 0L);
        Assert.assertEquals(in.peek(), -1);
        Assert.assertEquals(in.available(), 0);
        Assert.assertEquals(in.peek(), -1);
        Assert.assertEquals(in.available(), 0);
        Assert.assertFalse(in.isClosed(), "Should be still openned");
        in.close();
        if (in == EmptyInputStream.INSTANCE) {
            Assert.assertFalse(in.isClosed(), "EmptyInputStream can never be closed");
        } else {
            Assert.assertTrue(in.isClosed(), "Should have been closed");
        }
    }

    @Test(dataProvider = "inputStreamProvider", groups = { "unit" })
    public void testReadFromInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        while (in.peek() != -1) {
            Assert.assertTrue(in.available() > 0, "Should have more to read");
            Assert.assertEquals(in.peek(), in.read());
        }
        Assert.assertEquals(in.available(), 0);
        Assert.assertEquals(in.read(), -1);
        Assert.assertFalse(in.isClosed(), "Should be still opening");
        in.close();
        Assert.assertTrue(in.isClosed(), "Should have been closed");
    }

    @Test(dataProvider = "inputStreamProvider", groups = { "unit" })
    public void testReadAllFromInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        byte[] bytes = new byte[8];
        Assert.assertEquals(in.read(bytes, 1, 6), 6);
        Assert.assertEquals(new String(bytes, 1, 6), "efghip");
        Assert.assertEquals(in.read(bytes), -1);
        Assert.assertFalse(in.isClosed(), "Should be still opening");
        in.close();
        Assert.assertTrue(in.isClosed(), "Should have been closed");
    }

    @Test(dataProvider = "inputStreamProvider", groups = { "unit" })
    public void testReadByteFromInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        StringBuilder builder = new StringBuilder();
        while (in.available() > 0) {
            builder.append((char) in.readByte());
        }
        Assert.assertEquals(builder.toString(), "efghip");
        Assert.assertFalse(in.isClosed(), "Should be still opening");
        Assert.assertThrows(EOFException.class, () -> in.readByte());
        Assert.assertTrue(in.isClosed(), "Should have been closed");
    }

    @Test(dataProvider = "inputStreamProvider", groups = { "unit" })
    public void testReadBytesFromInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        Assert.assertEquals(new String(in.readBytes(2)), "ef");
        Assert.assertEquals(new String(in.readBytes(1)), "g");
        Assert.assertEquals(new String(in.readBytes(3)), "hip");
        Assert.assertFalse(in.isClosed(), "Should be still opening");
        Assert.assertThrows(EOFException.class, () -> in.readBytes(1));
        Assert.assertTrue(in.isClosed(), "Should have been closed");
    }

    @Test(dataProvider = "inputStreamProvider", groups = { "unit" })
    public void testReadBytesAllFromInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        Assert.assertEquals(new String(in.readBytes(6)), "efghip");
        Assert.assertFalse(in.isClosed(), "Should be still opening");
        Assert.assertThrows(EOFException.class, () -> in.readBytes(1));
        Assert.assertTrue(in.isClosed(), "Should have been closed");
    }

    @Test(dataProvider = "inputStreamProvider", groups = { "unit" })
    public void testReadBufferFromInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        Assert.assertEquals(in.readBuffer(3).asUnicodeString(), "efg");
        Assert.assertEquals(in.readBuffer(2).asUnicodeString(), "hi");
        Assert.assertEquals(in.readBuffer(1).asUnicodeString(), "p");
        Assert.assertFalse(in.isClosed(), "Should be still opening");
        Assert.assertThrows(EOFException.class, () -> in.readBuffer(1));
        Assert.assertTrue(in.isClosed(), "Should have been closed");
    }

    @Test(dataProvider = "inputStreamProvider", groups = { "unit" })
    public void testReadBufferUntilFromInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        Assert.assertEquals(in.readBufferUntil(new byte[] { 'f', 'g' }).asUnicodeString(), "efg");
        Assert.assertEquals(in.readBufferUntil(new byte[] { 'i' }).asUnicodeString(), "hi");
        Assert.assertEquals(in.readBufferUntil(new byte[] { 'p' }).asUnicodeString(), "p");
        Assert.assertFalse(in.isClosed(), "Should be still opening");
        Assert.assertTrue(in.readBufferUntil(new byte[0]).isEmpty(), "Should got nothing");
        Assert.assertFalse(in.isClosed(), "Should be still opening");
        Assert.assertTrue(in.readBufferUntil(new byte[1]).isEmpty(), "Should got nothing");
        Assert.assertTrue(in.isClosed(), "Should have been closed");
    }

    @Test(dataProvider = "inputStreamProvider", groups = { "unit" })
    public void testReadCustomFromInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        Assert.assertEquals(in.readCustom((b, o, l) -> 0).asAsciiString(),
                "");
        Assert.assertEquals(in.readCustom((b, o, l) -> 1).asAsciiString(),
                "e");
        final AtomicInteger i = new AtomicInteger(2);
        Assert.assertEquals(
                in.readCustom((b, o, l) -> i.get() < (l - o) ? i.get() : i.getAndAdd(0 - l + o) * 0 - 1)
                        .asAsciiString(),
                "fg");
        i.set(3);
        Assert.assertEquals(
                in.readCustom((b, o, l) -> i.get() < (l - o) ? i.get() : i.getAndAdd(0 - l + o) * 0 - 1)
                        .asAsciiString(),
                "hip");
        Assert.assertTrue(in.isClosed(), "Should have been closed");
    }

    @Test(dataProvider = "inputStreamProvider", groups = { "unit" })
    public void testReadCustomAllFromInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        Assert.assertEquals(in.readCustom((b, o, l) -> -1).asAsciiString(), "efghip");
        Assert.assertTrue(in.isClosed(), "Should have been closed");
    }

    @Test(dataProvider = "inputStreamProvider", groups = { "unit" })
    public void testReadCustomOneAndRestFromInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        Assert.assertEquals(in.readCustom((b, o, l) -> 1).asAsciiString(), "e");
        Assert.assertEquals(in.readCustom((b, o, l) -> -1).asAsciiString(), "fghip");
        Assert.assertTrue(in.isClosed(), "Should have been closed");
    }

    @Test(dataProvider = "inputStreamProvider", groups = { "unit" })
    public void testReadCustomOneByOneFromInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        StringBuilder builder = new StringBuilder();
        while (in.available() > 0) {
            builder.append(in.readCustom((b, o, l) -> 1).asAsciiString());
        }
        Assert.assertEquals(builder.toString(), "efghip");
        Assert.assertEquals(in.readCustom((b, o, l) -> (int) System.currentTimeMillis() % 2).asAsciiString(), "");
        Assert.assertTrue(in.isClosed(), "Should have been closed");
    }

    @Test(dataProvider = "inputStreamProvider", groups = { "unit" })
    public void testReadPartsFromInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        byte[] bytes = new byte[8];
        Assert.assertEquals(in.read(bytes, 2, 4), 4);
        Assert.assertEquals(new String(bytes, 2, 4), "efgh");
        Assert.assertEquals(in.read(bytes, 6, 1), 1);
        Assert.assertEquals(bytes[6], 0x69);
        Assert.assertTrue(in.available() > 0, "Should have more to read");
        Assert.assertFalse(in.isClosed(), "Should be still opening");
        in.close();
        Assert.assertTrue(in.isClosed(), "Should have been closed");
    }

    @Test(dataProvider = "inputStreamProvider", groups = { "unit" })
    public void testPipeInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClickHouseOutputStream wrapper = ClickHouseOutputStream.of(out);
        Assert.assertEquals(in.pipe(wrapper), 6L);
        wrapper.flush();
        Assert.assertEquals(new String(out.toByteArray()), "efghip");
        Assert.assertTrue(in.isClosed(), "Should have been closed");
    }

    @Test(dataProvider = "emptyInputStreamProvider", groups = { "unit" })
    public void testReadByteFromEmptyInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        Assert.assertThrows(EOFException.class, () -> in.readByte());
        if (in == EmptyInputStream.INSTANCE) {
            Assert.assertFalse(in.isClosed(), "EmptyInputStream can never be closed");
        } else {
            Assert.assertTrue(in.isClosed(), "Should have been closed");
        }
    }

    @Test(dataProvider = "emptyInputStreamProvider", groups = { "unit" })
    public void testReadBytesFromEmptyInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        Assert.assertEquals(in.readBytes(-1), new byte[0]);
        Assert.assertEquals(in.readBytes(0), new byte[0]);
        Assert.assertThrows(EOFException.class, () -> in.readBytes(1));
        if (in == EmptyInputStream.INSTANCE) {
            Assert.assertFalse(in.isClosed(), "EmptyInputStream can never be closed");
        } else {
            Assert.assertTrue(in.isClosed(), "Should have been closed");
        }
    }

    @Test(dataProvider = "emptyInputStreamProvider", groups = { "unit" })
    public void testReadBufferFromEmptyInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        Assert.assertEquals(in.readBuffer(-1), ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(in.readBuffer(0), ClickHouseByteBuffer.newInstance());
        Assert.assertThrows(EOFException.class, () -> in.readBuffer(1));
        if (in == EmptyInputStream.INSTANCE) {
            Assert.assertFalse(in.isClosed(), "EmptyInputStream can never be closed");
        } else {
            Assert.assertTrue(in.isClosed(), "Should have been closed");
        }
    }

    @Test(dataProvider = "emptyInputStreamProvider", groups = { "unit" })
    public void testReadCustomFromEmptyInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        Assert.assertEquals(in.readCustom((b, o, l) -> (int) (System.currentTimeMillis() % 2)),
                ClickHouseByteBuffer.newInstance());
        if (in == EmptyInputStream.INSTANCE) {
            Assert.assertFalse(in.isClosed(), "EmptyInputStream can never be closed");
        } else {
            Assert.assertTrue(in.isClosed(), "Should have been closed");
        }
    }

    @Test(dataProvider = "emptyInputStreamProvider", groups = { "unit" })
    public void testPipeEmptyInputStream(ClickHouseInputStream in) throws IOException {
        Assert.assertFalse(in.isClosed(), "Should be openned for read by default");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Assert.assertEquals(in.pipe(ClickHouseOutputStream.of(out)), 0L);
        if (in == EmptyInputStream.INSTANCE) {
            Assert.assertFalse(in.isClosed(), "EmptyInputStream can never be closed");
        } else {
            Assert.assertTrue(in.isClosed(), "Should have been closed");
        }
    }

    @Test(dataProvider = "streamProvider", groups = { "unit" })
    public void testNullEmptyOrClosedInput(BiFunction<Boolean, int[], ClickHouseInputStream> newStreamFunc)
            throws IOException {
        Assert.assertThrows(IllegalArgumentException.class, () -> newStreamFunc.apply(false, null));
        ClickHouseInputStream empty = newStreamFunc.apply(false, new int[0]);
        Assert.assertEquals(empty.isClosed(), false);
        Assert.assertEquals(empty.available(), 0);
        Assert.assertEquals(empty.read(), -1);
        Assert.assertEquals(empty.read(), -1);
        Assert.assertEquals(empty.read(new byte[1]), -1);
        Assert.assertEquals(empty.read(new byte[1]), -1);
        Assert.assertEquals(empty.readBytes(0), new byte[0]);
        Assert.assertThrows(EOFException.class, () -> empty.readByte());
        Assert.assertEquals(empty.isClosed(), true);
        Assert.assertThrows(IOException.class, () -> empty.read());

        ClickHouseInputStream empty1 = newStreamFunc.apply(false, new int[0]);
        Assert.assertEquals(empty1.isClosed(), false);
        Assert.assertThrows(EOFException.class, () -> empty1.readBytes(1));
        Assert.assertEquals(empty1.isClosed(), true);
        Assert.assertThrows(IOException.class, () -> empty1.read());

        ClickHouseInputStream chIn = newStreamFunc.apply(true, new int[] { 123 });
        Assert.assertEquals(chIn.isClosed(), true);
        Assert.assertEquals(chIn.available(), 0);
        Assert.assertEquals(chIn.isClosed(), true);
        Assert.assertEquals(ClickHouseInputStream.of(chIn), chIn);
        Assert.assertEquals(chIn.readBytes(0), new byte[0]);
        Assert.assertThrows(IOException.class, () -> chIn.readBytes(1));
        Assert.assertThrows(IOException.class, () -> chIn.read());
        Assert.assertThrows(IOException.class, () -> chIn.readByte());
        Assert.assertEquals(chIn.read(new byte[0]), 0);
        chIn.close();
        Assert.assertEquals(chIn.isClosed(), true);
    }

    @Test(dataProvider = "streamWithData", groups = { "unit" })
    public void testReadCustom(ClickHouseInputStream input) throws IOException {
        try (ClickHouseInputStream in = input) {
            Assert.assertEquals(in.readCustom((bytes, position, limit) -> 0), ClickHouseByteBuffer.newInstance());
            Assert.assertEquals(in.readCustom((bytes, position, limit) -> 1).compact(),
                    ClickHouseByteBuffer.of(new byte[] { 1 }));
            Assert.assertEquals(in.readCustom((bytes, position, limit) -> {
                int count = 0;
                for (int i = position; i < limit; i++) {
                    count++;
                    if (bytes[i] == 4) {
                        return count;
                    }
                }
                return -1;
            }).compact(), ClickHouseByteBuffer.of(new byte[] { 2, 3, 4 }));
            Assert.assertEquals(in.readCustom((bytes, position, limit) -> -1).compact(),
                    ClickHouseByteBuffer.of(new byte[] { 5 }));
            Assert.assertTrue(in.isClosed(), "Input stream should have been closed");
            Assert.assertThrows(IOException.class, () -> in.readCustom((bytes, position, limit) -> 0));
        }
        Assert.assertThrows(IOException.class, () -> input.readCustom((bytes, position, limit) -> -1));
    }

    @Test(groups = { "unit" })
    public void testReadCustomArray() throws IOException {
        try (ClickHouseInputStream in = new IterableByteArrayInputStream(
                Arrays.asList(null, new byte[0], new byte[] { 1, 2, 3 }, new byte[0], null, new byte[] { 4, 5 }, null,
                        new byte[0], null),
                null)) {
            Assert.assertEquals(in.readCustom(null), ClickHouseByteBuffer.newInstance());
            Assert.assertFalse(in.isClosed(), "Stream should not be closed");
            Assert.assertEquals(in.readCustom(new CustomReader((byte) 5, (byte) 2)::read),
                    ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 0, 2));
            Assert.assertFalse(in.isClosed(), "Stream should not be closed");
            Assert.assertEquals(in.readCustom(new CustomReader((byte) 5)::read),
                    ClickHouseByteBuffer.of(new byte[] { 3, 4, 5 }));
            Assert.assertFalse(in.isClosed(), "Stream should not be closed");
            Assert.assertEquals(in.readCustom(new CustomReader((byte) 6)::read), ClickHouseByteBuffer.newInstance());
            Assert.assertTrue(in.isClosed(), "Stream should have been closed");
        }

        try (ClickHouseInputStream in = new IterableByteArrayInputStream(
                Arrays.asList(null, new byte[0], new byte[] { 1, 2, 3 }, new byte[0], null, new byte[] { 4, 5 }, null,
                        new byte[0], null),
                null)) {
            Assert.assertFalse(in.isClosed(), "Stream should not be closed");
            Assert.assertEquals(in.readCustom(new CustomReader((byte) 0)::read),
                    ClickHouseByteBuffer.of(new byte[] { 1, 2, 3, 4, 5 }));
            Assert.assertTrue(in.isClosed(), "Stream should have been closed");
            Assert.assertEquals(in.readCustom(null), ClickHouseByteBuffer.newInstance());
            Assert.assertThrows(IOException.class, () -> {
                in.readCustom(new CustomReader((byte) 1, (byte) 2)::read);
            });
        }

        byte[] bytes = "萌\\N懵\\t哒\t🤣。".getBytes(StandardCharsets.UTF_8);
        try (ClickHouseInputStream in = new IterableByteArrayInputStream(
                Arrays.asList(null, new byte[0], bytes, new byte[0], null,
                        "ab".getBytes(StandardCharsets.US_ASCII), null, new byte[0], null),
                null)) {
            Assert.assertFalse(in.isClosed(), "Stream should not be closed");
            Assert.assertEquals(in.readCustom(new CustomReader((byte) '\n', (byte) '\t')::read),
                    ClickHouseByteBuffer.of(bytes, 0, 14));
            Assert.assertFalse(in.isClosed(), "Stream should not be closed");
            Assert.assertEquals(in.readCustom(new CustomReader((byte) '\t', (byte) '\n')::read),
                    ClickHouseByteBuffer.of("🤣。ab".getBytes(StandardCharsets.UTF_8), 0, 9));
            Assert.assertTrue(in.isClosed(), "Stream should have been closed");
            Assert.assertThrows(IOException.class, () -> {
                in.readCustom((b, o, l) -> {
                    int count = 0;
                    for (int i = o; i < l; i++) {
                        byte v = b[i];
                        count++;
                        if (v == (byte) 1 || v == (byte) 2) {
                            return count;
                        }
                    }
                    return -1;
                });
            });
        }
    }

    @Test(groups = { "unit" })
    public void testReadAllBuffers() throws IOException {
        Random r = new Random(System.currentTimeMillis());
        int bufferSize = 32 + r.nextInt(256);
        byte[] bytes = new byte[2000 + r.nextInt(100000)];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }

        try (ClickHouseInputStream in = ClickHouseInputStream.of(new ByteArrayInputStream(bytes), bufferSize, null)) {
            int i = 0;
            for (ClickHouseByteBuffer buf : in) {
                Assert.assertFalse(buf.isEmpty());
                for (int j = buf.position(); j < buf.length(); j++) {
                    Assert.assertEquals(buf.array()[j], (byte) (i++));
                }
            }
            Assert.assertEquals(i, bytes.length);
        }

        List<ByteBuffer> buffers = new LinkedList<>();
        for (int i = 0; i < bytes.length;) {
            int size = 3 + r.nextInt(bufferSize * 2);
            ByteBuffer b = size % 7 == 0 ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
            b.put(bytes, i, Math.min(size, bytes.length - i));
            i += b.position();
            ((Buffer) b).limit(b.position());
            ((Buffer) b).rewind();
            buffers.add(b);
        }
        try (ClickHouseInputStream in = ClickHouseInputStream.of(buffers.toArray(new ByteBuffer[0]))) {
            int i = 0;
            for (ClickHouseByteBuffer buf : in) {
                Assert.assertFalse(buf.isEmpty());
                for (int j = buf.position(); j < buf.length(); j++) {
                    Assert.assertEquals(buf.array()[j], bytes[i++]);
                }
            }
            Assert.assertEquals(i, bytes.length);
        }
    }

    @Test(groups = { "unit" })
    public void testReadCustomBuffer() throws IOException {
        try (ClickHouseInputStream in = new IterableByteBufferInputStream(
                Arrays.asList(null, ByteBuffer.allocateDirect(0), ByteBuffer.wrap(new byte[] { -1, 1, 2, 3, -4 }, 1, 3),
                        ByteBuffer.allocate(0), null, ByteBuffer.wrap(new byte[] { 4, 5 }), null,
                        ByteBuffer.allocate(0), null),
                null)) {
            Assert.assertEquals(in.readCustom(null), ClickHouseByteBuffer.newInstance());
            Assert.assertFalse(in.isClosed(), "Stream should not be closed");
            Assert.assertEquals(in.readCustom(new CustomReader((byte) 5, (byte) 2)::read).compact(),
                    ClickHouseByteBuffer.of(new byte[] { 1, 2 }, 0, 2));
            Assert.assertFalse(in.isClosed(), "Stream should not be closed");
            Assert.assertEquals(in.readCustom(new CustomReader((byte) 5)::read),
                    ClickHouseByteBuffer.of(new byte[] { 3, 4, 5 }));
            Assert.assertFalse(in.isClosed(), "Stream should not be closed");
            Assert.assertEquals(in.readCustom(new CustomReader((byte) 6)::read), ClickHouseByteBuffer.newInstance());
            Assert.assertTrue(in.isClosed(), "Stream should have been closed");
        }

        try (ClickHouseInputStream in = new IterableByteBufferInputStream(
                Arrays.asList(null, ByteBuffer.allocateDirect(0), ByteBuffer.wrap(new byte[] { 1, 2, 3 }),
                        ByteBuffer.allocate(0), null, ByteBuffer.wrap(new byte[] { 4, 5 }), null,
                        ByteBuffer.allocate(0), null),
                null)) {
            Assert.assertFalse(in.isClosed(), "Stream should not be closed");
            Assert.assertEquals(in.readCustom(new CustomReader((byte) 0)::read),
                    ClickHouseByteBuffer.of(new byte[] { 1, 2, 3, 4, 5 }));
            Assert.assertTrue(in.isClosed(), "Stream should have been closed");
            Assert.assertEquals(in.readCustom(null), ClickHouseByteBuffer.newInstance());
            Assert.assertThrows(IOException.class, () -> in.readCustom(new CustomReader((byte) 1, (byte) 2)::read));
        }

        byte[] bytes = "萌\\N懵\\t哒\t🤣。".getBytes(StandardCharsets.UTF_8);
        try (ClickHouseInputStream in = new IterableByteBufferInputStream(
                Arrays.asList(null, ByteBuffer.allocate(0), ByteBuffer.wrap(bytes), ByteBuffer.allocateDirect(0), null,
                        ByteBuffer.wrap("ab".getBytes(StandardCharsets.US_ASCII)), null, ByteBuffer.allocateDirect(0),
                        null),
                null)) {
            Assert.assertFalse(in.isClosed(), "Stream should not be closed");
            Assert.assertEquals(in.readCustom(new CustomReader((byte) '\n', (byte) '\t')::read).compact(),
                    ClickHouseByteBuffer.of(bytes, 0, 14).compact());
            Assert.assertFalse(in.isClosed(), "Stream should not be closed");
            Assert.assertEquals(in.readCustom(new CustomReader((byte) '\t', (byte) '\n')::read),
                    ClickHouseByteBuffer.of("🤣。ab".getBytes(StandardCharsets.UTF_8), 0, 9));
            Assert.assertTrue(in.isClosed(), "Stream should have been closed");
            Assert.assertThrows(IOException.class, () -> in.readCustom(new CustomReader((byte) 1, (byte) 2)::read));
        }
    }

    @Test(groups = { "unit" })
    public void testWeirdStuff() throws IOException {
        Assert.assertEquals(new byte[0], ClickHouseByteBuffer.EMPTY_BYTES);
        int i = 1756438978;
        // BinaryStreamUtils.writeVarInt(null, i);
    }

    @Test(groups = { "unit" })
    public void testRestrictedInputStream() throws IOException {
        ByteArrayInputStream bytes = new ByteArrayInputStream(new byte[] { 1, 2, 3, 4, 5, 6 });
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseInputStream.wrap(bytes, 0, -1, null));

        ClickHouseInputStream in = ClickHouseInputStream.wrap(bytes, 0, 0, null);
        Assert.assertTrue(in instanceof RestrictedInputStream);
        Assert.assertFalse(in.isClosed());
        Assert.assertEquals(in.available(), 0);
        Assert.assertEquals(((RestrictedInputStream) in).getRemaining(), 0);

        in = ClickHouseInputStream.wrap(bytes, 0, 1, null);
        Assert.assertEquals(in.available(), 1);
        Assert.assertEquals(in.read(), 1);
        Assert.assertEquals(in.available(), 0);
        Assert.assertEquals(in.read(), -1);

        in = ClickHouseInputStream.wrap(bytes, 0, 3, null);
        Assert.assertEquals(in.available(), 3);
        Assert.assertEquals(in.read(), 2);
        Assert.assertEquals(in.available(), 2);
        Assert.assertEquals(in.read(), 3);
        Assert.assertEquals(in.available(), 1);
        Assert.assertEquals(in.read(), 4);
        Assert.assertEquals(in.available(), 0);
        Assert.assertEquals(in.read(), -1);

        in = ClickHouseInputStream.wrap(bytes, 0, 3, null);
        Assert.assertEquals(in.available(), 2);
        Assert.assertEquals(in.read(), 5);
        Assert.assertEquals(in.available(), 1);
        Assert.assertEquals(in.read(), 6);
        Assert.assertEquals(in.available(), 0);
        Assert.assertEquals(in.read(), -1);
        Assert.assertEquals(((RestrictedInputStream) in).getRemaining(), 1);
    }
}
