package com.clickhouse.client.stream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.clickhouse.client.ClickHouseByteBuffer;

import org.testng.Assert;
import org.testng.annotations.Test;

public class NonBlockingPipedOutputStreamTest {
    @Test(groups = { "unit" })
    public void testRead() throws Exception {
        NonBlockingPipedOutputStream stream = new NonBlockingPipedOutputStream(4, 3, 1, CapacityPolicy.fixedCapacity(3),
                null);
        Assert.assertEquals(stream.queue.size(), 0);
        try (InputStream in = stream.getInputStream()) {
            in.read();
            Assert.fail("Read should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Read timed out") == 0);
        }

        stream.queue.add(new byte[] { (byte) 3 });
        Assert.assertEquals(stream.queue.size(), 1);
        try (InputStream in = stream.getInputStream()) {
            Assert.assertEquals(in.read(), 3);
            in.read();
            Assert.fail("Read should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Read timed out") == 0);
        }

        stream.queue.add(new byte[] { (byte) 3, (byte) 4 });
        Assert.assertEquals(stream.queue.size(), 1);
        try (InputStream in = stream.getInputStream()) {
            Assert.assertEquals(in.read(), 3);
            Assert.assertEquals(in.read(), 4);
            in.read();
            Assert.fail("Read should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Read timed out") == 0);
        }

        stream.queue.clear();
        stream.queue.add(ClickHouseByteBuffer.EMPTY_BYTES);
        Assert.assertEquals(stream.queue.size(), 1);
        try (InputStream in = stream.getInputStream()) {
            Assert.assertEquals(in.read(), -1);
        }

        stream.queue.add(new byte[] { (byte) 3, (byte) 4 });
        // stream.queue.put(buf);
        stream.queue.add(ClickHouseByteBuffer.EMPTY_BYTES);
        Assert.assertEquals(stream.queue.size(), 2);
        try (InputStream in = stream.getInputStream()) {
            Assert.assertEquals(in.read(), 3);
            Assert.assertEquals(in.read(), 4);
            Assert.assertEquals(in.read(), -1);
        }

        try (InputStream in = stream.getInputStream()) {
            in.close();
            in.read();
            Assert.fail("Read should fail");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("closed") > 0);
        }
    }

    @Test(groups = { "unit" })
    public void testReadBytes() throws Exception {
        NonBlockingPipedOutputStream stream = new NonBlockingPipedOutputStream(4, 3, 1, CapacityPolicy.fixedCapacity(3),
                null);
        Assert.assertEquals(stream.queue.size(), 0);
        byte[] bytes = new byte[3];
        try (InputStream in = stream.getInputStream()) {
            in.read(bytes);
            Assert.fail("Read should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Read timed out") == 0);
        }
        try (InputStream in = stream.getInputStream()) {
            in.read(bytes, 0, 1);
            Assert.fail("Read should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Read timed out") == 0);
        }

        stream.queue.add(new byte[] { (byte) 3, (byte) 4 });
        Assert.assertEquals(stream.queue.size(), 1);
        try (InputStream in = stream.getInputStream()) {
            in.read(bytes);
            Assert.fail("Read should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Read timed out") == 0);
        }
        stream.queue.add(new byte[] { (byte) 3, (byte) 4 });
        Assert.assertEquals(stream.queue.size(), 1);
        try (InputStream in = stream.getInputStream()) {
            Assert.assertEquals(in.read(bytes, 0, 2), 2);
            in.read(bytes, 0, 1);
            Assert.fail("Read should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Read timed out") == 0);
        }
        stream.queue.add(new byte[] { (byte) 3, (byte) 4 });
        Assert.assertEquals(stream.queue.size(), 1);
        try (InputStream in = stream.getInputStream()) {
            in.read(bytes, 0, 3);
            Assert.fail("Read should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Read timed out") == 0);
        }

        stream.queue.add(new byte[] { (byte) 3, (byte) 4 });
        stream.queue.add(ClickHouseByteBuffer.EMPTY_BYTES);
        Assert.assertEquals(stream.queue.size(), 2);
        try (InputStream in = stream.getInputStream()) {
            Assert.assertEquals(in.read(bytes, 0, 3), 2);
            Assert.assertEquals(in.read(bytes, 0, 1), -1);
            Assert.assertEquals(in.read(bytes, 0, 2), -1);
        }

        try (InputStream in = stream.getInputStream()) {
            in.close();
            in.read(bytes, 0, 3);
            Assert.fail("Read should fail");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("closed") > 0);
        }
    }

    @Test(groups = { "unit" })
    public void testWrite() throws Exception {
        NonBlockingPipedOutputStream stream = new NonBlockingPipedOutputStream(2, 3, 2, CapacityPolicy.fixedCapacity(3),
                null);
        Assert.assertEquals(stream.queue.size(), 0);
        try (OutputStream out = stream) {
            out.write(5);
            Assert.assertEquals(stream.queue.size(), 0);
            out.write(6);
            Assert.assertEquals(stream.queue.size(), 1);
            out.write(7);
            Assert.assertEquals(stream.queue.size(), 1);
            out.flush();
            Assert.assertEquals(stream.queue.size(), 2);
            Assert.assertEquals(stream.queue.poll(), new byte[] { (byte) 5, (byte) 6 });
            Assert.assertEquals(stream.queue.poll(), new byte[] { (byte) 7 });
        }

        stream = new NonBlockingPipedOutputStream(1, 1, 2, CapacityPolicy.fixedCapacity(1), null);
        Assert.assertEquals(stream.queue.size(), 0);
        try (OutputStream out = stream) {
            out.write(5);
            Assert.assertEquals(stream.queue.size(), 1);
            out.write(6);
            Assert.fail("Write should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Write timed out") == 0);
        }

        Assert.assertEquals(stream.queue.size(), 2);
        stream.queue.clear();
        Assert.assertEquals(stream.queue.size(), 0);
        try (OutputStream out = stream) {
            out.close();
            out.write(1);
            Assert.fail("Write should fail");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("closed") > 0);
        }
    }

    @Test(groups = { "unit" })
    public void testWriteBytes() throws Exception {
        NonBlockingPipedOutputStream stream = new NonBlockingPipedOutputStream(2, 3, 2, CapacityPolicy.fixedCapacity(3),
                null);
        Assert.assertEquals(stream.queue.size(), 0);
        try (OutputStream out = stream) {
            out.write(new byte[] { (byte) 9, (byte) 10 });
            Assert.assertEquals(stream.queue.size(), 1);
            out.flush();
            Assert.assertEquals(stream.queue.size(), 1);
            out.write(new byte[] { (byte) 11, (byte) 12 }, 1, 1);
            Assert.assertEquals(stream.queue.size(), 1);
            out.flush();
            Assert.assertEquals(stream.queue.size(), 2);
            Assert.assertEquals(stream.queue.poll(), new byte[] { (byte) 9, (byte) 10 });
            Assert.assertEquals(stream.queue.poll(), new byte[] { (byte) 12 });
        }

        try (OutputStream out = stream) {
            out.close();
            out.write(new byte[] { (byte) 13, (byte) 14 }, 0, 1);
            Assert.fail("Write should fail");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("closed") > 0);
        }
    }

    @Test(groups = { "unit" })
    public void testPipedStream() throws Exception {
        final int timeout = 10000;
        ExecutorService executor = Executors.newFixedThreadPool(2);
        for (int bufferSize = -1; bufferSize < 10; bufferSize++) {
            for (int queueLength = -1; queueLength < 10; queueLength++) {
                final NonBlockingPipedOutputStream stream = new NonBlockingPipedOutputStream(bufferSize, queueLength,
                        timeout, CapacityPolicy.fixedCapacity(queueLength), null);
                try (InputStream in = stream.getInputStream(); OutputStream out = stream) {
                    int count = 10000;
                    CountDownLatch latch = new CountDownLatch(count + 1);
                    executor.execute(() -> {
                        for (int i = 0; i < count; i++) {
                            byte[] bytes = new byte[] { (byte) (0xFF & i), (byte) (0xFF & i + 1),
                                    (byte) (0xFF & i + 2) };
                            try {
                                out.write(bytes);
                            } catch (IOException e) {
                                Thread.currentThread().interrupt();
                                throw new UncheckedIOException(e);
                                // Assert.fail("Failed to write", e);
                            }
                        }

                        try {
                            out.close();
                        } catch (IOException e) {
                            Thread.currentThread().interrupt();
                            throw new UncheckedIOException(e);
                            // Assert.fail("Failed to write", e);
                        }
                    });

                    executor.execute(() -> {
                        for (int i = 0; i < count; i++) {
                            byte[] bytes = new byte[] { (byte) (0xFF & i), (byte) (0xFF & i + 1),
                                    (byte) (0xFF & i + 2) };
                            byte[] b = new byte[bytes.length];
                            try {
                                Assert.assertEquals(in.read(b), b.length);
                                latch.countDown();
                                if (!Arrays.equals(b, bytes)) {
                                    System.out.println("Why?");
                                }
                                Assert.assertEquals(b, bytes);
                            } catch (IOException e) {
                                Thread.currentThread().interrupt();
                                throw new UncheckedIOException(e);
                                // Assert.fail("Failed to read", e);
                            }
                        }

                        try {
                            Assert.assertEquals(in.read(), -1);
                            latch.countDown();
                        } catch (IOException e) {
                            Thread.currentThread().interrupt();
                            throw new UncheckedIOException(e);
                            // Assert.fail("Failed to read EOF", e);
                        }
                    });

                    latch.await(timeout / 1000, TimeUnit.SECONDS);
                }
            }
        }
    }
}
