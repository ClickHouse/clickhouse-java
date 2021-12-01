package com.clickhouse.client.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.Buffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHousePipedStreamTest {
    @Test(groups = { "unit" })
    public void testRead() throws Exception {
        ClickHousePipedStream stream = new ClickHousePipedStream(4, 3, 1);
        Assert.assertEquals(stream.queue.size(), 0);
        try (InputStream in = stream.getInput()) {
            in.read();
            Assert.fail("Read should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Read timed out") == 0);
        }

        ByteBuffer buf = ByteBuffer.allocate(1).put(new byte[] { (byte) 3 });
        stream.queue.put((ByteBuffer) ((Buffer) buf).rewind());
        Assert.assertEquals(stream.queue.size(), 1);
        try (InputStream in = stream.getInput()) {
            Assert.assertEquals(in.read(), 3);
            in.read();
            Assert.fail("Read should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Read timed out") == 0);
        }

        buf = ByteBuffer.allocate(2).put(new byte[] { (byte) 3, (byte) 4 });
        stream.queue.put((ByteBuffer) ((Buffer) buf).rewind());
        Assert.assertEquals(stream.queue.size(), 1);
        try (InputStream in = stream.getInput()) {
            Assert.assertEquals(in.read(), 3);
            Assert.assertEquals(in.read(), 4);
            in.read();
            Assert.fail("Read should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Read timed out") == 0);
        }

        stream.queue.clear();
        stream.queue.put(ClickHousePipedStream.EMPTY);
        Assert.assertEquals(stream.queue.size(), 1);
        try (InputStream in = stream.getInput()) {
            Assert.assertEquals(in.read(), -1);
        }

        stream.queue.put((ByteBuffer) ((Buffer) buf).rewind());
        stream.queue.put(buf);
        stream.queue.put(ClickHousePipedStream.EMPTY);
        Assert.assertEquals(stream.queue.size(), 3);
        try (InputStream in = stream.getInput()) {
            Assert.assertEquals(in.read(), 3);
            Assert.assertEquals(in.read(), 4);
            Assert.assertEquals(in.read(), -1);
        }

        try (InputStream in = stream.getInput()) {
            in.close();
            in.read();
            Assert.fail("Read should fail");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("closed") > 0);
        }
    }

    @Test(groups = { "unit" })
    public void testReadBytes() throws Exception {
        ClickHousePipedStream stream = new ClickHousePipedStream(4, 3, 1);
        Assert.assertEquals(stream.queue.size(), 0);
        byte[] bytes = new byte[3];
        try (InputStream in = stream.getInput()) {
            in.read(bytes);
            Assert.fail("Read should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Read timed out") == 0);
        }
        try (InputStream in = stream.getInput()) {
            in.read(bytes, 0, 1);
            Assert.fail("Read should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Read timed out") == 0);
        }

        ByteBuffer buf = ByteBuffer.allocate(2).put(new byte[] { (byte) 3, (byte) 4 });
        stream.queue.put((ByteBuffer) ((Buffer) buf).rewind());
        Assert.assertEquals(stream.queue.size(), 1);
        try (InputStream in = stream.getInput()) {
            in.read(bytes);
            Assert.fail("Read should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Read timed out") == 0);
        }
        stream.queue.put((ByteBuffer) ((Buffer) buf).rewind());
        Assert.assertEquals(stream.queue.size(), 1);
        try (InputStream in = stream.getInput()) {
            Assert.assertEquals(in.read(bytes, 0, 2), 2);
            in.read(bytes, 0, 1);
            Assert.fail("Read should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Read timed out") == 0);
        }
        stream.queue.put((ByteBuffer) ((Buffer) buf).rewind());
        Assert.assertEquals(stream.queue.size(), 1);
        try (InputStream in = stream.getInput()) {
            in.read(bytes, 0, 3);
            Assert.fail("Read should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Read timed out") == 0);
        }

        buf = ByteBuffer.allocate(2).put(new byte[] { (byte) 3, (byte) 4 });
        stream.queue.put((ByteBuffer) ((Buffer) buf).rewind());
        stream.queue.put(ClickHousePipedStream.EMPTY);
        Assert.assertEquals(stream.queue.size(), 2);
        try (InputStream in = stream.getInput()) {
            Assert.assertEquals(in.read(bytes, 0, 3), 2);
            Assert.assertEquals(in.read(bytes, 0, 1), -1);
            Assert.assertEquals(in.read(bytes, 0, 2), -1);
        }

        try (InputStream in = stream.getInput()) {
            in.close();
            in.read(bytes, 0, 3);
            Assert.fail("Read should fail");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("closed") > 0);
        }
    }

    @Test(groups = { "unit" })
    public void testWrite() throws Exception {
        ClickHousePipedStream stream = new ClickHousePipedStream(2, 3, 2);
        Assert.assertEquals(stream.queue.size(), 0);
        try (OutputStream out = stream) {
            out.write(5);
            Assert.assertEquals(stream.queue.size(), 0);
            out.write(6);
            Assert.assertEquals(stream.queue.size(), 0);
            out.write(7);
            Assert.assertEquals(stream.queue.size(), 1);
            out.flush();
            Assert.assertEquals(stream.queue.size(), 2);
            Assert.assertEquals(stream.queue.take().array(), new byte[] { (byte) 5, (byte) 6 });
            Assert.assertEquals(stream.queue.take().array(), new byte[] { (byte) 7, (byte) 0 });
        }

        stream = new ClickHousePipedStream(1, 1, 2);
        Assert.assertEquals(stream.queue.size(), 0);
        try (OutputStream out = stream) {
            out.write(5);
            Assert.assertEquals(stream.queue.size(), 0);
            out.write(6);
            Assert.assertEquals(stream.queue.size(), 1);
            out.write(7);
            Assert.fail("Write should be timed out");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().indexOf("Write timed out") == 0);
        }

        Assert.assertEquals(stream.queue.size(), 1);
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
        ClickHousePipedStream stream = new ClickHousePipedStream(2, 3, 2);
        Assert.assertEquals(stream.queue.size(), 0);
        try (OutputStream out = stream) {
            out.write(new byte[] { (byte) 9, (byte) 10 });
            Assert.assertEquals(stream.queue.size(), 0);
            out.flush();
            Assert.assertEquals(stream.queue.size(), 1);
            out.write(new byte[] { (byte) 11, (byte) 12 }, 1, 1);
            Assert.assertEquals(stream.queue.size(), 1);
            out.flush();
            Assert.assertEquals(stream.queue.size(), 2);
            Assert.assertEquals(stream.queue.take().array(), new byte[] { (byte) 9, (byte) 10 });
            Assert.assertEquals(stream.queue.take().array(), new byte[] { (byte) 12, (byte) 0 });
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
        ExecutorService executor = Executors.newFixedThreadPool(2);
        for (int bufferSize = -1; bufferSize < 10; bufferSize++) {
            for (int queueLength = -1; queueLength < 10; queueLength++) {
                ClickHousePipedStream stream = new ClickHousePipedStream(bufferSize, queueLength, 10000);
                try (InputStream in = stream.getInput(); OutputStream out = stream) {
                    int count = 10000;
                    CountDownLatch latch = new CountDownLatch(count + 1);
                    executor.execute(() -> {
                        for (int i = 0; i < count; i++) {
                            byte[] bytes = new byte[] { (byte) (0xFF & i), (byte) (0xFF & i + 1),
                                    (byte) (0xFF & i + 2) };
                            try {
                                out.write(bytes);
                            } catch (IOException e) {
                                Assert.fail("Failed to write", e);
                            }
                        }

                        try {
                            out.close();
                        } catch (IOException e) {
                            Assert.fail("Failed to write", e);
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
                                Assert.assertEquals(b, bytes);
                            } catch (IOException e) {
                                Assert.fail("Failed to read", e);
                            }
                        }

                        try {
                            Assert.assertEquals(in.read(), -1);
                            latch.countDown();
                        } catch (IOException e) {
                            Assert.fail("Failed to read EOF", e);
                        }
                    });

                    latch.await();
                }
            }
        }
    }
}
