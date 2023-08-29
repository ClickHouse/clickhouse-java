package com.clickhouse.client;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

public class AbstractClientTest {
    static class SimpleClient extends AbstractClient<Object[]> {
        @Override
        protected boolean checkHealth(ClickHouseNode server, int timeout) {
            return false;
        }

        @Override
        protected Collection<ClickHouseProtocol> getSupportedProtocols() {
            return Collections.singletonList(ClickHouseProtocol.POSTGRESQL);
        }

        @Override
        protected Object[] newConnection(Object[] connection, ClickHouseNode server, ClickHouseRequest<?> request) {
            if (connection != null) {
                closeConnection(connection, false);
            }

            return new Object[] { request.getConfig(), server };
        }

        @Override
        protected void closeConnection(Object[] connection, boolean force) {
            connection[0] = null;
            connection[1] = null;
        }

        @Override
        protected ClickHouseResponse sendAsync(ClickHouseRequest<?> sealedRequest, Object... args)
                throws ClickHouseException, IOException {
            return send(sealedRequest);
        }

        @Override
        protected ClickHouseResponse send(ClickHouseRequest<?> sealedRequest) throws ClickHouseException, IOException {
            return null;
        }
    }

    @Test(groups = { "unit" })
    public void testClose() {
        SimpleClient sc = new SimpleClient();
        Assert.assertFalse(sc.isInitialized());
        sc.close();
        Assert.assertFalse(sc.isInitialized());

        sc.init(new ClickHouseConfig());
        Assert.assertNotNull(sc.getExecutor());
        Assert.assertTrue(sc.isInitialized());
        Assert.assertNotNull(sc.getConfig());
        Assert.assertNull(sc.getServer());
        sc.close();
        Assert.assertFalse(sc.isInitialized());
        Assert.assertThrows(IllegalStateException.class, () -> sc.getConfig());
        Assert.assertThrows(IllegalStateException.class, () -> sc.getExecutor());
        Assert.assertThrows(IllegalStateException.class, () -> sc.getServer());

        SimpleClient client = new SimpleClient();
        client.init(new ClickHouseConfig());
        ClickHouseRequest<?> req = client.read(ClickHouseNode.builder().build());
        ClickHouseConfig config = new ClickHouseConfig();
        sc.init(config);
        Assert.assertNotNull(sc.getExecutor());
        Assert.assertTrue(sc.isInitialized());
        Assert.assertNotNull(sc.getConfig());
        Assert.assertNull(sc.getServer());
        Assert.assertEquals(sc.getConnection(req), new Object[] { req.getConfig(), req.getServer() });
        sc.close();
        Assert.assertFalse(sc.isInitialized());
        Assert.assertThrows(IllegalStateException.class, () -> sc.getConfig());
        Assert.assertThrows(IllegalStateException.class, () -> sc.getConnection(req));
        Assert.assertThrows(IllegalStateException.class, () -> sc.getExecutor());
        Assert.assertThrows(IllegalStateException.class, () -> sc.getServer());
    }

    @Test(groups = { "unit" })
    public void testCloseRunningClient() throws InterruptedException {
        SimpleClient client = new SimpleClient();
        client.init(new ClickHouseConfig());
        ClickHouseRequest<?> req = client.read(ClickHouseNode.builder().build());

        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                Object[] conn = client.getConnection(req);
                Thread.sleep(1000L);
                client.close();
                latch.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }).start();
        Assert.assertTrue(client.isInitialized());
        Assert.assertTrue(latch.await(5000L, TimeUnit.MILLISECONDS));
        Assert.assertFalse(client.isInitialized());
    }

    @Test(groups = { "unit" })
    public void testGetAndCloseConnection() {
        SimpleClient client = new SimpleClient();
        client.init(new ClickHouseConfig());
        ClickHouseRequest<?> req = client.read(ClickHouseNode.builder().build());

        SimpleClient sc = new SimpleClient();
        sc.init(new ClickHouseConfig());
        Assert.assertEquals(sc.getConnection(req), new Object[] { req.getConfig(), req.getServer() });

        req = client.read(ClickHouseNode.of("127.0.0.1", ClickHouseProtocol.POSTGRESQL, 9100, "test"));
        Object[] conn = sc.getConnection(req);
        Assert.assertEquals(conn, new Object[] { req.getConfig(), req.getServer() });
        sc.close();
        Assert.assertNull(conn[0]);
        Assert.assertNull(conn[1]);
    }

    @Test(groups = { "unit" })
    public void testInit() {
        SimpleClient client = new SimpleClient();
        client.init(new ClickHouseConfig());
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.read(ClickHouseNode.builder().port(ClickHouseProtocol.MYSQL).build()).getServer());

        ClickHouseRequest<?> req = client.read(ClickHouseNode.builder().build());
        SimpleClient sc = new SimpleClient();
        Assert.assertFalse(sc.isInitialized());
        Assert.assertThrows(IllegalStateException.class, () -> sc.read(ClickHouseNode.builder().build()));
        Assert.assertThrows(IllegalStateException.class, () -> sc.getConfig());
        Assert.assertThrows(IllegalStateException.class, () -> sc.getConnection(req));
        Assert.assertThrows(IllegalStateException.class, () -> sc.getExecutor());
        Assert.assertThrows(IllegalStateException.class, () -> sc.getServer());

        Assert.assertNotNull(req.getServer());

        ClickHouseConfig config = new ClickHouseConfig();
        sc.init(config);
        if (config.getMaxThreadsPerClient() > 0) {
            Assert.assertNotEquals(sc.getExecutor(), ClickHouseClient.getExecutorService());
        } else {
            Assert.assertEquals(sc.getExecutor(), ClickHouseClient.getExecutorService());
        }
        Assert.assertTrue(sc.isInitialized());
        Assert.assertTrue(sc.getConfig() != config);
        Assert.assertNull(sc.getServer());
        Assert.assertEquals(sc.getConnection(req), new Object[] { req.getConfig(), req.getServer() });
        Assert.assertEquals(sc.getServer(), req.getServer());

        ClickHouseConfig newConfig = new ClickHouseConfig();
        sc.init(newConfig);
        if (config.getMaxThreadsPerClient() > 0) {
            Assert.assertNotEquals(sc.getExecutor(), ClickHouseClient.getExecutorService());
        } else {
            Assert.assertEquals(sc.getExecutor(), ClickHouseClient.getExecutorService());
        }
        Assert.assertTrue(sc.isInitialized());
        Assert.assertTrue(sc.getConfig() != config);
        Assert.assertEquals(sc.getConnection(req), new Object[] { req.getConfig(), req.getServer() });
        Assert.assertEquals(sc.getServer(), req.getServer());
    }

    @Test(groups = { "unit" })
    public void testSwitchNode() throws InterruptedException {
        ClickHouseConfig config = new ClickHouseConfig();
        SimpleClient client = new SimpleClient();
        client.init(config);
        ClickHouseRequest<?> req1 = client.read(ClickHouseNode.builder().build());
        ClickHouseRequest<?> req2 = client
                .read(ClickHouseNode.of("127.0.0.1", ClickHouseProtocol.POSTGRESQL, 9100, "test"));

        Object[] conn1 = client.getConnection(req1);
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                Thread.sleep(1000L);
                Object[] conn2 = client.getConnection(req2);
                latch.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }).start();
        Assert.assertEquals(conn1, new Object[] { req1.getConfig(), req1.getServer() });
        Assert.assertTrue(latch.await(5000L, TimeUnit.MILLISECONDS));
        Assert.assertTrue(client.isInitialized());
        Assert.assertNull(conn1[0]);
        Assert.assertNull(conn1[1]);
        Object[] conn2 = client.getConnection(req2);
        Assert.assertTrue(conn1 != conn2);
        Assert.assertEquals(conn2, new Object[] { req2.getConfig(), req2.getServer() });
    }
}
