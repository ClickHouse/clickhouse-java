package com.clickhouse.client.http;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseSocketFactory;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.http.config.HttpConnectionProvider;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseUtils;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.http.trafficlistener.WiremockNetworkTrafficListener;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.net.URIBuilder;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ApacheHttpConnectionImplTest extends ClickHouseHttpClientTest {
    public static class CustomSocketFactory implements ClickHouseSocketFactory {
        private static final AtomicBoolean created = new AtomicBoolean();

        @Override
        public <T> T create(ClickHouseConfig config, Class<T> clazz) throws IOException, UnsupportedOperationException {
            if (config == null || clazz == null) {
                throw new IllegalArgumentException("Non-null configuration and class are required");
            } else if (List.class.isAssignableFrom(clazz)) {
                return clazz.cast(Collections.singletonList(config));
            } else if (PlainConnectionSocketFactory.class.isAssignableFrom(clazz)) {
                if (created.compareAndSet(false, true)) {
                    return ApacheHttpConnectionImpl.ApacheHttpClientSocketFactory.instance.create(config, clazz);
                } else {
                    throw new IOException("socket factory has already created");
                }
            }
            throw new UnsupportedOperationException(ClickHouseUtils.format("Class %s is not supported", clazz));
        }

        @Override
        public boolean supports(Class<?> clazz) {
            return List.class.isAssignableFrom(clazz) || PlainConnectionSocketFactory.class.isAssignableFrom(clazz);
        }
    }

    @Override
    protected Map<ClickHouseOption, Serializable> getClientOptions() {
        return Collections.singletonMap(ClickHouseHttpOption.CONNECTION_PROVIDER,
                HttpConnectionProvider.APACHE_HTTP_CLIENT);
    }

    @Test(groups = { "integration" })
    public void testConnection() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (ClickHouseClient client = ClickHouseClient.newInstance()) {

            ClickHouseRequest<?> req1 = newRequest(client, server);
            try (ClickHouseResponse resp = req1.query("select 1").executeAndWait()) {
                Assert.assertEquals(resp.firstRecord().getValue(0).asString(), "1");
            }

            // req2 will use same connection with req1
            ClickHouseRequest<?> req2 = newRequest(client, server);
            try (ClickHouseResponse resp = req2.query("select 1").executeAndWait()) {
                Assert.assertEquals(resp.firstRecord().getValue(0).asString(), "1");
            }
        }
    }

    @Test(groups = { "integration" })
    @Ignore("Need to disable the option to provide custom socket factory. note: need to remove it")
    public void testCustomOptions() throws Exception {
        Map<String, String> customOptions = new HashMap<>();
        customOptions.put(ClickHouseHttpOption.CONNECTION_PROVIDER.getKey(),
                HttpConnectionProvider.APACHE_HTTP_CLIENT.name());
        customOptions.put("", "nothing");
        customOptions.put("my.custom.option.1", "one");

        ClickHouseNode server = getServer(getProtocol(), customOptions);
        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseRequest<?> req = newRequest(client, server);
            try (ClickHouseResponse resp = req.query("select 1").executeAndWait()) {
                Assert.assertEquals(resp.firstRecord().getValue(0).asString(), "1");
            }
        }

        customOptions.put(ClickHouseClientOption.CUSTOM_SOCKET_FACTORY.getKey(), CustomSocketFactory.class.getName());
        customOptions.put(ClickHouseClientOption.CUSTOM_SOCKET_FACTORY_OPTIONS.getKey(), "a=1, b = 2, c='3\\,5'");
        server = getServer(getProtocol(), customOptions);
        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            Assert.assertFalse(CustomSocketFactory.created.get());
            ClickHouseRequest<?> req = newRequest(client, server);

            Map<String, String> addtionalOptions = new TreeMap<>();
            addtionalOptions.put("a", "1");
            addtionalOptions.put("b", "2");
            addtionalOptions.put("c", "'3,5'");
            Assert.assertEquals(req.getConfig().getCustomSocketFactory(), CustomSocketFactory.class.getName());
            Assert.assertEquals(req.getConfig().getCustomSocketFactoryOptions(), addtionalOptions);
            try (ClickHouseResponse resp = req.query("select 1").executeAndWait()) {
                Assert.assertEquals(resp.firstRecord().getValue(0).asString(), "1");
            }
            Assert.assertTrue(CustomSocketFactory.created.get());
        } finally {
            CustomSocketFactory.created.set(false);
        }
    }

    private WireMockServer faultyServer;

    @Test(groups = {"unit"})
    public void testFailureWhileRequest() {
        faultyServer = new WireMockServer(9090);
        faultyServer.start();
        try {
            faultyServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                    .willReturn(WireMock.aResponse().withFault(Fault.EMPTY_RESPONSE)).build());

            ClickHouseHttpClient httpClient = new ClickHouseHttpClient();
            ClickHouseConfig config = new ClickHouseConfig();
            httpClient.init(config);
            ClickHouseRequest request = httpClient.read("http://localhost:9090/").query("SELECT 1");

            try {
                httpClient.executeAndWait(request);
            } catch (ClickHouseException e) {
                Assert.assertEquals(e.getErrorCode(), ClickHouseException.ERROR_NETWORK);
                return;
            }

            Assert.fail("Should throw exception");
        } finally {
            faultyServer.stop();
        }
    }

    @Test(groups = {"unit"}, dataProvider = "retryOnFailureProvider")
    public void testRetryOnFailure(StubMapping failureStub) {
        faultyServer = new WireMockServer(9090);
        faultyServer.start();
        try {
            faultyServer.addStubMapping(failureStub);
            faultyServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                            .withRequestBody(WireMock.equalTo("SELECT 1"))
                            .inScenario("Retry")
                            .whenScenarioStateIs("Failed")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("X-ClickHouse-Summary",
                                            "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}"))
                    .build());

            ClickHouseHttpClient httpClient = new ClickHouseHttpClient();
            Map<ClickHouseOption, Serializable> options = new HashMap<>();
            options.put(ClickHouseHttpOption.AHC_RETRY_ON_FAILURE, true);
            ClickHouseConfig config = new ClickHouseConfig(options);
            httpClient.init(config);
            ClickHouseRequest request = httpClient.read("http://localhost:9090/").query("SELECT 1");

            ClickHouseResponse response = null;
            try {
                response = httpClient.executeAndWait(request);
            } catch (ClickHouseException e) {
                Assert.fail("Should not throw exception", e);
            }
            Assert.assertEquals(response.getSummary().getReadBytes(), 10);
            Assert.assertEquals(response.getSummary().getReadRows(), 1);
        } finally {
            faultyServer.stop();
        }
    }

    @DataProvider(name = "retryOnFailureProvider")
    private static StubMapping[] retryOnFailureProvider() {
        return new StubMapping[] {
                WireMock.post(WireMock.anyUrl())
                        .withRequestBody(WireMock.equalTo("SELECT 1"))
                        .inScenario("Retry")
                        .whenScenarioStateIs(Scenario.STARTED)
                        .willReturn(WireMock.aResponse().withFault(Fault.EMPTY_RESPONSE))
                        .willSetStateTo("Failed")
                        .build()
                ,WireMock.post(WireMock.anyUrl())
                        .withRequestBody(WireMock.equalTo("SELECT 1"))
                        .inScenario("Retry")
                        .whenScenarioStateIs(Scenario.STARTED)
                        .willReturn(WireMock.aResponse().withStatus(HttpStatus.SC_SERVICE_UNAVAILABLE))
                        .willSetStateTo("Failed")
                        .build()
        };
    }

    @Test(groups = {"unit"}, dataProvider = "validationTimeoutProvider")
    public void testNoHttpResponseExceptionWithValidation(long validationTimeout) {

        faultyServer = new WireMockServer(9090);
        faultyServer.start();

        faultyServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                .inScenario("validateOnStaleConnection")
                .withRequestBody(WireMock.equalTo("SELECT 100"))
                .willReturn(WireMock.aResponse()
                    .withHeader("X-ClickHouse-Summary",
                                "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}"))
                .build());


        ClickHouseHttpClient httpClient = new ClickHouseHttpClient();
        Map<ClickHouseOption, Serializable> options = new HashMap<>();
        options.put(ClickHouseHttpOption.AHC_VALIDATE_AFTER_INACTIVITY, validationTimeout);
        options.put(ClickHouseHttpOption.MAX_OPEN_CONNECTIONS, 1);
        ClickHouseConfig config = new ClickHouseConfig(options);
        httpClient.init(config);
        ClickHouseRequest request = httpClient.read("http://localhost:9090/").query("SELECT 100");

        Runnable powerBlink = () -> {
            try {
                Thread.sleep(100);
                faultyServer.stop();
                Thread.sleep(50);
                faultyServer.start();
            } catch (InterruptedException e) {
                Assert.fail("Unexpected exception", e);
            }
        };
        try {
            ClickHouseResponse response = httpClient.executeAndWait(request);
            Assert.assertEquals(response.getSummary().getReadRows(), 1);
            response.close();
            new Thread(powerBlink).start();
            Thread.sleep(200);
            response = httpClient.executeAndWait(request);
            Assert.assertEquals(response.getSummary().getReadRows(), 1);
            response.close();
        } catch (Exception e) {
            if (validationTimeout < 0) {
                Assert.assertTrue(e instanceof ClickHouseException);
                Assert.assertTrue(e.getCause() instanceof ConnectException);
            } else {
                Assert.fail("Unexpected exception", e);
            }
        } finally {
            faultyServer.stop();
        }
    }

    @DataProvider(name = "validationTimeoutProvider")
    public static Object[] validationTimeoutProvider() {
        return new Long[] {-1L , 100L };
    }

    @Test(groups = {"integration"},dataProvider = "testConnectionTTLProvider")
    @SuppressWarnings("java:S2925")
    public void testConnectionTTL(Map<ClickHouseOption, Serializable> options, int openSockets) throws Exception {
        if (isCloud()) {
            // skip for cloud because wiremock proxy need extra configuration. TODO: need to fix it
            return;
        }
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        int proxyPort = new Random().nextInt(1000) + 10000;
        System.out.println("proxyPort: " + proxyPort);
        ConnectionCounterListener connectionCounter = new ConnectionCounterListener();
        WireMockServer proxy = new WireMockServer(WireMockConfiguration
                .options().port(proxyPort)
                .networkTrafficListener(connectionCounter)
                .notifier(new Slf4jNotifier(true)));
        proxy.start();
        URIBuilder targetURI = new URIBuilder(server.getBaseUri())
                .setPath("");
        proxy.addStubMapping(WireMock.post(WireMock.anyUrl())
                .willReturn(WireMock.aResponse().proxiedFrom(targetURI.build().toString())).build());

        Map<ClickHouseOption, Serializable> baseOptions = new HashMap<>();
        baseOptions.put(ClickHouseClientOption.PROXY_PORT, proxyPort);
        baseOptions.put(ClickHouseClientOption.PROXY_HOST, "localhost");
        baseOptions.put(ClickHouseClientOption.PROXY_TYPE, ClickHouseProxyType.HTTP);
        baseOptions.put(ClickHouseDefaults.PASSWORD, getPassword());
        baseOptions.put(ClickHouseDefaults.USER, "default");
        baseOptions.putAll(options);

        ClickHouseConfig config = new ClickHouseConfig(baseOptions);
        try (ClickHouseClient client = ClickHouseClient.builder().config(config).build()) {
            try (ClickHouseResponse resp = client.read(server).query("select 1").executeAndWait()) {
                Assert.assertEquals(resp.firstRecord().getValue(0).asString(), "1");
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                Assert.fail("Unexpected exception", e);
            }

            try (ClickHouseResponse resp = client.read(server).query("select 1").executeAndWait()) {
                Assert.assertEquals(resp.firstRecord().getValue(0).asString(), "1");
            }
        } catch (Exception e) {
            Assert.fail("Unexpected exception", e);
        } finally {
            Assert.assertEquals(connectionCounter.opened.get(), openSockets);
            proxy.stop();
        }
    }

    @DataProvider(name = "testConnectionTTLProvider")
    public static Object[][]  testConnectionTTLProvider() {
        HashMap<ClickHouseOption, Serializable> disabledKeepAlive = new HashMap<>();
        disabledKeepAlive.put(ClickHouseHttpOption.KEEP_ALIVE_TIMEOUT, 1000L);
        disabledKeepAlive.put(ClickHouseHttpOption.KEEP_ALIVE, false);
        HashMap<ClickHouseOption, Serializable> fifoOption = new HashMap<>();
        fifoOption.put(ClickHouseClientOption.CONNECTION_TTL, 1000L);
        fifoOption.put(ClickHouseHttpOption.CONNECTION_REUSE_STRATEGY, "FIFO");
        return new Object[][] {
                { Collections.singletonMap(ClickHouseClientOption.CONNECTION_TTL, 1000L), 2 },
                { Collections.singletonMap(ClickHouseClientOption.CONNECTION_TTL, 2000L), 1 },
                { Collections.singletonMap(ClickHouseHttpOption.KEEP_ALIVE_TIMEOUT, 2000L), 1 },
                { Collections.singletonMap(ClickHouseHttpOption.KEEP_ALIVE_TIMEOUT, 500L), 2 },
                { disabledKeepAlive, 2 },
                { fifoOption, 2 }
        };
    }

    private static class ConnectionCounterListener implements WiremockNetworkTrafficListener {

        private AtomicInteger opened = new AtomicInteger(0);
        private AtomicInteger closed = new AtomicInteger(0);

        @Override
        public void opened(Socket socket) {
            opened.incrementAndGet();
            System.out.println("Opened: " + socket);
        }

        @Override
        public void incoming(Socket socket, ByteBuffer bytes) {
            // ignore
        }

        @Override
        public void outgoing(Socket socket, ByteBuffer bytes) {
            // ignore
        }

        @Override
        public void closed(Socket socket) {
            closed.incrementAndGet();
            System.out.println("Closed: " + socket);
        }
    }
}
