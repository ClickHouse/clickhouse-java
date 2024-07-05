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
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.http.config.HttpConnectionProvider;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.admin.model.ScenarioState;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

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

    @Test(groups = {"unit"})
    public void testRetryOnFailure() {
        faultyServer = new WireMockServer(9090);
        faultyServer.start();
        try {
            faultyServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                            .withRequestBody(WireMock.equalTo("SELECT 1"))
                            .inScenario("Retry")
                            .whenScenarioStateIs(Scenario.STARTED)
                            .willReturn(WireMock.aResponse().withFault(Fault.EMPTY_RESPONSE))
                            .willSetStateTo("Failed")
                    .build());
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
}
