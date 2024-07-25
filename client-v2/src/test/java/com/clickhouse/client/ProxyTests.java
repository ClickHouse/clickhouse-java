package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.enums.ProxyType;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.insert.SamplePOJO;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static org.testng.Assert.fail;

public class ProxyTests extends BaseIntegrationTest{
    private Client client;

    private WireMockServer proxy = new WireMockServer(8666);

    private WireMockServer privateProxy = new WireMockServer(WireMockConfiguration.options()
            .port(8667)
            .notifier(new Slf4jNotifier(true)));

    @BeforeMethod(groups = { "integration" })
    public void setUp() throws IOException {
        proxy.start();
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        proxy.addStubMapping(post(urlMatching("/.*"))
                .willReturn(aResponse().proxiedFrom("http://localhost:" + node.getPort())).build());
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, "clickhouse", 8123, false)
                .setUsername("default")
                .setPassword("")
                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "false").equals("true"))
                .addProxy(ProxyType.HTTP, "localhost", 8666)
                .build();

        // private proxy
        privateProxy.start();
        // stub order - from least specific to most specific
        privateProxy.addStubMapping(post(urlMatching("/.*"))
                .willReturn(aResponse()
                        .withStatus(HttpStatus.SC_PROXY_AUTHENTICATION_REQUIRED)
                        .withHeader(HttpHeaders.PROXY_AUTHENTICATE, "Basic realm=\"Access DB\"")).build());
        privateProxy.addStubMapping(post(urlMatching("/.*"))
                .withHeader(HttpHeaders.PROXY_AUTHORIZATION, equalTo("Basic dXNlcjpwYXNz"))
                .willReturn(aResponse().proxiedFrom("http://localhost:" + node.getPort())).build());
    }

    @AfterMethod(groups = { "integration" })
    public void teardown() {
        proxy.stop();
        privateProxy.stop();
        client.close();
    }

    private void createTable(String tableQuery) throws ClickHouseException {
        try (ClickHouseClient client = ClickHouseClient.builder().config(new ClickHouseConfig())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build()) {
            client.read(getServer(ClickHouseProtocol.HTTP)).query(tableQuery).executeAndWait().close();
        }
    }
    
    @Test(groups = { "integration" })
    public void testSimpleQuery() throws Exception {
        List<GenericRecord> records = client.queryAll("select timezone()");
        Assert.assertEquals(records.stream().findFirst().get().getString(1), "UTC");
    }

    @Test(groups = { "integration" })
    public void testInsert() throws Exception {
        String tableName = "simple_pojo_disable_proxy_table";
        String createSQL = SamplePOJO.generateTableCreateSQL(tableName);
        System.out.println(createSQL);
        createTable(createSQL);

        client.register(SamplePOJO.class, client.getTableSchema(tableName, "default"));
        List<Object> simplePOJOs = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            simplePOJOs.add(new SamplePOJO());
        }

        try {
            InsertResponse response = client.insert(tableName, simplePOJOs).get(120, TimeUnit.SECONDS);
            Assert.assertEquals(response.getWrittenRows(), 1000);
        } catch (Exception e) {
            fail("Should not have thrown exception.", e);
        }
    }

    @Test(groups = { "integration" })
    public void testPrivateProxyWithoutAuth() {
        Client localClient = null;
        try {
            localClient = new Client.Builder()
                    .addEndpoint(Protocol.HTTP, "clickhouse", 8123, false)
                    .setUsername("user")
                    .setPassword("")
                    .useNewImplementation(true)
//                    .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "false").equals("true"))
                    .addProxy(ProxyType.HTTP, "localhost", 8667)
                    .build();
            List<GenericRecord> records = localClient.queryAll("select timezone()");
            Assert.fail("Should have thrown exception.");
        } catch (ClientException e) {
            e.printStackTrace();
            Assert.assertTrue(e.getCause() instanceof ClientMisconfigurationException);
        } finally {
            if (localClient != null) {
                localClient.close();
            }

        }
    }

    @Test(groups = { "integration" })
    public void testPrivateProxyWithCredentials() {
        Client localClient = null;
        try {
            localClient = new Client.Builder()
                    .addEndpoint(Protocol.HTTP, "clickhouse", 8123, false)
                    .setUsername("user")
                    .setPassword("")
                    .useNewImplementation(true)
                    .addProxy(ProxyType.HTTP, "localhost", 8667)
                    .setProxyCredentials("user", "pass")
                    .build();
            List<GenericRecord> records = localClient.queryAll("select timezone()");
            Assert.assertEquals(records.stream().findFirst().get().getString(1), "UTC");
        } catch (Exception e) {
            Assert.fail("Should not have thrown exception.", e);
        } finally {
            if (localClient != null) {
                localClient.close();
            }

        }
    }
}
