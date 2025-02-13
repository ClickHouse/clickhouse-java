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
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static org.testng.Assert.fail;

public class ProxyTests extends BaseIntegrationTest{
    private ThreadLocal<Client> client = new ThreadLocal<>();

    private ThreadLocal<WireMockServer> proxy = new ThreadLocal<>();

    @BeforeMethod(groups = { "integration" })
    public void setUp() throws IOException {
    }

    @AfterMethod(groups = { "integration" })
    public void teardown() {
        if (isCloud()) {
            return; // nothing to stop
        }

        proxy.get().stop();
        client.get().close();
    }

    @Test(groups = { "integration" })
    public void testSimpleQuery() throws Exception {
        if (isCloud()) {
            return; // to specific setup for cloud, may be later
        }

        client.set(clientBuilder(initProxy(), false).build());
        addProxyStub();

        List<GenericRecord> records = client.get().queryAll("select timezone()");
        Assert.assertEquals(records.stream().findFirst().get().getString(1), "UTC");
    }

    @Test(groups = { "integration" })
    public void testInsert() throws Exception {
        if (isCloud()) {
            return; // to specific setup for cloud, may be later
        }
        String tableName = "simple_pojo_disable_proxy_table";
        String createSQL = SamplePOJO.generateTableCreateSQL(tableName);
        client.set(clientBuilder(initProxy(), false).build());
        addProxyStub();

        client.get().execute(createSQL).get();
        client.get().register(SamplePOJO.class, client.get().getTableSchema(tableName, "default"));
        List<Object> simplePOJOs = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            simplePOJOs.add(new SamplePOJO());
        }

        try {
            InsertResponse response = client.get().insert(tableName, simplePOJOs).get(120, TimeUnit.SECONDS);
            Assert.assertEquals(response.getWrittenRows(), 1000);
        } catch (Exception e) {
            fail("Should not have thrown exception.", e);
        }
    }

    @Test(groups = { "integration" })
    public void testPrivateProxyWithoutAuth() {
        if (isCloud()) {
            return; // to specific setup for cloud, may be later
        }

        client.set(clientBuilder(initProxy(), true).build());
        addPrivateProxyStub();

        try {
            client.get().execute("select 1").get();
            Assert.fail("Should have thrown exception.");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof ClientException);
        } catch (ClientMisconfigurationException e) {
            Assert.assertTrue(e.getMessage().contains("Proxy authentication required"));
        } catch (Exception e) {
            Assert.fail("Should have thrown exception.", e);
        }
    }

    @Test(groups = { "integration" })
    public void testPrivateProxyWithCredentials() {
        if (isCloud()) {
            return; // to specific setup for cloud, may be later
        }

        client.set(clientBuilder(initProxy(), true)
                .setProxyCredentials("user", "pass").build());
        addPrivateProxyStub();

        try {
            client.get().execute("select 1");
        } catch (ClientException e) {
            e.printStackTrace();
            Assert.fail("Should not have thrown exception.", e);
        }
    }

    @Test(groups = { "integration" })
    public void testProxyWithCookies() {
        if (isCloud()) {
            return; // to specific setup for cloud, may be later
        }

        client.set(clientBuilder(initProxy(), true).build());
        final int targetPort = getServer(ClickHouseProtocol.HTTP).getPort();

        proxy.get().addStubMapping(post(urlMatching("/.*"))
                        .inScenario("routeCookies")
                        .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withHeader(HttpHeaders.SET_COOKIE, "routeName=routeA")
                        .proxiedFrom("http://localhost:" + targetPort)).willSetStateTo("cookies").build());

        proxy.get().addStubMapping(post(urlMatching("/.*"))
                        .inScenario("routeCookies")
                        .whenScenarioStateIs("cookies")
                        .withHeader(HttpHeaders.COOKIE, equalTo("routeName=routeA"))
                .willReturn(aResponse().proxiedFrom("http://localhost:" + targetPort)).build());

        try {
            client.get().execute("select 1").get();
            client.get().execute("select 1").get();
        } catch (Exception e) {
            fail("Should not have thrown exception.", e);
        }
    }

    @Test(groups = { "integration" })
    public void testProxyWithDisabledCookies() {
        if (isCloud()) {
            return; // to specific setup for cloud, may be later
        }

        client.set(clientBuilder(initProxy(), true).setHttpCookiesEnabled(false).build());
        final int targetPort = getServer(ClickHouseProtocol.HTTP).getPort();

        proxy.get().addStubMapping(post(anyUrl())
                .inScenario("routeCookies")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withHeader(HttpHeaders.SET_COOKIE, "routeName=routeA")
                        .proxiedFrom("http://localhost:" + targetPort))
                .willSetStateTo("cookies").build());

        proxy.get().addStubMapping(post(anyUrl())
                .inScenario("routeCookies")
                .whenScenarioStateIs("cookies")
                .withHeader(HttpHeaders.COOKIE, equalTo("routeName=routeA"))
                .willReturn(aResponse().proxiedFrom("http://localhost:" + targetPort)).build());

        proxy.get().addStubMapping(post(anyUrl())
                .inScenario("routeCookies")
                .whenScenarioStateIs("cookies")
                .withHeader(HttpHeaders.COOKIE, WireMock.absent())
                .willReturn(aResponse().withStatus(HttpStatus.SC_BAD_GATEWAY)).build());

        try {
            client.get().execute("select 1").get();
        } catch (Exception e) {
            fail("Should not have thrown exception.", e);
        }
        try {
            client.get().execute("select 1").get();
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof ClientException);
        } catch (ClientException e) {
            Assert.assertTrue(e.getMessage().contains("Server returned '502 Bad gateway'"));
        } catch (Exception e) {
            Assert.fail("Should have thrown exception.", e);
        }
    }

    private Client.Builder clientBuilder(int proxyPort, boolean onlyNewImplementation) {
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, "clickhouse", 8123, false)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .useNewImplementation(onlyNewImplementation ? onlyNewImplementation :
                        System.getProperty("client.tests.useNewImplementation", "true").equals("true"))
                .addProxy(ProxyType.HTTP, "localhost", proxyPort);
    }

    private int initProxy() {
        WireMockServer wireMock = new WireMockServer(WireMockConfiguration.options()
//                .notifier(new Slf4jNotifier(true))
        );
        wireMock.start();
        proxy.set(wireMock);
        return wireMock.port();
    }

    private void addProxyStub() {
        final int targetPort = getServer(ClickHouseProtocol.HTTP).getPort();
        proxy.get().addStubMapping(post(urlMatching("/.*"))
                .willReturn(aResponse().proxiedFrom("http://localhost:" + targetPort)).build());
    }

    private void addPrivateProxyStub() {
        final int targetPort = getServer(ClickHouseProtocol.HTTP).getPort();
        proxy.get().addStubMapping(post(urlMatching("/.*"))
                .willReturn(aResponse()
                        .withStatus(HttpStatus.SC_PROXY_AUTHENTICATION_REQUIRED)
                        .withHeader(HttpHeaders.PROXY_AUTHENTICATE, "Basic realm=\"Access DB\"")).build());
        proxy.get().addStubMapping(post(urlMatching("/.*"))
                .withHeader(HttpHeaders.PROXY_AUTHORIZATION, equalTo("Basic dXNlcjpwYXNz"))
                .willReturn(aResponse().proxiedFrom("http://localhost:" + targetPort)).build());
    }
}
