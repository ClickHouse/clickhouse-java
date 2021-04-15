package ru.yandex.clickhouse.util;

import org.apache.http.HttpHost;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;

import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.time.Instant;

import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;


public class ClickHouseHttpClientBuilderTest {

    private static WireMockServer mockServer;

    @BeforeClass
    public static void beforeAll() {
        mockServer = new WireMockServer(
            WireMockConfiguration.wireMockConfig().dynamicPort());
        mockServer.start();
    }

    @AfterMethod
    public void afterTest() {
        mockServer.resetAll();
    }

    @AfterClass
    public static void afterAll() {
        mockServer.stop();
        mockServer = null;
    }

    @Test
    public void testCreateClientContextNull() {
        assertNull(ClickHouseHttpClientBuilder.createClientContext(null).getAuthCache());
    }

    @Test
    public void testCreateClientContextNoUserNoPass() {
        assertNull(ClickHouseHttpClientBuilder.createClientContext(new ClickHouseProperties())
            .getAuthCache());
    }

    @Test
    public void testCreateClientContextNoHost() {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUser("myUser");
        props.setPassword("mySecret");
        assertNull(ClickHouseHttpClientBuilder.createClientContext(props).getAuthCache());
    }

    @Test
    public void testCreateClientContextUserPass() {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUser("myUser");
        props.setPassword("mySecret");
        props.setHost("127.0.0.1");
        assertEquals(
            ClickHouseHttpClientBuilder.createClientContext(props).getAuthCache()
                .get(HttpHost.create("http://127.0.0.1:80")).getSchemeName(),
            "basic");
    }

    @Test
    public void testCreateClientContextOnlyUser() {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUser("myUser");
        props.setHost("127.0.0.1");
        assertEquals(
            ClickHouseHttpClientBuilder.createClientContext(props).getAuthCache()
                .get(HttpHost.create("http://127.0.0.1:80")).getSchemeName(),
            "basic");
    }

    @Test
    public void testCreateClientContextOnlyPass() {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setPassword("myPass");
        props.setHost("127.0.0.1");
        assertEquals(
            ClickHouseHttpClientBuilder.createClientContext(props).getAuthCache()
                .get(HttpHost.create("http://127.0.0.1:80")).getSchemeName(),
            "basic");
    }

    @Test
    public void testHttpClientsWithSharedCookie() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setHost("localhost");
        props.setPort(mockServer.port());
        props.setUseSharedCookieStore(true);
        CloseableHttpClient client = new ClickHouseHttpClientBuilder(props).buildClient();
        String cookie = "AWS-ALB=random-value-" + Instant.now().toEpochMilli();
        mockServer.stubFor(WireMock.get(WireMock.urlPathMatching("/cookie/get"))
                                   .willReturn(WireMock.aResponse()
                                                       .withStatus(200)
                                                       .withHeader("set-cookie", cookie)
                                                       .withBody("OK")));
        HttpGet getCookie = new HttpGet(mockServer.baseUrl() + "/cookie/get");
        client.execute(getCookie);
        CloseableHttpClient clientWithSharedCookieStore = new ClickHouseHttpClientBuilder(props).buildClient();
        props.setUseSharedCookieStore(false);
        CloseableHttpClient clientWithPrivateCookieStore = new ClickHouseHttpClientBuilder(props).buildClient();
        HttpGet checkCookie = new HttpGet(mockServer.baseUrl() + "/cookie/check");
        clientWithPrivateCookieStore.execute(checkCookie);
        mockServer.verify(getRequestedFor(WireMock.urlEqualTo("/cookie/check")).withoutHeader("cookie"));
        clientWithSharedCookieStore.execute(checkCookie);
        mockServer.verify(getRequestedFor(WireMock.urlEqualTo("/cookie/check")).withHeader("cookie", equalTo(cookie)));
    }

    @Test(dataProvider = "authUserPassword")
    public void testHttpAuthParametersCombination(String authorization, String user,
        String password, String expectedAuthHeader) throws Exception
    {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setHost("localhost");
        props.setPort(mockServer.port());
        props.setUser(user);
        props.setPassword(password);
        props.setHttpAuthorization(authorization);
        CloseableHttpClient client = new ClickHouseHttpClientBuilder(props).buildClient();
        HttpPost post = new HttpPost(mockServer.baseUrl());
        client.execute(post, ClickHouseHttpClientBuilder.createClientContext(props));
        mockServer.verify(
            postRequestedFor(WireMock.anyUrl())
                .withHeader("Authorization", equalTo(expectedAuthHeader)));
    }

    @DataProvider(name = "authUserPassword")
    private static Object[][] provideAuthUserPasswordTestData() {
        return new Object[][] {
            {
                "Digest username=\"foo\"", null, null, "Digest username=\"foo\""
            },
            {
                "Digest username=\"foo\"", "bar", null, "Digest username=\"foo\""
            },
            {
                "Digest username=\"foo\"", null, "baz", "Digest username=\"foo\""
            },
            {
                "Digest username=\"foo\"", "bar", "baz", "Digest username=\"foo\""
            },
            {
                null, "bar", "baz", "Basic YmFyOmJheg==" // bar:baz
            },
            {
                null, "bar", null, "Basic YmFyOg==" // bar:
            },
            {
                null, null, "baz", "Basic ZGVmYXVsdDpiYXo=" // default:baz
            },
        };
    }

    private static WireMockServer newServer() {
        WireMockServer server = new WireMockServer(
            WireMockConfiguration.wireMockConfig().dynamicPort());
        server.start();
        server.stubFor(WireMock.post(WireMock.urlPathMatching("/*"))
                .willReturn(WireMock.aResponse().withStatus(200).withHeader("Connection", "Keep-Alive")
                        .withHeader("Content-Type", "text/plain; charset=UTF-8")
                        .withHeader("Transfer-Encoding", "chunked").withHeader("Keep-Alive", "timeout=3")
                        .withBody("OK.........................").withFixedDelay(2)));
        return server;
    }

    private static void shutDownServerWithDelay(final WireMockServer server, final long delayMs) {
        new Thread() {
            public void run() {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                server.shutdownServer();
                server.stop();
            }
        }.start();
    }

    // @Test(dependsOnMethods = { "testWithRetry" }, expectedExceptions = { NoHttpResponseException.class })
    public void testWithoutRetry() throws Exception {
        final WireMockServer server = newServer();

        ClickHouseProperties props = new ClickHouseProperties();
        props.setMaxRetries(0);
        ClickHouseHttpClientBuilder builder = new ClickHouseHttpClientBuilder(props);
        CloseableHttpClient client = builder.buildClient();
        HttpPost post = new HttpPost("http://localhost:" + server.port() + "/?db=system&query=select%201");

        shutDownServerWithDelay(server, 500);

        try {
            client.execute(post);
        } finally {
            client.close();
        }
    }

    // @Test(expectedExceptions = { HttpHostConnectException.class })
    public void testWithRetry() throws Exception {
        final WireMockServer server = newServer();

        ClickHouseProperties props = new ClickHouseProperties();
        // props.setMaxRetries(3);
        ClickHouseHttpClientBuilder builder = new ClickHouseHttpClientBuilder(props);
        CloseableHttpClient client = builder.buildClient();
        HttpContext context = new BasicHttpContext();
        context.setAttribute("is_idempotent", Boolean.TRUE);
        HttpPost post = new HttpPost("http://localhost:" + server.port() + "/?db=system&query=select%202");
        
        shutDownServerWithDelay(server, 500);

        try {
            client.execute(post, context);
        } finally {
            client.close();
        }
    }
}
