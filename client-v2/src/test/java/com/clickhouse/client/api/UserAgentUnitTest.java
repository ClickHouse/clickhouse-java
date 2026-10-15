package com.clickhouse.client.api;

import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import org.apache.hc.core5.http.HttpHeaders;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class UserAgentUnitTest {

    private WireMockServer mockServer;

    @BeforeMethod
    public void setUp() {
        mockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        mockServer.start();
        mockServer.stubFor(WireMock.post(WireMock.anyUrl())
                .willReturn(WireMock.aResponse().withStatus(200)));
    }

    @AfterMethod
    public void tearDown() {
        if (mockServer != null) {
            mockServer.stop();
        }
    }

    @Test(dataProvider = "userAgentData")
    public void testUserAgentHeader(Consumer<Client.Builder> configure, String expectedUserAgent) throws Exception {
        Client.Builder builder = newClientBuilder();
        configure.accept(builder);
        try (Client client = builder.build()) {
            assertUserAgent(client, null, expectedUserAgent);
        }
    }

    @DataProvider(name = "userAgentData")
    public static Object[][] userAgentData() {
        Map<String, String> flinkMetadata = new LinkedHashMap<>();
        flinkMetadata.put("fv", "flink/2.0.0");
        flinkMetadata.put("api", "table");

        Map<String, String> withEmptyValue = new LinkedHashMap<>();
        withEmptyValue.put("api", "");
        withEmptyValue.put("fv", "flink/2.0.0");

        return new Object[][]{
                {(Consumer<Client.Builder>) b -> { }, userAgent(null, null)},
                {(Consumer<Client.Builder>) b -> b.setClientName("Flink-ClickHouse-Sink/0.2.0")
                        .setClientMetadata(flinkMetadata),
                        userAgent("Flink-ClickHouse-Sink/0.2.0", "fv:flink/2.0.0; api:table")},
                {(Consumer<Client.Builder>) b -> b.setOption(ClientConfigProperties.CLIENT_METADATA.getKey(),
                        "api=table, fv=flink/2.0.0"),
                        userAgent(null, "api:table; fv:flink/2.0.0")},
                {(Consumer<Client.Builder>) b -> b.setClientMetadata(Collections.singletonMap("build", "a=b,c\\d")),
                        userAgent(null, "build:a=b,c\\d")},
                {(Consumer<Client.Builder>) b -> b.setClientMetadata(withEmptyValue),
                        userAgent(null, "fv:flink/2.0.0")},
                {(Consumer<Client.Builder>) b -> b.setClientMetadata(flinkMetadata).setClientMetadata(null),
                        userAgent(null, null)},
                {(Consumer<Client.Builder>) b -> b.setClientName("my-app/1.0")
                        .httpHeader(HttpHeaders.USER_AGENT, "custom-agent/2.0")
                        .setClientMetadata(Collections.singletonMap("api", "table")),
                        userAgent("custom-agent/2.0", "api:table")},
        };
    }

    @Test
    public void testClientMetadataKeptWhenApplicationPartChanges() throws Exception {
        try (Client client = newClientBuilder()
                .setClientName("my-app/1.0")
                .setClientMetadata(Collections.singletonMap("api", "table"))
                .build()) {
            client.updateClientName("new-app/2.0");
            assertUserAgent(client, null, userAgent("new-app/2.0", "api:table"));
            assertUserAgent(client, new QuerySettings().httpHeader(HttpHeaders.USER_AGENT, "op-agent/3.0"),
                    userAgent("op-agent/3.0", "api:table"));
            assertUserAgent(client, (QuerySettings) new QuerySettings()
                            .setOption(ClientConfigProperties.CLIENT_METADATA.getKey(), "fv=flink/2.0.0"),
                    userAgent("new-app/2.0", "api:table"));
        }
    }

    @Test
    public void testInvalidClientMetadataRejectedOnBuild() {
        Client.Builder builder = newClientBuilder()
                .setClientMetadata(Collections.singletonMap("api", "table; os:other"));
        Assert.expectThrows(ClientMisconfigurationException.class, builder::build);
    }

    private Client.Builder newClientBuilder() {
        return new Client.Builder()
                .addEndpoint("http://localhost:" + mockServer.port())
                .setUsername("default")
                .setPassword("");
    }

    private void assertUserAgent(Client client, QuerySettings settings, String expectedUserAgent) throws Exception {
        mockServer.resetRequests();
        try (QueryResponse response = client.query("SELECT 1", settings).get(10, TimeUnit.SECONDS)) {
            Assert.assertNotNull(response);
        }
        List<LoggedRequest> requests = mockServer.findAll(WireMock.postRequestedFor(WireMock.anyUrl()));
        Assert.assertFalse(requests.isEmpty(), "no request was sent");
        for (LoggedRequest request : requests) {
            Assert.assertEquals(request.getHeader(HttpHeaders.USER_AGENT), expectedUserAgent);
        }
    }

    private static String userAgent(String application, String metadata) {
        return (application == null ? "" : application + " ")
                + "clickhouse-java-v2/" + Client.clientVersion
                + " (" + (metadata == null ? "" : metadata + "; ")
                + "lv:java/" + System.getProperty("java.version")
                + "; os:" + System.getProperty("os.name") + ")";
    }
}
