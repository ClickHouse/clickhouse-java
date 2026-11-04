package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.enums.Protocol;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.apache.hc.core5.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

public class HttpWireLoggingTest {
    private static final String USERNAME = "wire-user-canary";
    private static final String PASSWORD = "wire-password-canary";
    private static final String BASIC_TOKEN = Base64.getEncoder().encodeToString(
            (USERNAME + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8));
    private static final String BEARER_TOKEN = "wire-bearer-token-canary";
    private static final String CUSTOM_HEADER = "X-Sensitive-Canary";
    private static final String CUSTOM_VALUE = "wire-custom-header-canary";
    private static final String RESPONSE_VALUE = "wire-response-canary";

    @DataProvider(name = "connectionManagersAndAuthentication")
    public Object[][] connectionManagersAndAuthentication() {
        return new Object[][] {
                {true, false},
                {true, true},
                {false, false},
                {false, true}
        };
    }

    @Test(dataProvider = "connectionManagersAndAuthentication")
    public void testSensitiveValuesAreNotWrittenToWireLog(boolean pooled, boolean bearer) throws Exception {
        WireMockServer server = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        server.start();
        try {
            String expectedAuthorization = bearer
                    ? "Bearer " + BEARER_TOKEN
                    : "Basic " + BASIC_TOKEN;
            server.stubFor(WireMock.post(WireMock.anyUrl())
                    .withHeader(HttpHeaders.AUTHORIZATION, WireMock.equalTo(expectedAuthorization))
                    .withHeader(CUSTOM_HEADER, WireMock.equalTo(CUSTOM_VALUE))
                    .willReturn(WireMock.aResponse()
                            .withStatus(200)
                            .withBody(RESPONSE_VALUE)));

            Assert.assertTrue(LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).isDebugEnabled());
            Assert.assertTrue(LoggerFactory.getLogger("org.apache.hc.client5.http.wire").isDebugEnabled());
            String logs = captureStdErr(() -> {
                Client.Builder builder = new Client.Builder()
                        .addEndpoint(Protocol.HTTP, "localhost", server.port(), false)
                        .setOption(ClientConfigProperties.CONNECTION_POOL_ENABLED.getKey(), String.valueOf(pooled))
                        .compressServerResponse(false)
                        .httpHeader(CUSTOM_HEADER, CUSTOM_VALUE);
                if (bearer) {
                    builder.useBearerTokenAuth(BEARER_TOKEN);
                } else {
                    builder.setUsername(USERNAME).setPassword(PASSWORD);
                }
                try (Client client = builder.build()) {
                    client.execute("SELECT 1").get(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            server.verify(WireMock.postRequestedFor(WireMock.anyUrl()));
            Assert.assertFalse(logs.contains("org.apache.hc.client5.http.wire"), logs);
            Assert.assertFalse(logs.contains(USERNAME), logs);
            Assert.assertFalse(logs.contains(PASSWORD), logs);
            Assert.assertFalse(logs.contains(BASIC_TOKEN), logs);
            Assert.assertFalse(logs.contains(BEARER_TOKEN), logs);
            Assert.assertFalse(logs.contains(expectedAuthorization), logs);
            Assert.assertFalse(logs.contains(CUSTOM_VALUE), logs);
            Assert.assertFalse(logs.contains(RESPONSE_VALUE), logs);
            Assert.assertTrue(logs.contains("client compression:"), logs);
            Assert.assertTrue(logs.contains(pooled
                    ? "PoolingHttpClientConnectionManager"
                    : "BasicHttpClientConnectionManager"), logs);
            Assert.assertTrue(LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).isDebugEnabled());
            Assert.assertTrue(LoggerFactory.getLogger("org.apache.hc.client5.http.wire").isDebugEnabled());
        } finally {
            server.stop();
        }
    }

    private static synchronized String captureStdErr(Runnable action) {
        PrintStream original = System.err;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream capture = new PrintStream(buffer, true);
        System.setErr(capture);
        try {
            action.run();
        } finally {
            capture.flush();
            System.setErr(original);
        }
        return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
    }
}
