package com.clickhouse.client.api;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ClientFailoverUnitTest {

    @Test
    public void testWireMockFailoverOnly() throws Exception {
        WireMockServer mockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        mockServer.start();
        try {
            mockServer.stubFor(WireMock.post(WireMock.anyUrl())
                    .willReturn(WireMock.aResponse().withStatus(200)
                            .withHeader("Content-Type", "text/plain")
                            .withBody("")));

            try (Client client = new Client.Builder()
                    .addEndpoint("http://127.0.0.1:1") // dead endpoint
                    .addEndpoint("http://localhost:" + mockServer.port()) // healthy mock endpoint
                    .setUsername("default")
                    .setPassword("password")
                    .setDefaultDatabase("default")
                    .setMaxRetries(3)
                    .build()) {

                try (QueryResponse response = client.query("SELECT 1").get(10, TimeUnit.SECONDS)) {
                    Assert.assertNotNull(response);
                }
            }
        } finally {
            mockServer.stop();
        }
    }

    @Test
    public void testRetryWarnNamesExceptionClass() throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream originalErr = System.err;
        System.setErr(new PrintStream(captured, true, "UTF-8"));
        try (Client client = new Client.Builder()
                .addEndpoint("http://127.0.0.1:1") // nothing listens here -> connection refused on every attempt
                .setUsername("default")
                .setPassword("")
                .setDefaultDatabase("default")
                .setMaxRetries(2)
                .build()) {
            try {
                client.query("SELECT 1").get(30, TimeUnit.SECONDS);
                Assert.fail("a query against a dead endpoint should fail after exhausting retries");
            } catch (Exception expected) {
                // every attempt hits the dead endpoint
            }
        } finally {
            System.err.flush();
            System.setErr(originalErr);
        }

        StringBuilder retryWarns = new StringBuilder();
        for (String line : captured.toString("UTF-8").split("\\R")) {
            if (line.contains(" WARN ") && line.contains("Retrying.")) {
                retryWarns.append(line).append('\n');
            }
        }
        String warn = retryWarns.toString();
        Assert.assertFalse(warn.isEmpty(),
                "expected a consolidated retry WARN to be emitted:\n" + captured.toString("UTF-8"));
        // The cause must name the exception class (informative even when getMessage() is null),
        // not just its message.
        Assert.assertTrue(Pattern.compile("cause: [\\w$.]+(Exception|Error):").matcher(warn).find(),
                "retry WARN should name the exception class in the cause, was:\n" + warn);
    }
}
