package com.clickhouse.client.api;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
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

    @DataProvider(name = "retryLogging")
    public static Object[][] retryLogging() {
        return new Object[][] {
                // healthy endpoint after the dead one, maxRetries
                {true, 3},
                {false, 2},
        };
    }

    /**
     * A retried query logs no WARN per attempt: a query that recovers logs one summary naming the type of the
     * last failure, and a query that fails after all attempts logs nothing beyond the bounded endpoint state
     * changes, because its exception reaches the caller. Neither logs the endpoint address or the query ID.
     */
    @Test(dataProvider = "retryLogging")
    public void testRetryLoggedOncePerOperation(boolean recovers, int maxRetries) throws Exception {
        String queryId = "retry-log-query-id-" + System.nanoTime();
        WireMockServer mockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        mockServer.start();
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream originalErr = System.err;
        System.setErr(new PrintStream(captured, true, "UTF-8"));
        try {
            mockServer.stubFor(WireMock.post(WireMock.anyUrl())
                    .willReturn(WireMock.aResponse().withStatus(200).withBody("")));
            Client.Builder builder = new Client.Builder()
                    .addEndpoint("http://127.0.0.1:1") // nothing listens here -> connection refused
                    .setUsername("default")
                    .setPassword("")
                    .setDefaultDatabase("default")
                    .setMaxRetries(maxRetries);
            if (recovers) {
                builder.addEndpoint("http://localhost:" + mockServer.port());
            }
            try (Client client = builder.build()) {
                client.query("SELECT 1", new QuerySettings().setQueryId(queryId)).get(30, TimeUnit.SECONDS).close();
                Assert.assertTrue(recovers, "a query against a dead endpoint must fail after exhausting retries");
            } catch (Exception e) {
                Assert.assertFalse(recovers, "the query must recover on the healthy endpoint: " + e);
            }
        } finally {
            System.err.flush();
            System.setErr(originalErr);
            mockServer.stop();
        }

        String logged = captured.toString("UTF-8");
        List<String> warns = new ArrayList<>();
        for (String line : logged.split("\\R")) {
            if (line.contains(" WARN ")) {
                warns.add(line);
            }
        }
        List<String> summaries = new ArrayList<>();
        int quarantines = 0;
        for (String warn : warns) {
            if (warn.contains("succeeded after")) {
                summaries.add(warn);
            } else if (warn.contains("quarantined")) {
                quarantines++;
            }
        }
        Assert.assertEquals(summaries.size(), recovers ? 1 : 0, "retry summaries:\n" + logged);
        if (recovers) {
            Assert.assertTrue(Pattern.compile("Query succeeded after 1 retry; last retried failure: [\\w$.]+(Exception|Error)")
                    .matcher(summaries.get(0)).find(), "the summary must name the failure type: " + summaries.get(0));
        }
        Assert.assertEquals(quarantines, 1, "the dead endpoint must be reported once per quarantine:\n" + logged);
        Assert.assertTrue(warns.size() <= 3, "a retried query must not log a WARN per attempt:\n" + logged);
        Assert.assertFalse(logged.contains("Retrying"), "per-attempt retry messages must not be logged:\n" + logged);
        Assert.assertFalse(logged.contains(queryId), "the query ID must not be logged:\n" + logged);
        Assert.assertFalse(logged.contains("127.0.0.1:1"), "the endpoint address must not be logged:\n" + logged);
    }
}
