package com.clickhouse.client.api;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

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
}
