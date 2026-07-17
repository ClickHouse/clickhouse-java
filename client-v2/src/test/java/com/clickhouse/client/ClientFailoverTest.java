package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ClientFaultCause;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseFormat;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for endpoint failover behavior in client-v2.
 *
 * <p>Verifies that when multiple endpoints are configured and the first
 * endpoint is unreachable, the client automatically fails over to the
 * next available endpoint.</p>
 */
public class ClientFailoverTest extends BaseIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientFailoverTest.class);

    /**
     * Configures a dead endpoint (port 1, nothing listens) as the primary
     * and the actual test server as the backup. Verifies that a query
     * succeeds by failing over to the backup node.
     */
    @Test(groups = {"integration"})
    public void testQueryFailoverToBackupNode() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        try (Client client = new Client.Builder()
                .addEndpoint("http://127.0.0.1:1") 
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .retryOnFailures(ClientFaultCause.ConnectTimeout, ClientFaultCause.NoHttpResponse)
                .setMaxRetries(3)
                .build()) {

            List<GenericRecord> result = client.queryAll("SELECT 1 AS val");
            Assert.assertFalse(result.isEmpty(), "Expected at least one record");
            Assert.assertEquals(result.get(0).getInteger("val"), Integer.valueOf(1), "Query should succeed via failover to the backup node");
        }
    }

    /**
     * Verifies that when all endpoints are healthy, the primary (first)
     * endpoint is consistently used. This tests the "affinity" behavior.
     */
    @Test(groups = {"integration"})
    public void testPrimaryEndpointAffinityWhenHealthy() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        try (Client client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .build()) {

            for (int i = 0; i < 5; i++) {
                List<GenericRecord> result = client.queryAll("SELECT " + i + " AS val");
                Assert.assertFalse(result.isEmpty());
                Assert.assertEquals(result.get(0).getInteger("val"), Integer.valueOf(i));
            }
        }
    }

    /**
     * Verifies that insert operations also failover when the primary
     * endpoint is down.
     */
    @Test(groups = {"integration"})
    public void testInsertFailoverToBackupNode() throws Exception {
        if (isCloud()) {
            return; 
        }

        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();

        try (Client adminClient = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .build()) {
            adminClient.execute("DROP TABLE IF EXISTS failover_insert_test").get(10, TimeUnit.SECONDS).close();
            adminClient.execute("CREATE TABLE failover_insert_test (val UInt32) ENGINE MergeTree ORDER BY ()").get(10, TimeUnit.SECONDS).close();
        }

        try (Client client = new Client.Builder()
                .addEndpoint("http://127.0.0.1:1")  
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .retryOnFailures(ClientFaultCause.ConnectTimeout, ClientFaultCause.NoHttpResponse)
                .setMaxRetries(3)
                .build()) {

            String csvData = "42\n";
            client.insert("failover_insert_test",
                    new ByteArrayInputStream(csvData.getBytes()),
                    ClickHouseFormat.CSV).get(30, TimeUnit.SECONDS).close();
        }

        try (Client verifyClient = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .build()) {
            List<GenericRecord> result = verifyClient.queryAll("SELECT val FROM failover_insert_test");
            Assert.assertFalse(result.isEmpty(), "Expected at least one row after failover insert");
            Assert.assertEquals(result.get(0).getInteger("val"), Integer.valueOf(42));

            verifyClient.execute("DROP TABLE IF EXISTS failover_insert_test").get(10, TimeUnit.SECONDS).close();
        }
    }

    @Test(groups = {"integration"})
    public void testMultipleBackups() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        try (Client client = new Client.Builder()
                .addEndpoint("http://127.0.0.1:1")
                .addEndpoint("http://127.0.0.1:2") 
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure) // working
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .retryOnFailures(ClientFaultCause.ConnectTimeout, ClientFaultCause.NoHttpResponse)
                .setMaxRetries(3)
                .build()) {

            List<GenericRecord> result = client.queryAll("SELECT 1 AS val");
            Assert.assertFalse(result.isEmpty());
            Assert.assertEquals(result.get(0).getInteger("val"), Integer.valueOf(1));
        }
    }

    @Test(groups = {"integration"}, expectedExceptions = ClientException.class)
    public void testAllEndpointsDead() {
        try (Client client = new Client.Builder()
                .addEndpoint("http://127.0.0.1:1")
                .addEndpoint("http://127.0.0.1:2")
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .retryOnFailures(ClientFaultCause.ConnectTimeout, ClientFaultCause.NoHttpResponse)
                .setMaxRetries(3)
                .build()) {

            client.queryAll("SELECT 1 AS val");
        }
    }

    @Test(groups = {"integration"}, expectedExceptions = ClientException.class)
    public void testRetryLimitReached() {
        try (Client client = new Client.Builder()
                .addEndpoint("http://127.0.0.1:1")
                .addEndpoint("http://127.0.0.1:2")
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .retryOnFailures(ClientFaultCause.ConnectTimeout, ClientFaultCause.NoHttpResponse)
                .setMaxRetries(1)
                .build()) {

            client.queryAll("SELECT 1 AS val");
        }
    }

    @Test(groups = {"integration"})
    public void testDuplicateEndpointRegistrationAndOrderPreservation() throws Exception {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();

        String uriA = "http://127.0.0.1:1";
        String uriB = "http://127.0.0.1:2";

        try (Client client = new Client.Builder()
                .addEndpoint(uriA)
                .addEndpoint(uriA)
                .addEndpoint(uriB)
                .addEndpoint(uriB) 
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .build()) {
                    
            java.lang.reflect.Field field = Client.class.getDeclaredField("endpoints");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            List<com.clickhouse.client.api.transport.Endpoint> endpoints = 
                    (List<com.clickhouse.client.api.transport.Endpoint>) field.get(client);

            Assert.assertEquals(endpoints.size(), 2, "Should only have unique endpoints");
            Assert.assertEquals(endpoints.get(0).getURI().toString(), uriA + "/", "First endpoint should be " + uriA + "/");
            Assert.assertEquals(endpoints.get(1).getURI().toString(), uriB + "/", "Second endpoint should be " + uriB + "/");
        }
    }

    @Test(groups = {"integration"}, expectedExceptions = IllegalArgumentException.class)
    public void testNoEndpointsConfigured() {
        new Client.Builder()
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .build();
    }

    @Test(groups = {"integration"})
    public void testHTTP503Failover() throws Exception {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();

        WireMockServer mockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        mockServer.start();
        try {
            mockServer.stubFor(WireMock.post(WireMock.anyUrl())
                    .willReturn(WireMock.aResponse().withStatus(503).withBody("Service Unavailable")));

            try (Client client = new Client.Builder()
                    .addEndpoint("http://localhost:" + mockServer.port())
                    .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                    .setUsername("default")
                    .setPassword(ClickHouseServerForTest.getPassword())
                    .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                    .setMaxRetries(3)
                    .build()) {

                List<GenericRecord> result = client.queryAll("SELECT 1 AS val");
                Assert.assertFalse(result.isEmpty(), "Expected at least one record");
                Assert.assertEquals(result.get(0).getInteger("val"), Integer.valueOf(1),
                        "Query should succeed via failover after 503");
            }
        } finally {
            mockServer.stop();
        }
    }
}
