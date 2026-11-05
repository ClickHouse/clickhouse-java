package com.clickhouse.client;

import com.clickhouse.client.config.ClickHouseClientOption;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Map;

/**
 * Base class for all integration tests.
 */
public abstract class BaseIntegrationTest {
    @BeforeTest(groups = {"integration"})
    public static void setupClickHouseContainer() {
        ClickHouseServerForTest.beforeSuite();
    }

    @AfterTest(groups = {"integration"})
    public static void teardownClickHouseContainer() {
        ClickHouseServerForTest.afterSuite();
    }

    protected ClickHouseNode getSecureServer(ClickHouseProtocol protocol) {
        return ClickHouseServerForTest.getClickHouseNode(protocol, true, ClickHouseNode.builder().addOption(ClickHouseClientOption.SSL.getKey(), "true").build());
    }

    protected ClickHouseNode getSecureServer(ClickHouseProtocol protocol, ClickHouseNode base) {
        return ClickHouseServerForTest.getClickHouseNode(protocol, true, base);
    }

    protected ClickHouseNode getServer(ClickHouseProtocol protocol) {
        return ClickHouseServerForTest.getClickHouseNode(protocol, isCloud(), ClickHouseNode.builder().build());
    }

    protected ClickHouseNode getServer(ClickHouseProtocol protocol, ClickHouseNode base) {
        return ClickHouseServerForTest.getClickHouseNode(protocol, isCloud(), base);
    }

    protected ClickHouseNode getServer(ClickHouseProtocol protocol, int port) {
        return ClickHouseServerForTest.getClickHouseNode(protocol, port);
    }

    protected ClickHouseNode getServer(ClickHouseProtocol protocol, Map<String, String> options) {
        return ClickHouseServerForTest.getClickHouseNode(protocol, options);
    }

    protected String getIpAddress(ClickHouseNode server) {
        String ipAddress = server.getHost();
        try {
            ipAddress = InetAddress.getByName(ipAddress).getHostAddress();
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Not able to resolve %s to get its IP address", server.getHost()), e);
        }
        return ipAddress;
    }

    protected boolean isCloud() {
        return ClickHouseServerForTest.isCloud();
    }

    protected String getConnectionProtocol() {
        if (isCloud()) {
            return "https";
        } else {
            return "http";
        }
    }

    protected String getPassword() {
        return ClickHouseServerForTest.getPassword();
    }

    protected boolean runQuery(String query) {
        return ClickHouseServerForTest.runQuery(query);
    }

    protected static boolean isCluster() {
        return ClickHouseServerForTest.isCluster();
    }

    protected static String onCluster() {
        return ClickHouseServerForTest.isCluster() ? " ON CLUSTER 'test_cluster'" : "";
    }

    /**
     * Waits until each replica of the test cluster returns {@code expectedRows} rows of
     * {@code system.<table>} that match {@code condition}, so that the next request sees a
     * created or changed user, role, settings profile or grant on any replica.
     * Does nothing when the tests do not run on a cluster.
     */
    protected static void waitForAllReplicas(String table, String condition, int expectedRows) {
        ClickHouseServerForTest.waitForAllReplicas(table, condition, expectedRows);
    }

    /**
     * Same check as {@link #waitForAllReplicas(String, String, int)}. {@code query} runs the
     * replica SQL with the caller's client and returns one matching-row count per replica host.
     */
    protected static void waitForAllReplicas(String table, String condition, int expectedRows,
            ClickHouseServerForTest.ReplicaQuery query) {
        ClickHouseServerForTest.waitForAllReplicas(table, condition, expectedRows, query);
    }

    protected boolean createDatabase(String dbName) {
        return ClickHouseServerForTest.runQuery("CREATE DATABASE IF NOT EXISTS `" + dbName + "`" + onCluster());
    }
    protected boolean dropDatabase(String dbName) {
        return ClickHouseServerForTest.runQuery("DROP DATABASE IF EXISTS `" + dbName + "`" + onCluster());
    }
}
