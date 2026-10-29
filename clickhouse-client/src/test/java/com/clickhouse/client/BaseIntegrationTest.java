package com.clickhouse.client;

import com.clickhouse.client.config.ClickHouseClientOption;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Map;

/**
 * Base class for all integration tests.
 */
public abstract class BaseIntegrationTest {
    private static final int REPLICA_CHECK_ATTEMPTS = 100;
    private static final long REPLICA_CHECK_INTERVAL_MS = 100L;

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
        if (!isCluster()) {
            return;
        }
        String sql = "SELECT hostName() AS host, countIf(" + condition + ") AS matching_rows FROM clusterAllReplicas('"
                + ClusterClickHouseContainerStarter.CLUSTER_NAME + "', system." + table + ") GROUP BY host";
        String state = "";
        for (int attempt = 1; attempt <= REPLICA_CHECK_ATTEMPTS; attempt++) {
            try {
                state = ClickHouseServerForTest.queryForResult(sql).trim();
                if (hasRowsOnAllReplicas(state, expectedRows)) {
                    return;
                }
            } catch (IOException e) {
                state = e.getMessage();
            }
            try {
                Thread.sleep(REPLICA_CHECK_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        Assert.fail("Expected " + expectedRows + " rows matching [" + condition + "] in system." + table + " on all "
                + ClusterClickHouseContainerStarter.NODE_COUNT + " replicas, but got [host, rows]:\n" + state);
    }

    private static boolean hasRowsOnAllReplicas(String result, int expectedRows) {
        String[] lines = result.split("\n");
        if (lines.length != ClusterClickHouseContainerStarter.NODE_COUNT) {
            return false;
        }
        for (String line : lines) {
            String[] columns = line.split("\t");
            if (columns.length != 2 || !columns[1].equals(String.valueOf(expectedRows))) {
                return false;
            }
        }
        return true;
    }

    protected boolean createDatabase(String dbName) {
        return ClickHouseServerForTest.runQuery("CREATE DATABASE IF NOT EXISTS `" + dbName + "`" + onCluster());
    }
    protected boolean dropDatabase(String dbName) {
        return ClickHouseServerForTest.runQuery("DROP DATABASE IF EXISTS `" + dbName + "`" + onCluster());
    }
}
