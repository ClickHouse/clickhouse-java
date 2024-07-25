package com.clickhouse.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Map;

import com.clickhouse.client.config.ClickHouseClientOption;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;

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

    protected boolean createDatabase(String dbName) {
        return ClickHouseServerForTest.runQuery("CREATE DATABASE IF NOT EXISTS " + dbName);
    }
    protected boolean dropDatabase(String dbName) {
        return ClickHouseServerForTest.runQuery("DROP DATABASE IF EXISTS " + dbName);
    }
}
