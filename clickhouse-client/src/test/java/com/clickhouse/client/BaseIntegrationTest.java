package com.clickhouse.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Map;

import org.testng.annotations.BeforeTest;

/**
 * Base class for all integration tests.
 */
public abstract class BaseIntegrationTest {
    @BeforeTest(groups = { "integration" })
    public static void setupClickHouseContainer() {
        ClickHouseServerForTest.beforeSuite();
    }

    protected ClickHouseNode getSecureServer(ClickHouseProtocol protocol) {
        return ClickHouseServerForTest.getClickHouseNode(protocol, true, ClickHouseNode.builder().build());
    }

    protected ClickHouseNode getSecureServer(ClickHouseProtocol protocol, ClickHouseNode base) {
        return ClickHouseServerForTest.getClickHouseNode(protocol, true, base);
    }

    protected ClickHouseNode getServer(ClickHouseProtocol protocol) {
        return ClickHouseServerForTest.getClickHouseNode(protocol, false, ClickHouseNode.builder().build());
    }

    protected ClickHouseNode getServer(ClickHouseProtocol protocol, ClickHouseNode base) {
        return ClickHouseServerForTest.getClickHouseNode(protocol, false, base);
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

}
