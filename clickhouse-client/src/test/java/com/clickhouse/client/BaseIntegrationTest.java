package com.clickhouse.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;

import org.testng.annotations.BeforeTest;

/**
 * Base class for all integration tests.
 */
public abstract class BaseIntegrationTest {
    @BeforeTest(groups = { "integration" })
    public static void setupClickHouseContainer() {
        ClickHouseServerForTest.beforeSuite();
    }

    protected ClickHouseNode getServer(ClickHouseProtocol protocol) {
        return ClickHouseServerForTest.getClickHouseNode(protocol, ClickHouseNode.builder().build());
    }

    protected ClickHouseNode getServer(ClickHouseProtocol protocol, ClickHouseNode base) {
        return ClickHouseServerForTest.getClickHouseNode(protocol, base);
    }

    protected ClickHouseNode getServer(ClickHouseProtocol protocol, int port) {
        return ClickHouseServerForTest.getClickHouseNode(protocol, port);
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
