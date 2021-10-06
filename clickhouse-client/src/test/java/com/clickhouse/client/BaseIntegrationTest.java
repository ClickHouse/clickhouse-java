package com.clickhouse.client;

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
}
