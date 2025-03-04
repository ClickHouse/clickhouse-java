package com.clickhouse.jdbc.perf;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.jdbc.ClickHouseDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static com.clickhouse.client.ClickHouseServerForTest.isCloud;

public class JDBCBenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCBenchmarkBase.class);
    protected static Connection jdbcV1 = null;
    protected static Connection jdbcV2 = null;

    public void setup() throws SQLException {
        BaseIntegrationTest.setupClickHouseContainer();
        // TODO: create two connections
        Properties properties = new Properties();
        properties.put("user", "default");
        properties.put("password", "test_default_password");

        ClickHouseNode node = ClickHouseServerForTest.getClickHouseNode(ClickHouseProtocol.HTTP, isCloud(), ClickHouseNode.builder().build());

        LOGGER.info(String.format("clickhouse endpoint [%s:%s]", node.getHost(), node.getPort()));
        jdbcV1 = new ClickHouseDriver().connect(String.format("jdbc:clickhouse://%s:%s?clickhouse.jdbc.v1=true", node.getHost(), node.getPort()), properties);
        jdbcV2 = new ClickHouseDriver().connect(String.format("jdbc:clickhouse://%s:%s", node.getHost(), node.getPort()), properties);

    }

    public void tearDown() throws SQLException {
        LOGGER.info("Tearing down JDBC connections and ClickHouse container.");
        if (jdbcV1 != null) jdbcV1.close();
        if (jdbcV2 != null) jdbcV2.close();
        BaseIntegrationTest.teardownClickHouseContainer();
    }

}
