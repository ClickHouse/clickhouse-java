package com.clickhouse.r2dbc;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;

public abstract class BaseR2dbcTest extends BaseIntegrationTest {
    protected static final String CUSTOM_PROTOCOL_NAME = System.getProperty("protocol", "http").toUpperCase();
    protected static final ClickHouseProtocol DEFAULT_PROTOCOL = ClickHouseProtocol
            .valueOf(CUSTOM_PROTOCOL_NAME.indexOf("HTTP") >= 0 ? "HTTP" : CUSTOM_PROTOCOL_NAME);
    protected static final String EXTRA_PARAM = CUSTOM_PROTOCOL_NAME.indexOf("HTTP") >= 0
            && !"HTTP".equals(CUSTOM_PROTOCOL_NAME) ? "http_connection_provider=" + CUSTOM_PROTOCOL_NAME : "";

    @BeforeAll
    @BeforeClass
    public static void beforeSuite() throws Exception {
        ClickHouseServerForTest.beforeSuite();
    }

    @AfterAll
    @AfterClass
    public static void afterSuite() throws Exception {
        ClickHouseServerForTest.afterSuite();
    }

    protected ConnectionFactory getConnectionFactory(ClickHouseProtocol protocol, String... parameters) {
        StringBuilder builder = new StringBuilder(getServer(protocol).toUri("r2dbc:ch:").toString());
        for (String queryString : parameters) {
            if (queryString != null && !queryString.isEmpty()) {
                if (queryString.charAt(0) != '&') {
                    builder.append('&');
                }
                builder.append(queryString);
            }
        }
        ConnectionFactory connectionFactory = ConnectionFactories.get(builder.toString());
        return connectionFactory;
    }
}
