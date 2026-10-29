package com.clickhouse.jdbc;

import com.clickhouse.jdbc.internal.ExceptionUtils;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Executor;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class ConnectionNetworkTimeoutTest {
    @Test(timeOut = 15000)
    public void testRestoringNetworkTimeoutPreservesConfiguredSocketTimeout() throws Exception {
        WireMockServer server = new WireMockServer(WireMockConfiguration.options()
                .bindAddress("127.0.0.1").dynamicPort());
        server.start();
        try {
            // RowBinaryWithNamesAndTypes: one UInt8 column named "1", containing one row with value 1.
            server.stubFor(WireMock.post(WireMock.anyUrl())
                    .withRequestBody(WireMock.equalTo("SELECT 1"))
                    .willReturn(WireMock.aResponse().withFixedDelay(1800)
                            .withHeader("X-ClickHouse-Format", "RowBinaryWithNamesAndTypes")
                            .withHeader("X-ClickHouse-Summary", "{\"read_rows\":\"1\",\"read_bytes\":\"1\"}")
                            .withBody(new byte[] {1, 1, '1', 5, 'U', 'I', 'n', 't', '8', 1})));

            Properties properties = new Properties();
            properties.setProperty("socket_timeout", "500");
            properties.setProperty("server_time_zone", "UTC");
            properties.setProperty("user", "default");
            properties.setProperty("password", "local-test");
            properties.setProperty("retry", "0");
            properties.setProperty("compress", "false");
            try (Connection connection = new ConnectionImpl(
                    "jdbc:clickhouse://127.0.0.1:" + server.port() + "/default", properties)) {
                Executor executor = Runnable::run;
                int originalTimeout = connection.getNetworkTimeout();
                connection.setNetworkTimeout(executor, 5000);
                connection.setNetworkTimeout(executor, originalTimeout);

                try (Statement statement = connection.createStatement()) {
                    SQLException error = expectThrows(SQLException.class, () -> {
                        try (ResultSet result = statement.executeQuery("SELECT 1")) {
                            result.next();
                        }
                    });
                    assertTrue(ExceptionUtils.getRootCause(error) instanceof SocketTimeoutException,
                            "Expected a socket timeout, got: " + error);
                }
            }
        } finally {
            server.stop();
        }
    }

    @DataProvider
    public static Object[][] configuredSocketTimeouts() {
        return new Object[][] {
                {"500", 500},
                {"0", 0},
                {null, 0},
        };
    }

    @Test(dataProvider = "configuredSocketTimeouts")
    public void testGetNetworkTimeoutReportsEffectiveSocketTimeout(String socketTimeout, int expected) throws Exception {
        try (Connection connection = new ConnectionImpl("jdbc:clickhouse://127.0.0.1:1/default",
                connectionProperties(socketTimeout))) {
            Executor executor = Runnable::run;
            int originalTimeout = connection.getNetworkTimeout();
            assertEquals(originalTimeout, expected);

            connection.setNetworkTimeout(executor, 5000);
            assertEquals(connection.getNetworkTimeout(), 5000);

            connection.setNetworkTimeout(executor, originalTimeout);
            assertEquals(connection.getNetworkTimeout(), expected);
        }
    }

    @Test(timeOut = 15000)
    public void testZeroNetworkTimeoutDisablesConfiguredSocketTimeout() throws Exception {
        WireMockServer server = new WireMockServer(WireMockConfiguration.options()
                .bindAddress("127.0.0.1").dynamicPort());
        server.start();
        try {
            server.stubFor(WireMock.post(WireMock.anyUrl())
                    .withRequestBody(WireMock.equalTo("SELECT 1"))
                    .willReturn(WireMock.aResponse().withFixedDelay(1800)
                            .withHeader("X-ClickHouse-Format", "RowBinaryWithNamesAndTypes")
                            .withHeader("X-ClickHouse-Summary", "{\"read_rows\":\"1\",\"read_bytes\":\"1\"}")
                            .withBody(new byte[] {1, 1, '1', 5, 'U', 'I', 'n', 't', '8', 1})));
            try (Connection connection = new ConnectionImpl(
                    "jdbc:clickhouse://127.0.0.1:" + server.port() + "/default", connectionProperties("500"))) {
                connection.setNetworkTimeout(Runnable::run, 0);
                assertEquals(connection.getNetworkTimeout(), 0);

                try (Statement statement = connection.createStatement();
                     ResultSet result = statement.executeQuery("SELECT 1")) {
                    assertTrue(result.next());
                    assertEquals(result.getInt(1), 1);
                }
            }
        } finally {
            server.stop();
        }
    }

    private static Properties connectionProperties(String socketTimeout) {
        Properties properties = new Properties();
        if (socketTimeout != null) {
            properties.setProperty("socket_timeout", socketTimeout);
        }
        properties.setProperty("server_time_zone", "UTC");
        properties.setProperty("user", "default");
        properties.setProperty("password", "local-test");
        properties.setProperty("retry", "0");
        properties.setProperty("compress", "false");
        return properties;
    }
}
