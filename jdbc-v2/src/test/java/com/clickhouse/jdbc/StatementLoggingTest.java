package com.clickhouse.jdbc;

import com.clickhouse.client.api.ClientConfigProperties;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.slf4j.spi.LocationAwareLogger;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

/**
 * With every driver and client logger at TRACE, executing a statement logs neither its SQL nor its bound values,
 * whether the server accepts it or returns an error that echoes it. The server is mocked: no user is created.
 */
public class StatementLoggingTest {

    private static final String CANARY_PASSWORD = "canary-password-5e2a";
    private static final String CANARY_VALUE = "canary-value-8b1c";
    private static final String CANARY_ERROR = "canary-error-3d9f";
    private static final String CANARY_QUERY_ID = "canary-query-id-7c4e";

    private static final List<String> LOGGERS = Arrays.asList(
            "com.clickhouse.jdbc.ConnectionImpl",
            "com.clickhouse.jdbc.PreparedStatementImpl",
            "com.clickhouse.jdbc.StatementImpl",
            "com.clickhouse.jdbc.internal.SqlParserFacade",
            "com.clickhouse.jdbc.internal.parser.javacc.ClickHouseSqlParser",
            "com.clickhouse.client.api.Client",
            "com.clickhouse.client.api.internal.HttpAPIClientHelper",
            "com.clickhouse.client.api.transport.ClientNodeSelector");

    @DataProvider
    public static Object[][] statementLoggingDP() {
        return new Object[][] {
                // SQL parser, prepared statement, server error
                {"JAVACC", false, false},
                {"JAVACC", true, false},
                {"ANTLR4", false, true},
                {"ANTLR4_PARAMS_PARSER", true, true},
        };
    }

    @Test(groups = {"unit"}, dataProvider = "statementLoggingDP")
    public void testStatementLogsNoSqlOrValues(String parser, boolean prepared, boolean serverError) throws Exception {
        WireMockServer server = new WireMockServer(WireMockConfiguration.options().dynamicPort()
                .notifier(new ConsoleNotifier(false)));
        server.start();
        try {
            server.stubFor(WireMock.post(WireMock.anyUrl()).willReturn(serverError
                    ? WireMock.serverError().withHeader("X-ClickHouse-Exception-Code", "62")
                            .withBody("Code: 62. DB::Exception: " + CANARY_ERROR + " near '" + CANARY_PASSWORD + "'")
                    : WireMock.ok()));

            Properties properties = new Properties();
            properties.setProperty(ClientConfigProperties.COMPRESS_SERVER_RESPONSE.getKey(), "false");
            properties.setProperty(ClientConfigProperties.SERVER_TIMEZONE.getKey(), "UTC"); // no server info query
            properties.setProperty(DriverProperties.SQL_PARSER.getKey(), parser);
            properties.put(DriverProperties.QUERY_ID_GENERATOR.getKey(),
                    (java.util.function.Supplier<String>) () -> CANARY_QUERY_ID);

            String logged = LoggingTestUtils.captureLogs(LOGGERS, LocationAwareLogger.TRACE_INT, () -> {
                try (Connection conn = new ConnectionImpl("jdbc:clickhouse://localhost:" + server.port() + "/default",
                        properties)) {
                    if (serverError) {
                        SQLException e = expectThrows(SQLException.class, () -> execute(conn, prepared));
                        assertTrue(e.getMessage().contains(CANARY_ERROR), "the server error must reach the caller");
                    } else {
                        execute(conn, prepared);
                    }
                }
            });

            server.verify(WireMock.postRequestedFor(WireMock.anyUrl())
                    .withRequestBody(WireMock.containing(prepared ? CANARY_VALUE : CANARY_PASSWORD)));
            for (String canary : Arrays.asList(CANARY_PASSWORD, CANARY_VALUE, CANARY_ERROR, CANARY_QUERY_ID,
                    "canary_user")) {
                assertFalse(logged.contains(canary), "'" + canary + "' must not be logged: " + logged);
            }
        } finally {
            server.stop();
        }
    }

    private static void execute(Connection conn, boolean prepared) throws SQLException {
        if (prepared) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "ALTER USER canary_user IDENTIFIED BY 'x' SETTINGS custom_tag = ?")) {
                ps.setString(1, CANARY_VALUE);
                ps.executeUpdate();
            }
        } else {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE USER IF NOT EXISTS canary_user IDENTIFIED BY '" + CANARY_PASSWORD + "'");
            }
        }
    }
}
