package com.clickhouse.examples.jdbc;

import java.io.ByteArrayInputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseRequestManager;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.SqlExceptionUtils;

public class Advanced {
    private static Connection getConnection(String url, Properties props) throws SQLException {
        Connection conn = DriverManager.getConnection(url, props);
        System.out.println("Connected to: " + conn.getMetaData().getURL());
        return conn;
    }

    private static Connection getConnection(String url) throws SQLException {
        return getConnection(url, new Properties());
    }

    private static ByteArrayInputStream newInputStream(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.US_ASCII));
    }

    static String customQueryId(String url) throws SQLException {
        String sql = "select 1";
        String queryId = "my-query-id";
        String result = "";
        try (Connection conn = getConnection(url); Statement stmt = conn.createStatement()) {
            stmt.unwrap(ClickHouseRequest.class).manager(new ClickHouseRequestManager() {
                private final AtomicInteger id = new AtomicInteger(0);

                @Override
                public String createQueryId() {
                    return "my-query-" + id.incrementAndGet();
                }
            });
            try (ClickHouseResponse resp = stmt.unwrap(ClickHouseRequest.class).query(sql, queryId).executeAndWait()) {
                result = resp.firstRecord().getValue(1).asString();
            } catch (ClickHouseException | UncheckedIOException e) {
                throw SqlExceptionUtils.handle(e);
            }
        }
        return result;
    }

    static String exteralTables(String url) throws SQLException {
        String sql = "select a.name as n1, b.name as n2 from {tt 'table1'} a inner join {tt 'table2'} b on a.id=b.id";
        try (Connection conn = getConnection(url); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1,
                    ClickHouseExternalTable.builder().name("table1").columns("id Int32, name Nullable(String)")
                            .format(ClickHouseFormat.CSV)
                            .content(newInputStream("1,a\n2,b")).build());
            ps.setObject(2,
                    ClickHouseExternalTable.builder().name("table2").columns("id Int32, name String")
                            .format(ClickHouseFormat.JSONEachRow)
                            .content(newInputStream("{\"id\":3,\"name\":\"c\"}\n{\"id\":1,\"name\":\"d\"}")).build());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("Should have at least one record");
                }

                // n1=a, n2=d
                return String.format("n1=%s, n2=%s", rs.getString(1), rs.getString(2));
            }
        }
    }

    static String manualTransaction(String url) throws SQLException {
        Properties props = new Properties();
        // props.setProperty(JdbcConfig.PROP_AUTO_COMMIT, "false");
        props.setProperty("autoCommit", "false");
        // props.setProperty(JdbcConfig.PROP_TX_SUPPORT, "true");
        props.setProperty("transactionSupport", "true");
        try (Connection conn = getConnection(url, props)) {
            if (!((ClickHouseConnection) conn).isTransactionSupported()) {
                System.out.println("Re-establishing connection until transaction is supported...");
                return manualTransaction(url);
            }

            conn.commit();
            return "Transaction committed!";
        }
    }

    static String namedParameter(String url) throws SQLException {
        Properties props = new Properties();
        // props.setProperty(JdbcConfig.PROP_NAMED_PARAM, "true");
        props.setProperty("namedParameter", "true");
        // two parameters:
        // * a - String
        // * b - DateTime64(3)
        String sql = "select :a as a1, :a(String) as a2, :b(DateTime64(3)) as b";
        try (Connection conn = getConnection(url, props); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, "a");
            ps.setObject(2, LocalDateTime.of(2022, 1, 7, 22, 48, 17, 123000000));

            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("Should have at least one record");
                }

                // a1=a, a2=a, b=2022-01-07 22:48:17.123
                return String.format("a1=%s, a2=%s, b=%s", rs.getString(1), rs.getString(2), rs.getString("B"));
            }
        }
    }

    static String renameResponseColumn(String url) throws SQLException {
        Properties props = new Properties();
        // props.setProperty(ClickHouseClientOption.RENAME_RESPONSE_COLUMN.getKey(),
        // ClickHouseRenameMethod.TO_CAMELCASE_WITHOUT_PREFIX.name());
        props.setProperty("rename_response_column", "TO_CAMELCASE_WITHOUT_PREFIX");

        String sql = "SELECT c.`simple.string_value`, n.number\n"
                + "FROM (SELECT 1 number, 'string' `simple.string_value`) c\n"
                + "INNER JOIN (SELECT 1 number) n ON n.number = c.number";
        try (Connection conn = getConnection(url, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            String firstColumn = rs.getMetaData().getColumnName(1);
            String secondColumn = rs.getMetaData().getColumnName(2);

            if (!rs.next()) {
                throw new IllegalStateException("Should have at least one record");
            }

            // return "stringValue=string, number=1"
            // instead of "simple.string_value=string, n.number=1",
            return String.format("%s=%s, %s=%s", firstColumn, rs.getString(1), secondColumn, rs.getInt(2));
        }
    }

    static String unwrapToUseClientApi(String url) throws SQLException {
        String sql = "select 1 n union all select 2 n";
        try (Connection conn = getConnection(url); Statement stmt = conn.createStatement()) {
            ClickHouseRequest<?> request = stmt.unwrap(ClickHouseRequest.class);
            // server setting is not allowed in read-only mode
            if (!conn.isReadOnly()) {
                // not required for ClickHouse 22.7+, only works for HTTP protocol
                request.set("send_progress_in_http_headers", 1);
            }
            try (ClickHouseResponse response = request.query(sql).executeAndWait()) {
                int count = 0;
                // may throw UncheckedIOException (due to restriction of Iterable interface)
                for (ClickHouseRecord r : response.records()) {
                    count++;
                }
                return String.format("Result Rows: %d (read bytes: %d)", count, response.getSummary().getReadBytes());
            } catch (ClickHouseException e) {
                throw SqlExceptionUtils.handle(e);
            }
        }
    }

    public static void main(String[] args) {
        // randomly pick one of the two endpoints to connect to,
        // fail over to the other when there's connection issue
        String url = System.getProperty("chUrl",
                "jdbc:ch://(https://explorer@play.clickhouse.com:443),"
                        + "(https://demo:demo@github.demo.trial.altinity.cloud)"
                        + "/default?failover=1&load_balancing_policy=random");

        try {
            System.out.println(exteralTables(url));
            System.out.println(namedParameter(url));
            System.out.println(renameResponseColumn(url));
            System.out.println(unwrapToUseClientApi(url));

            // requires ClickHouse 22.6+ with transaction enabled
            System.out.println(manualTransaction(url));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
