package com.clickhouse.jdbc.comparison;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.jdbc.ConnectionImpl;
import com.clickhouse.jdbc.JdbcIntegrationTest;
import com.clickhouse.jdbc.internal.ClickHouseConnectionImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class DateTimeComparisonTest extends JdbcIntegrationTest {
    public Connection getJdbcConnectionV1(Properties properties) throws SQLException {
        Properties info = new Properties();
        info.setProperty("user", "default");
        info.setProperty("password", ClickHouseServerForTest.getPassword());
        info.setProperty("database", ClickHouseServerForTest.getDatabase());

        if (properties != null) {
            info.putAll(properties);
        }

        return new ClickHouseConnectionImpl(getJDBCEndpointString(), info);
    }

    public Connection getJdbcConnectionV2(Properties properties) throws SQLException {
        Properties info = new Properties();
        info.setProperty("user", "default");
        info.setProperty("password", ClickHouseServerForTest.getPassword());
        info.setProperty("database", ClickHouseServerForTest.getDatabase());

        if (properties != null) {
            info.putAll(properties);
        }

        info.setProperty(ClientConfigProperties.DATABASE.getKey(), ClickHouseServerForTest.getDatabase());

        return new ConnectionImpl(getJDBCEndpointString(), info);
    }

    public String getJDBCEndpointString() {
        return "jdbc:ch:" + (isCloud() ? "" : "http://") +
                ClickHouseServerForTest.getClickHouseAddress(ClickHouseProtocol.HTTP, false) + "/" + (isCloud() ? ClickHouseServerForTest.getDatabase() : "");
    }

    private void run(String query) throws SQLException {
        try (Connection connection = getJdbcConnectionV2(null)) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("CREATE DATABASE IF NOT EXISTS " + ClickHouseServerForTest.getDatabase());
                stmt.execute(query);
            }
        }
    }

    @Test (groups = "integration", enabled = true)
    public void setDateTest() throws SQLException {
        run("DROP TABLE IF EXISTS test_date");
        run("CREATE TABLE IF NOT EXISTS test_date (id Int8, d1 Date, d2 Date, d3 Date) ENGINE = MergeTree ORDER BY id");

        try (Connection connV1 = getJdbcConnectionV1(null)) {
            try (PreparedStatement stmtV1 = connV1.prepareStatement("INSERT INTO test_date VALUES (1, ?, ?, ?)")) {//INSERT with V1
                stmtV1.setDate(1, java.sql.Date.valueOf("2021-01-01"));
                stmtV1.setDate(2, java.sql.Date.valueOf("2021-01-01"), new GregorianCalendar());
                stmtV1.setDate(3, java.sql.Date.valueOf("2021-01-01"), new GregorianCalendar(TimeZone.getTimeZone("UTC")));
                stmtV1.execute();
            }
        }

        try (Connection connV1 = getJdbcConnectionV1(null);
             Connection connV2 = getJdbcConnectionV2(null)) {
            try (Statement stmtV1 = connV1.createStatement();
                 Statement stmtV2 = connV2.createStatement()) {
                ResultSet rsV1 = stmtV1.executeQuery("SELECT * FROM test_date");
                ResultSet rsV2 = stmtV2.executeQuery("SELECT * FROM test_date");
                assertTrue(rsV1.next());
                assertTrue(rsV2.next());

                assertEquals(rsV2.getDate(2), rsV1.getDate(2));
                assertEquals(rsV2.getDate(3, new GregorianCalendar()), rsV1.getDate(3, new GregorianCalendar()));
                assertEquals(rsV2.getDate(4, new GregorianCalendar(TimeZone.getTimeZone("UTC"))),
                        rsV1.getDate(4, new GregorianCalendar(TimeZone.getTimeZone("UTC"))));
            }
        }
    }


    @Test (groups = "integration", enabled = true)
    public void setTimeTest() throws SQLException {
        run("DROP TABLE IF EXISTS test_time");
        run("CREATE TABLE IF NOT EXISTS test_time (id Int8, t1 Datetime, t2 Datetime, t3 Datetime) ENGINE = MergeTree ORDER BY id");

        try (Connection connV1 = getJdbcConnectionV1(null)) {
            try (PreparedStatement stmtV1 = connV1.prepareStatement("INSERT INTO test_time VALUES (1, ?, ?, ?)")) {//INSERT with V1
                stmtV1.setTime(1, java.sql.Time.valueOf("12:34:56"));
                stmtV1.setTime(2, java.sql.Time.valueOf("12:34:56"), new GregorianCalendar());
                stmtV1.setTime(3, java.sql.Time.valueOf("12:34:56"), new GregorianCalendar(TimeZone.getTimeZone("UTC")));
                stmtV1.execute();
            }
        }

        try (Connection connV1 = getJdbcConnectionV1(null);
             Connection connV2 = getJdbcConnectionV2(null)) {
            try (Statement stmtV1 = connV1.createStatement();
                 Statement stmtV2 = connV2.createStatement()) {
                ResultSet rsV1 = stmtV1.executeQuery("SELECT * FROM test_time");
                ResultSet rsV2 = stmtV2.executeQuery("SELECT * FROM test_time");
                assertTrue(rsV1.next());
                assertTrue(rsV2.next());

                assertEquals(rsV2.getTime(2), rsV1.getTime(2));
                assertEquals(rsV2.getTime(3, new GregorianCalendar()), rsV1.getTime(3, new GregorianCalendar()));
                assertEquals(rsV2.getTime(4, new GregorianCalendar(TimeZone.getTimeZone("UTC"))),
                        rsV1.getTime(4, new GregorianCalendar(TimeZone.getTimeZone("UTC"))));
            }
        }
    }

    @Test (groups = "integration", enabled = true)
    public void setTimestampTest() throws SQLException {
        run("DROP TABLE IF EXISTS test_timestamp");
        run("CREATE TABLE IF NOT EXISTS test_timestamp (id Int8, t1 Datetime64(3), t2 Datetime64(6), t3 Datetime64(9), t4 DateTime64(9)) ENGINE = MergeTree ORDER BY id");

        Timestamp ts = new Timestamp(System.currentTimeMillis());

        try (Connection connV1 = getJdbcConnectionV1(null)) {
            try (PreparedStatement stmtV1 = connV1.prepareStatement("INSERT INTO test_timestamp VALUES (1, ?, ?, ?, ?)")) {//INSERT with V1
                stmtV1.setTimestamp(1, java.sql.Timestamp.valueOf("2021-01-01 01:23:45"));
                stmtV1.setTimestamp(2, java.sql.Timestamp.valueOf("2021-01-01 01:23:45"), new GregorianCalendar());
                stmtV1.setTimestamp(3, java.sql.Timestamp.valueOf("2021-01-01 01:23:45"), new GregorianCalendar(TimeZone.getTimeZone("UTC")));
                stmtV1.setTimestamp(4, ts);
                stmtV1.execute();
            }
        }

        try (Connection connV1 = getJdbcConnectionV1(null);
             Connection connV2 = getJdbcConnectionV2(null)) {
            try (Statement stmtV1 = connV1.createStatement();
                 Statement stmtV2 = connV2.createStatement()) {
                ResultSet rsV1 = stmtV1.executeQuery("SELECT * FROM test_timestamp");
                ResultSet rsV2 = stmtV2.executeQuery("SELECT * FROM test_timestamp");
                assertTrue(rsV1.next());
                assertTrue(rsV2.next());

                assertEquals(rsV2.getTimestamp(2), rsV1.getTimestamp(2));
                assertEquals(rsV2.getTimestamp(3, new GregorianCalendar()), rsV1.getTimestamp(3, new GregorianCalendar()));
                assertEquals(rsV2.getTimestamp(4, new GregorianCalendar(TimeZone.getTimeZone("UTC"))),
                        rsV1.getTimestamp(4, new GregorianCalendar(TimeZone.getTimeZone("UTC"))));
                assertEquals(rsV2.getTimestamp(5), rsV1.getTimestamp(5));
            }
        }
    }
}
