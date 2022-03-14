package com.clickhouse.jdbc;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;

import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.config.ClickHouseClientOption;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseConnectionTest extends JdbcIntegrationTest {
    @Override
    public ClickHouseConnection newConnection(Properties properties) throws SQLException {
        return newDataSource(properties).getConnection();
    }

    @Test(groups = "integration")
    public void testCreateArray() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties())) {
            Array array = conn.createArrayOf("Array(Int8)", null);
            Assert.assertEquals(array.getArray(), new byte[0]);
        }
    }

    @Test(groups = "integration")
    public void testNonExistDatabase() throws Exception {
        String database = UUID.randomUUID().toString();
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.DATABASE.getKey(), database);
        SQLException exp = null;
        try (ClickHouseConnection conn = newConnection(props)) {
            // do nothing
        }
        Assert.assertNull(exp, "Should not have SQLException even the database does not exist");

        try (ClickHouseConnection conn = newConnection(props)) {
            Assert.assertEquals(conn.getSchema(), database);
            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("select 1")) {
                Assert.fail("Should have SQLException");
            }
        } catch (SQLException e) {
            exp = e;
        }
        Assert.assertNotNull(exp, "Should have SQLException since the database does not exist");
        Assert.assertEquals(exp.getErrorCode(), 81);

        props.setProperty(JdbcConfig.PROP_CREATE_DATABASE, Boolean.TRUE.toString());
        try (ClickHouseConnection conn = newConnection(props)) {
            exp = null;
        }
        Assert.assertNull(exp, "Should not have SQLException because database will be created automatically");

        props.setProperty(JdbcConfig.PROP_CREATE_DATABASE, Boolean.FALSE.toString());
        try (ClickHouseConnection conn = newConnection(props)) {
            Assert.assertEquals(conn.getSchema(), database);
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            ClickHouseUtils.format("select * from system.databases where name='%s'", database))) {
                Assert.assertTrue(rs.next(), "Should have at least one record in system.databases table");
                Assert.assertEquals(rs.getString("name"), database);
                Assert.assertFalse(rs.next(), "Should have only one record in system.databases table");
                exp = new SQLException();
            }
        }
        Assert.assertNotNull(exp, "Should not have SQLException because the database has been created");
    }

    @Test(groups = "integration")
    public void testReadOnly() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "dba");
        props.setProperty("password", "dba");
        try (Connection conn = newConnection(props); Statement stmt = conn.createStatement()) {
            Assert.assertFalse(conn.isReadOnly(), "Connection should NOT be readonly");
            Assert.assertFalse(stmt.execute(
                    "drop table if exists test_readonly; drop user if exists readonly1; drop user if exists readonly2; "
                            + "create table test_readonly(id String)engine=Memory; "
                            + "create user readonly1 IDENTIFIED WITH no_password SETTINGS readonly=1; "
                            + "create user readonly2 IDENTIFIED WITH no_password SETTINGS readonly=2; "
                            + "grant insert on test_readonly TO readonly1, readonly2"));
            conn.setReadOnly(false);
            Assert.assertFalse(conn.isReadOnly(), "Connection should NOT be readonly");
            conn.setReadOnly(true);
            Assert.assertTrue(conn.isReadOnly(), "Connection should be readonly");

            try (Statement s = conn.createStatement()) {
                SQLException exp = null;
                try {
                    s.execute("insert into test_readonly values('readonly1')");
                } catch (SQLException e) {
                    exp = e;
                }
                Assert.assertNotNull(exp, "Should fail with SQL exception");
                Assert.assertEquals(exp.getErrorCode(), 164);
            }

            conn.setReadOnly(false);
            Assert.assertFalse(conn.isReadOnly(), "Connection should NOT be readonly");

            try (Statement s = conn.createStatement()) {
                Assert.assertFalse(s.execute("insert into test_readonly values('readonly1')"));
            }
        }

        props.clear();
        props.setProperty("user", "readonly1");
        try (Connection conn = newConnection(props); Statement stmt = conn.createStatement()) {
            Assert.assertTrue(conn.isReadOnly(), "Connection should be readonly");
            conn.setReadOnly(true);
            Assert.assertTrue(conn.isReadOnly(), "Connection should be readonly");
            SQLException exp = null;
            try {
                stmt.execute("insert into test_readonly values('readonly1')");
            } catch (SQLException e) {
                exp = e;
            }
            Assert.assertNotNull(exp, "Should fail with SQL exception");
            Assert.assertEquals(exp.getErrorCode(), 164);

            exp = null;
            try {
                conn.setReadOnly(true);
                stmt.execute("set max_result_rows=5; select 1");
            } catch (SQLException e) {
                exp = e;
            }
            Assert.assertNotNull(exp, "Should fail with SQL exception");
            Assert.assertEquals(exp.getErrorCode(), 164);
        }

        props.setProperty("user", "readonly2");
        try (Connection conn = newConnection(props); Statement stmt = conn.createStatement()) {
            Assert.assertTrue(conn.isReadOnly(), "Connection should be readonly");
            Assert.assertTrue(stmt.execute("set max_result_rows=5; select 1"));

            Assert.assertThrows(SQLException.class, () -> conn.setReadOnly(false));
            Assert.assertTrue(conn.isReadOnly(), "Connection should be readonly");

            SQLException exp = null;
            try (Statement s = conn.createStatement()) {
                Assert.assertFalse(s.execute("insert into test_readonly values('readonly2')"));
            } catch (SQLException e) {
                exp = e;
            }
            Assert.assertNotNull(exp, "Should fail with SQL exception");
            Assert.assertEquals(exp.getErrorCode(), 164);

            conn.setReadOnly(true);
            Assert.assertTrue(conn.isReadOnly(), "Connection should be readonly");
        }

        props.setProperty(ClickHouseClientOption.SERVER_TIME_ZONE.getKey(), "UTC");
        props.setProperty(ClickHouseClientOption.SERVER_VERSION.getKey(), "21.8");
        try (Connection conn = newConnection(props); Statement stmt = conn.createStatement()) {
            Assert.assertFalse(conn.isReadOnly(), "Connection should NOT be readonly");
            Assert.assertTrue(stmt.execute("set max_result_rows=5; select 1"));

            conn.setReadOnly(true);
            Assert.assertTrue(conn.isReadOnly(), "Connection should be readonly");
            conn.setReadOnly(false);
            Assert.assertFalse(conn.isReadOnly(), "Connection should NOT be readonly");

            SQLException exp = null;
            try (Statement s = conn.createStatement()) {
                Assert.assertFalse(s.execute("insert into test_readonly values('readonly2')"));
            } catch (SQLException e) {
                exp = e;
            }
            Assert.assertNotNull(exp, "Should fail with SQL exception");
            Assert.assertEquals(exp.getErrorCode(), 164);
        }
    }
}
