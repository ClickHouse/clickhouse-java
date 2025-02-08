package com.clickhouse.jdbc;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.value.UnsignedByte;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ClickHouseConnectionTest extends JdbcIntegrationTest {
    @BeforeMethod(groups = "integration")
    public void setV1() {
        System.setProperty("clickhouse.jdbc.v1","true");
    }
    @Override
    public ClickHouseConnection newConnection(Properties properties) throws SQLException {
        return (ClickHouseConnection) newDataSource(properties).getConnection();
    }

    @Test(groups = "integration")
    public void testCentralizedConfiguration() throws SQLException {
        if (isCloud()) return; //TODO: testCentralizedConfiguration - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747

        Properties props = new Properties();
        props.setProperty("custom_settings", "max_result_rows=1");
        try (ClickHouseConnection conn = newConnection(props); Statement stmt = conn.createStatement()) {
            // gRPC stopped working since 23.3 with below error:
            // SQL Code: 649, DB::Exception: Transaction Control Language queries are
            // allowed only inside session: while starting a transaction with
            // 'implicit_transaction'
            if (stmt.unwrap(ClickHouseRequest.class).getServer().getProtocol() == ClickHouseProtocol.GRPC) {
                throw new SkipException("Skip the test as transaction is supported since 22.7");
            }

            Assert.assertEquals(conn.getConfig().getResponseCompressAlgorithm(), ClickHouseCompression.LZ4);
            Assert.assertTrue(conn.getJdbcConfig().isAutoCommit());
            Assert.assertFalse(conn.getJdbcConfig().isTransactionSupported());
            Assert.assertThrows(SQLException.class,
                    () -> conn.createStatement().executeQuery("select * from numbers(2)"));
        }

        props.setProperty("user", "poorman1");
        props.setProperty("password", "poorman_111");
        props.setProperty("autoCommit", "false");
        props.setProperty("compress_algorithm", "lz4");
        props.setProperty("transactionSupport", "false");
        try (ClickHouseConnection conn = newConnection(props)) {
            Assert.assertEquals(conn.getConfig().getResponseCompressAlgorithm(), ClickHouseCompression.GZIP);
            Assert.assertFalse(conn.getJdbcConfig().isAutoCommit());
            Assert.assertTrue(conn.getJdbcConfig().isTransactionSupported());
            Assert.assertThrows(SQLException.class,
                    () -> conn.createStatement().executeQuery("select * from numbers(2)"));
            conn.rollback();
        } catch (SQLException e) {
            if (e.getErrorCode() != 649) {
                Assert.fail(e.getMessage());
            }
        }

        props.setProperty("user", "poorman2");
        props.setProperty("password", "poorman_222");
        try (ClickHouseConnection conn = newConnection(props)) {
            Assert.assertEquals(conn.getConfig().getResponseCompressAlgorithm(), ClickHouseCompression.GZIP);
            Assert.assertTrue(conn.getJdbcConfig().isAutoCommit());
            Assert.assertTrue(conn.getJdbcConfig().isTransactionSupported());
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("select * from numbers(2)")) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertTrue(rs.next(), "Should have two rows");
                Assert.assertFalse(rs.next(), "Should have only two rows");
            }
        }
    }

    @Test(groups = "integration")
    public void testCreateArray() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties())) {
            Assert.assertThrows(SQLException.class, () -> conn.createArrayOf("Int8", null));
            Assert.assertThrows(SQLException.class, () -> conn.createArrayOf("UInt8", null));

            Array array = conn.createArrayOf("Nullable(Int8)", null);
            Assert.assertEquals(array.getArray(), new Byte[0]);
            array = conn.createArrayOf("Nullable(UInt8)", null);
            Assert.assertEquals(array.getArray(), new UnsignedByte[0]);
            array = conn.createArrayOf("Array(Int8)", null);
            Assert.assertEquals(array.getArray(), new byte[0][]);
            array = conn.createArrayOf("Array(UInt8)", null);
            Assert.assertEquals(array.getArray(), new byte[0][]);
            array = conn.createArrayOf("Array(Nullable(Int8))", null);
            Assert.assertEquals(array.getArray(), new Byte[0][]);
            array = conn.createArrayOf("Array(Nullable(UInt8))", null);
            Assert.assertEquals(array.getArray(), new UnsignedByte[0][]);

            array = conn.createArrayOf("Int8", new Byte[] { -1, 0, 1 });
            Assert.assertEquals(array.getArray(), new byte[] { -1, 0, 1 });
            array = conn.createArrayOf("UInt8", new Byte[] { -1, 0, 1 });
            Assert.assertEquals(array.getArray(), new byte[] { -1, 0, 1 });

            array = conn.createArrayOf("Nullable(Int8)", new Byte[] { -1, null, 1 });
            Assert.assertEquals(array.getArray(), new Byte[] { -1, null, 1 });
            array = conn.createArrayOf("Nullable(UInt8)", new Byte[] { -1, null, 1 });
            Assert.assertEquals(array.getArray(), new Byte[] { -1, null, 1 });
            array = conn.createArrayOf("Nullable(UInt8)",
                    new UnsignedByte[] { UnsignedByte.MAX_VALUE, null, UnsignedByte.ONE });
            Assert.assertEquals(array.getArray(),
                    new UnsignedByte[] { UnsignedByte.MAX_VALUE, null, UnsignedByte.ONE });

            array = conn.createArrayOf("Array(Int8)", new byte[][] { { -1, 0, 1 } });
            Assert.assertEquals(array.getArray(), new byte[][] { { -1, 0, 1 } });
            array = conn.createArrayOf("Array(UInt8)", new Byte[][] { { -1, 0, 1 } });
            Assert.assertEquals(array.getArray(), new Byte[][] { { -1, 0, 1 } });

            // invalid but works
            array = conn.createArrayOf("Array(Int8)", new Byte[] { -1, 0, 1 });
            Assert.assertEquals(array.getArray(), new Byte[] { -1, 0, 1 });
            array = conn.createArrayOf("Array(UInt8)", new byte[][] { { -1, 0, 1 } });
            Assert.assertEquals(array.getArray(), new byte[][] { { -1, 0, 1 } });
        }
    }

    @Test(groups = "integration")
    public void testAutoCommitMode() throws SQLException {
        if (isCloud()) return; //TODO: testAutoCommitMode - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747

        Properties props = new Properties();
        props.setProperty("transactionSupport", "true");

        for (int i = 0; i < 10; i++) {
            try (ClickHouseConnection conn = newConnection(props); Statement stmt = conn.createStatement()) {
                if (!conn.getServerVersion().check("[22.7,)")
                        // gRPC stopped working since 23.3 with below error:
                        // SQL Code: 649, DB::Exception: Transaction Control Language queries are
                        // allowed only inside session: while starting a transaction with
                        // 'implicit_transaction'
                        || stmt.unwrap(ClickHouseRequest.class).getServer().getProtocol() == ClickHouseProtocol.GRPC) {
                    throw new SkipException("Skip the test as transaction is supported since 22.7");
                }
                stmt.execute("select 1, throwIf(" + i + " % 3 = 0)");
                stmt.executeQuery("select number, toDateTime(number), toString(number), throwIf(" + i + " % 5 = 0)"
                        + " from numbers(100000)");
            } catch (SQLException e) {
                if (i % 3 == 0 || i % 5 == 0) {
                    Assert.assertEquals(e.getErrorCode(), 395, "Expected error code 395 but we got: " + e.getMessage());
                } else {
                    Assert.fail("Should not have exception");
                }
            }
        }
    }

    @Test(groups = "integration")
    public void testNonExistDatabase() throws SQLException {
        String database = UUID.randomUUID().toString();
        Properties props = new Properties();
        props.setProperty(JdbcConfig.PROP_DATABASE_TERM, JdbcConfig.TERM_SCHEMA);
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
        Assert.assertEquals(exp.getErrorCode(), 81, "Expected error code 81 but we got: " + exp.getMessage());

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

    @Test(groups = "integration", enabled = false)
    // Disabled because will be fixed later. (Should be tested in the new JDBC driver)
    public void testReadOnly() throws SQLException {
        if (isCloud()) return; //TODO: testReadOnly - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        Properties props = new Properties();
        props.setProperty("user", "dba");
        props.setProperty("password", "dba");
        try (Connection conn = newConnection(props); Statement stmt = conn.createStatement()) {
            Assert.assertFalse(conn.isReadOnly(), "Connection should NOT be readonly");
            Assert.assertFalse(stmt.execute(
                    "drop table if exists test_readonly; drop user if exists readonly1; drop user if exists readonly2; "
                            + "create table test_readonly(id String)engine=Memory; "
                            + "create user readonly1 IDENTIFIED BY 'some_password' SETTINGS readonly=1; "
                            + "create user readonly2 IDENTIFIED BY 'some_password' SETTINGS readonly=2; "
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
                Assert.assertEquals(exp.getErrorCode(), 164, "Expected error code 164 but we got: " + exp.getMessage());
            }

            conn.setReadOnly(false);
            Assert.assertFalse(conn.isReadOnly(), "Connection should NOT be readonly");

            try (Statement s = conn.createStatement()) {
                Assert.assertFalse(s.execute("insert into test_readonly values('readonly1')"));
            }
        }

        props.clear();
        props.setProperty("user", "readonly1");
        props.setProperty("password", "some_password");
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
            Assert.assertEquals(exp.getErrorCode(), 164, "Expected error code 164 but we got: " + exp.getMessage());

            exp = null;
            try {
                conn.setReadOnly(true);
                stmt.execute("set max_result_rows=5; select 1");
            } catch (SQLException e) {
                exp = e;
            }
            Assert.assertNotNull(exp, "Should fail with SQL exception");
            Assert.assertEquals(exp.getErrorCode(), 164, "Expected error code 164 but we got: " + exp.getMessage());
        }

        props.setProperty("user", "readonly2");
        props.setProperty("password", "some_password");
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
            Assert.assertEquals(exp.getErrorCode(), 164, "Expected error code 164 but we got: " + exp.getMessage());

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
            Assert.assertEquals(exp.getErrorCode(), 164, "Expected error code 164 but we got: " + exp.getMessage());
        }
    }

    @Test(groups = "integration")
    public void testAutoCommit() throws SQLException {
        if (isCloud()) return; //TODO: testAutoCommit - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747

        Properties props = new Properties();
        props.setProperty("transactionSupport", "true");
        String tableName = "test_jdbc_tx_auto_commit";
        try (ClickHouseConnection c = newConnection(props); Statement s = c.createStatement()) {
            if (!c.getServerVersion().check("[22.7,)")
                    // gRPC stopped working since 23.3 with below error:
                    // SQL Code: 649, DB::Exception: Transaction Control Language queries are
                    // allowed only inside session: while starting a transaction with
                    // 'implicit_transaction'
                    || s.unwrap(ClickHouseRequest.class).getServer().getProtocol() == ClickHouseProtocol.GRPC) {
                throw new SkipException("Skip the test as transaction is supported since 22.7");
            }
            s.execute("drop table if exists " + tableName + "; "
                    + "create table " + tableName + "(id UInt64) engine=MergeTree order by id");
        }

        try (ClickHouseConnection conn = newConnection();
                ClickHouseConnection txConn = newConnection(props);
                Statement stmt = conn.createStatement();
                Statement txStmt = txConn.createStatement();
                PreparedStatement ps = conn.prepareStatement("insert into " + tableName);
                PreparedStatement txPs = txConn.prepareStatement("insert into " + tableName)) {
            Assert.assertTrue(conn.getAutoCommit());
            Assert.assertTrue(txConn.getAutoCommit());
            Assert.assertFalse(conn.isTransactionSupported());
            Assert.assertTrue(txConn.isTransactionSupported());
            Assert.assertFalse(conn.isImplicitTransactionSupported());
            if (txConn.getServerVersion().check("[22.7,)")) {
                Assert.assertTrue(txConn.isImplicitTransactionSupported(),
                        "Implicit transaction is supported since 22.7");
            } else {
                Assert.assertFalse(txConn.isImplicitTransactionSupported(),
                        "Implicit transaction is NOT supported before 22.7");
            }

            checkRowCount(stmt, "select 1", 1);
            checkRowCount(txStmt, "select 1", 1);

            txStmt.execute("drop table if exists " + tableName + "; "
                    + "create table " + tableName + "(id UInt64) engine=MergeTree order by id");
            checkRowCount(stmt, tableName, 0);
            checkRowCount(txStmt, tableName, 0);

            stmt.executeUpdate("insert into " + tableName + " values(1)");
            checkRowCount(stmt, tableName, 1);
            checkRowCount(txStmt, tableName, 1);

            txStmt.executeUpdate("insert into " + tableName + " values(2)");
            checkRowCount(stmt, tableName, 2);
            checkRowCount(txStmt, tableName, 2);

            try (Connection c = newConnection(props); Statement s = c.createStatement()) {
                c.setAutoCommit(false);
                s.executeUpdate("insert into " + tableName + " values(-1)");
                checkRowCount(stmt, tableName, 3);
                checkRowCount(txStmt, tableName, 2);
                checkRowCount(s, tableName, 3);
                c.rollback();
                checkRowCount(stmt, tableName, 2);
                checkRowCount(txStmt, tableName, 2);
                checkRowCount(s, tableName, 2);
            }
            checkRowCount(stmt, tableName, 2);
            checkRowCount(txStmt, tableName, 2);

            try (Connection c = newConnection(props); Statement s = c.createStatement()) {
                c.setAutoCommit(false);
                s.executeUpdate("insert into " + tableName + " values(-2)");
                checkRowCount(stmt, tableName, 3);
                checkRowCount(txStmt, tableName, 2);
                checkRowCount(s, tableName, 3);
            }
            checkRowCount(stmt, tableName, 3);
            checkRowCount(txStmt, tableName, 3);

            ps.setInt(1, 3);
            ps.addBatch();
            ps.setInt(1, 4);
            ps.addBatch();
            ps.executeBatch();
            checkRowCount(stmt, tableName, 5);
            checkRowCount(txStmt, tableName, 5);

            txPs.setInt(1, 5);
            txPs.addBatch();
            txPs.setInt(1, 6);
            txPs.addBatch();
            txPs.executeBatch();
            checkRowCount(stmt, tableName, 7);
            checkRowCount(txStmt, tableName, 7);
        }
    }

    @Test(groups = "integration")
    public void testManualTxApi() throws SQLException {
        if (isCloud()) return; //TODO: testManualTxApi - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747

        Properties props = new Properties();
        props.setProperty("autoCommit", "false");
        Properties txProps = new Properties();
        txProps.putAll(props);
        txProps.setProperty("transactionSupport", "true");
        String tableName = "test_jdbc_manual_tx_api";
        try (ClickHouseConnection c = newConnection(txProps); Statement s = c.createStatement()) {
            if (!c.getServerVersion().check("[22.7,)")
                    // gRPC stopped working since 23.3 with below error:
                    // SQL Code: 649, DB::Exception: Transaction Control Language queries are
                    // allowed only inside session: while starting a transaction with
                    // 'implicit_transaction'
                    || s.unwrap(ClickHouseRequest.class).getServer().getProtocol() == ClickHouseProtocol.GRPC) {
                throw new SkipException("Skip the test as transaction is supported since 22.7");
            }
            s.execute("drop table if exists " + tableName + "; "
                    + "create table " + tableName + "(id UInt64, value String) engine=MergeTree order by id");
        }

        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseConnection txConn = newConnection(txProps);
                Statement stmt = conn.createStatement();
                Statement txStmt = txConn.createStatement();
                PreparedStatement ps = conn.prepareStatement("insert into " + tableName);
                PreparedStatement txPs = txConn.prepareStatement("insert into " + tableName)) {
            Assert.assertFalse(conn.getAutoCommit());
            Assert.assertFalse(txConn.getAutoCommit());
            Assert.assertFalse(conn.isTransactionSupported());
            Assert.assertTrue(txConn.isTransactionSupported());
            Assert.assertFalse(conn.isImplicitTransactionSupported());
            if (txConn.getServerVersion().check("[22.7,)")) {
                Assert.assertTrue(txConn.isImplicitTransactionSupported(),
                        "Implicit transaction is supported since 22.7");
            } else {
                Assert.assertFalse(txConn.isImplicitTransactionSupported(),
                        "Implicit transaction is NOT supported before 22.7");
            }

            Assert.assertThrows(SQLException.class, () -> conn.begin());
            Assert.assertThrows(SQLException.class, () -> txConn.begin());

            checkRowCount(stmt, "select 1", 1);
            checkRowCount(txStmt, "select 1", 1);
            Assert.assertThrows(SQLException.class, () -> txConn.begin());
            txConn.commit();

            txConn.begin();
            checkRowCount(stmt, "select 1", 1);
            checkRowCount(txStmt, "select 1", 1);
            txConn.rollback();

            checkRowCount(stmt, tableName, 0);
            checkRowCount(txStmt, tableName, 0);

            txStmt.executeUpdate("insert into " + tableName + " values(0, '0')");
            checkRowCount(stmt, tableName, 1);
            checkRowCount(txStmt, tableName, 1);
            txConn.rollback();
            checkRowCount(stmt, tableName, 0);
            checkRowCount(txStmt, tableName, 0);

            stmt.executeUpdate("insert into " + tableName + " values(1, 'a')");
            checkRowCount(stmt, tableName, 1);
            checkRowCount(txStmt, tableName, 1);

            txStmt.executeUpdate("insert into " + tableName + " values(2, 'b')");
            checkRowCount(stmt, tableName, 2);
            checkRowCount(txStmt, tableName, 2);

            try (Connection c = newConnection(txProps); Statement s = c.createStatement()) {
                s.executeUpdate("insert into " + tableName + " values(-1, '-1')");
                checkRowCount(stmt, tableName, 3);
                checkRowCount(txStmt, tableName, 2);
                checkRowCount(s, tableName, 2);
                c.rollback();
                checkRowCount(stmt, tableName, 2);
                checkRowCount(txStmt, tableName, 2);
                checkRowCount(s, tableName, 1);
            }
            checkRowCount(stmt, tableName, 2);
            checkRowCount(txStmt, tableName, 2);

            try (Connection c = newConnection(txProps); Statement s = c.createStatement()) {
                s.executeUpdate("insert into " + tableName + " values(3, 'c')");
                checkRowCount(stmt, tableName, 3);
                checkRowCount(txStmt, tableName, 2);
                checkRowCount(s, tableName, 2);
                txConn.commit();
                checkRowCount(stmt, tableName, 3);
                checkRowCount(txStmt, tableName, 2);
                checkRowCount(s, tableName, 2);
            }
            checkRowCount(stmt, tableName, 3);
            checkRowCount(txStmt, tableName, 2);
            txConn.commit();
            checkRowCount(txStmt, tableName, 3);

            txConn.setAutoCommit(true);
            Assert.assertTrue(txConn.getAutoCommit());
            try (Statement s = txConn.createStatement()) {
                s.executeUpdate("insert into " + tableName + " values(4, 'd')");
                checkRowCount(stmt, tableName, 4);
                checkRowCount(txStmt, tableName, 4);
                checkRowCount(s, tableName, 4);
            }

            try (Statement s = txConn.createStatement()) {
                checkRowCount(stmt, tableName, 4);
                checkRowCount(txStmt, tableName, 4);
                checkRowCount(s, tableName, 4);
            }
        }
    }

    @Test(groups = "integration")
    public void testManualTxTcl() throws SQLException {
        if (isCloud()) return; //TODO: testManualTxTcl - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747

        Properties props = new Properties();
        props.setProperty("autoCommit", "false");
        Properties txProps = new Properties();
        txProps.putAll(props);
        txProps.setProperty("transactionSupport", "true");
        String tableName = "test_jdbc_manual_tx_tcl";
        try (ClickHouseConnection c = newConnection(txProps); Statement s = c.createStatement()) {
            if (!c.getServerVersion().check("[22.7,)")
                    // gRPC stopped working since 23.3 with below error:
                    // SQL Code: 649, DB::Exception: Transaction Control Language queries are
                    // allowed only inside session: while starting a transaction with
                    // 'implicit_transaction'
                    || s.unwrap(ClickHouseRequest.class).getServer().getProtocol() == ClickHouseProtocol.GRPC) {
                throw new SkipException("Skip the test as transaction is supported since 22.7");
            }
            s.execute("drop table if exists " + tableName + "; "
                    + "create table " + tableName + "(id UInt64, value String) engine=MergeTree order by id");
        }

        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseConnection txConn = newConnection(txProps);
                Statement stmt = conn.createStatement();
                Statement txStmt = txConn.createStatement();
                PreparedStatement ps = conn.prepareStatement("insert into " + tableName);
                PreparedStatement txPs = txConn.prepareStatement("insert into " + tableName)) {
            Assert.assertFalse(conn.getAutoCommit());
            Assert.assertFalse(txConn.getAutoCommit());
            Assert.assertFalse(conn.isTransactionSupported());
            Assert.assertTrue(txConn.isTransactionSupported());
            Assert.assertFalse(conn.isImplicitTransactionSupported());
            if (txConn.getServerVersion().check("[22.7,)")) {
                Assert.assertTrue(txConn.isImplicitTransactionSupported(),
                        "Implicit transaction is supported since 22.7");
            } else {
                Assert.assertFalse(txConn.isImplicitTransactionSupported(),
                        "Implicit transaction is NOT supported before 22.7");
            }

            Assert.assertThrows(SQLException.class, () -> stmt.execute("begin transaction"));
            Assert.assertThrows(SQLException.class, () -> txStmt.execute("begin transaction"));

            checkRowCount(stmt, "select 1", 1);
            checkRowCount(txStmt, "select 1", 1);
            try (Statement s = conn.createStatement()) {
                Assert.assertEquals(s.executeUpdate("commit"), 0);
            }
            try (Statement s = txConn.createStatement()) {
                Assert.assertEquals(s.executeUpdate("commit"), 0);
            }

            Assert.assertEquals(stmt.executeUpdate("begin transaction"), 0);
            Assert.assertEquals(txStmt.executeUpdate("begin transaction"), 0);
            checkRowCount(stmt, "begin transaction; select 1", 1);
            checkRowCount(txStmt, "begin transaction; select 1", 1);
            try (Statement s = txConn.createStatement()) {
                Assert.assertEquals(s.executeUpdate("rollback"), 0);
            }

            checkRowCount(stmt, tableName, 0);
            checkRowCount(txStmt, tableName, 0);

            txStmt.executeUpdate("insert into " + tableName + " values(0, '0')");
            checkRowCount(stmt, tableName, 1);
            checkRowCount(txStmt, tableName, 1);
            try (Statement s = txConn.createStatement()) {
                Assert.assertEquals(s.executeUpdate("rollback"), 0);
            }
            checkRowCount(stmt, tableName, 0);
            checkRowCount(txStmt, tableName, 0);

            stmt.executeUpdate("insert into " + tableName + " values(1, 'a')");
            checkRowCount(stmt, tableName, 1);
            checkRowCount(txStmt, tableName, 1);

            txStmt.executeUpdate("insert into " + tableName + " values(2, 'b')");
            checkRowCount(stmt, tableName, 2);
            checkRowCount(txStmt, tableName, 2);

            try (Connection c = newConnection(txProps); Statement s = c.createStatement()) {
                s.executeUpdate("insert into " + tableName + " values(-1, '-1')");
                checkRowCount(stmt, tableName, 3);
                checkRowCount(txStmt, tableName, 2);
                checkRowCount(s, tableName, 2);
                try (Statement ss = c.createStatement()) {
                    Assert.assertEquals(ss.executeUpdate("rollback"), 0);
                }
                checkRowCount(stmt, tableName, 2);
                checkRowCount(txStmt, tableName, 2);
                checkRowCount(s, tableName, 1);
            }
            checkRowCount(stmt, tableName, 2);
            checkRowCount(txStmt, tableName, 2);

            try (Connection c = newConnection(txProps); Statement s = c.createStatement()) {
                s.executeUpdate("insert into " + tableName + " values(3, 'c')");
                checkRowCount(stmt, tableName, 3);
                checkRowCount(txStmt, tableName, 2);
                checkRowCount(s, tableName, 2);
                try (Statement ss = txConn.createStatement()) {
                    Assert.assertEquals(ss.executeUpdate("commit"), 0);
                }
                checkRowCount(stmt, tableName, 3);
                checkRowCount(txStmt, tableName, 2);
                checkRowCount(s, tableName, 2);
            }
            checkRowCount(stmt, tableName, 3);
            checkRowCount(txStmt, tableName, 2);
            try (Statement s = txConn.createStatement()) {
                Assert.assertEquals(s.executeUpdate("commit"), 0);
            }
            checkRowCount(txStmt, tableName, 3);

            try (Statement s = conn.createStatement()) {
                Assert.assertEquals(s.executeUpdate("commit"), 0);
            }
            try (Statement s = txConn.createStatement()) {
                Assert.assertEquals(s.executeUpdate("commit"), 0);
            }
            txStmt.addBatch("begin transaction");
            txStmt.addBatch("insert into " + tableName + " values(4, 'd')");
            txStmt.addBatch("insert into " + tableName + " values(5, 'e')");
            txStmt.addBatch("commit");
            txStmt.executeBatch();

            txStmt.addBatch("insert into " + tableName + " values(6, 'f')");
            txStmt.addBatch("rollback");
            txStmt.executeBatch();

            txConn.setAutoCommit(true);
            Assert.assertTrue(txConn.getAutoCommit());
            try (Statement s = txConn.createStatement()) {
                s.executeUpdate("insert into " + tableName + " values(6, 'f')");
                checkRowCount(stmt, tableName, 6);
                checkRowCount(txStmt, tableName, 6);
                checkRowCount(s, tableName, 6);
            }

            try (Statement s = txConn.createStatement()) {
                checkRowCount(stmt, tableName, 6);
                checkRowCount(txStmt, tableName, 6);
                checkRowCount(s, tableName, 6);
            }
        }
    }

    @Test(groups = "integration")
    public void testNestedTransactions() throws SQLException {
        if (isCloud()) return; //TODO: testNestedTransactions - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747

        Properties props = new Properties();
        props.setProperty("autoCommit", "false");
        props.setProperty("transactionSupport", "true");
        String tableName = "test_jdbc_nested_tx";
        try (ClickHouseConnection c = newConnection(props); Statement s = c.createStatement()) {
            if (!c.getServerVersion().check("[22.7,)")) {
                throw new SkipException("Skip the test as transaction is supported since 22.7");
            }
            s.execute("drop table if exists " + tableName + "; "
                    + "create table " + tableName + "(id UInt64) engine=MergeTree order by id");
        }

        try (Connection conn = newConnection(props);
                Statement stmt = conn.createStatement();
                PreparedStatement ps = conn.prepareStatement("insert into " + tableName)) {
            checkRowCount(stmt, tableName, 0);
            stmt.executeQuery("insert into " + tableName + " values(1)");
            checkRowCount(stmt, tableName, 1);
            ps.setInt(1, 2);
            ps.executeUpdate();
            checkRowCount(stmt, tableName, 2);
            ps.setInt(1, 3);
            ps.executeBatch();
            checkRowCount(stmt, tableName, 2);
            ps.setInt(1, 3);
            ps.addBatch();
            ps.executeBatch();
            checkRowCount(stmt, tableName, 3);
            try (Connection c = newConnection(); Statement s = c.createStatement()) {
                checkRowCount(s, tableName, 3);
            }

            conn.rollback();
            checkRowCount(stmt, tableName, 0);
            try (Connection c = newConnection(); Statement s = c.createStatement()) {
                checkRowCount(s, tableName, 0);
            }
        }
    }

    @Test(groups = "integration")
    public void testParallelTransactions() throws SQLException {
        if (isCloud()) return; //TODO: testParallelTransactions - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747

        Properties props = new Properties();
        props.setProperty("autoCommit", "false");
        props.setProperty("transactionSupport", "true");
        String tableName = "test_jdbc_parallel_tx";
        try (ClickHouseConnection c = newConnection(props); Statement s = c.createStatement()) {
            if (!c.getServerVersion().check("[22.7,)")) {
                throw new SkipException("Skip the test as transaction is supported since 22.7");
            }
            s.execute("drop table if exists " + tableName + "; "
                    + "create table " + tableName + "(id UInt64) engine=MergeTree order by id");
        }

        try (Connection conn1 = newConnection(props);
                Connection conn2 = newConnection(props);
                Statement stmt1 = conn1.createStatement();
                Statement stmt2 = conn2.createStatement();
                PreparedStatement ps1 = conn1.prepareStatement("insert into " + tableName);
                PreparedStatement ps2 = conn2.prepareStatement("insert into " + tableName)) {
            stmt1.executeUpdate("insert into " + tableName + " values(-1)");
            checkRowCount(stmt1, tableName, 1);
            checkRowCount(stmt2, tableName, 0);
            conn1.rollback();
            checkRowCount(stmt1, tableName, 0);
            checkRowCount(stmt2, tableName, 0);

            stmt2.executeUpdate("insert into " + tableName + " values(-2)");
            checkRowCount(stmt1, tableName, 0);
            checkRowCount(stmt2, tableName, 1);
            conn2.commit();
            checkRowCount(stmt1, tableName, 0);
            checkRowCount(stmt2, tableName, 1);
            conn1.commit();
            checkRowCount(stmt1, tableName, 1);
            checkRowCount(stmt2, tableName, 1);

            ps1.setInt(1, 1);
            ps1.addBatch();
            ps1.setInt(1, 2);
            ps1.addBatch();
            ps1.setInt(1, 3);
            ps1.addBatch();
            ps1.executeBatch();
            checkRowCount(stmt1, tableName, 4);
            checkRowCount(stmt2, tableName, 1);
            conn1.commit();
            checkRowCount(stmt1, tableName, 4);
            checkRowCount(stmt2, tableName, 1);
            try (Connection c = newConnection(props); Statement s = c.createStatement()) {
                checkRowCount(s, tableName, 4);
            }

            ps2.setInt(1, 4);
            ps2.addBatch();
            ps2.setInt(1, 5);
            ps2.addBatch();
            ps2.setInt(1, 6);
            // ps2.addBatch();
            ps2.executeBatch();
            checkRowCount(stmt1, tableName, 4);
            checkRowCount(stmt2, tableName, 3);
            conn2.commit();
            checkRowCount(stmt1, tableName, 4);
            checkRowCount(stmt2, tableName, 6);
            try (Connection c = newConnection(props); Statement s = c.createStatement()) {
                checkRowCount(s, tableName, 6);
            }
        }
    }
}
