package com.clickhouse.jdbc.internal;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Properties;

import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseStatement;
import com.clickhouse.jdbc.JdbcIntegrationTest;
import com.clickhouse.jdbc.parser.ClickHouseSqlStatement;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ClickHouseConnectionImplTest extends JdbcIntegrationTest {
    @Test(groups = "integration")
    public void testManualCommit() throws SQLException {
        if (isCloud()) return; //TODO: testManualCommit - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        try (ClickHouseConnectionImpl conn = (ClickHouseConnectionImpl) newConnection()) {
            Assert.assertEquals(conn.getAutoCommit(), true);
            Assert.assertNull(conn.getTransaction(), "Should NOT have any transaction");
            conn.setAutoCommit(false);
            Assert.assertEquals(conn.getAutoCommit(), false);
            JdbcTransaction tx = conn.getJdbcTrasaction();
            Assert.assertNotNull(tx, "Should have transaction");
            Assert.assertEquals(tx.getQueries().size(), 0);
            Assert.assertEquals(tx.getSavepoints().size(), 0);
            Assert.assertEquals(tx.tx, conn.getTransaction());
            try (ClickHouseStatement stmt = conn.createStatement()) {
                stmt.execute("select 1; select 2");
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 0);

                Savepoint s = conn.setSavepoint("test");
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 1);
                conn.releaseSavepoint(s);
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 0);

                s = conn.setSavepoint("test");
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 1);
                stmt.execute("select 3");
                Assert.assertEquals(tx.getQueries().size(), 3);
                Assert.assertEquals(tx.getSavepoints().size(), 1);
                conn.releaseSavepoint(s);
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 0);

                s = conn.setSavepoint("test");
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 1);
                stmt.execute("select 3; select 4");
                Assert.assertEquals(tx.getQueries().size(), 4);
                Assert.assertEquals(tx.getSavepoints().size(), 1);
                conn.setSavepoint();
                Assert.assertEquals(tx.getQueries().size(), 4);
                Assert.assertEquals(tx.getSavepoints().size(), 2);
                stmt.execute("select 5");
                Assert.assertEquals(tx.getQueries().size(), 5);
                Assert.assertEquals(tx.getSavepoints().size(), 2);
                conn.releaseSavepoint(s);
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 0);

                conn.setSavepoint();
                conn.setSavepoint("test");
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 2);
            }

            try (ClickHouseStatement stmt = conn.createStatement()) {
                stmt.execute("select 6");
                Assert.assertEquals(tx.getQueries().size(), 3);
                Assert.assertEquals(tx.getSavepoints().size(), 2);
            }
            conn.commit();
            JdbcTransaction newTx = conn.getJdbcTrasaction();
            Assert.assertNotEquals(newTx, tx);
            Assert.assertNotNull(tx, "Should have transaction");
            Assert.assertEquals(tx.getQueries().size(), 0);
            Assert.assertEquals(tx.getSavepoints().size(), 0);
            Assert.assertNotNull(newTx, "Should have transaction");
            Assert.assertEquals(newTx.getQueries().size(), 0);
            Assert.assertEquals(newTx.getSavepoints().size(), 0);
            Assert.assertEquals(newTx.tx, conn.getTransaction());
            tx = newTx;

            try (ClickHouseStatement stmt = conn.createStatement()) {
                Savepoint s = conn.setSavepoint();
                stmt.execute("select 7; select 8");
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 1);
            }
            conn.commit();
            newTx = conn.getJdbcTrasaction();
            Assert.assertNotEquals(newTx, tx);
            Assert.assertNotNull(tx, "Should have transaction");
            Assert.assertEquals(tx.getQueries().size(), 0);
            Assert.assertEquals(tx.getSavepoints().size(), 0);
            Assert.assertNotNull(newTx, "Should have transaction");
            Assert.assertEquals(newTx.getQueries().size(), 0);
            Assert.assertEquals(newTx.getSavepoints().size(), 0);
            Assert.assertEquals(newTx.tx, conn.getTransaction());
        }
    }

    @Test(groups = "integration")
    public void testManualRollback() throws SQLException {
        if (isCloud()) return; //TODO: testManualRollback - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        try (ClickHouseConnectionImpl conn = (ClickHouseConnectionImpl) newConnection()) {
            Assert.assertEquals(conn.getAutoCommit(), true);
            Assert.assertNull(conn.getTransaction(), "Should NOT have any transaction");
            conn.setAutoCommit(false);
            Assert.assertEquals(conn.getAutoCommit(), false);
            JdbcTransaction tx = conn.getJdbcTrasaction();
            Assert.assertNotNull(tx, "Should have transaction");
            Assert.assertEquals(tx.getQueries().size(), 0);
            Assert.assertEquals(tx.getSavepoints().size(), 0);
            Assert.assertEquals(tx.tx, conn.getTransaction());
            try (ClickHouseStatement stmt = conn.createStatement()) {
                stmt.execute("select 1; select 2");
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 0);

                Savepoint s = conn.setSavepoint("test");
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 1);
                conn.rollback(s);
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 0);

                s = conn.setSavepoint("test");
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 1);
                stmt.execute("select 3");
                Assert.assertEquals(tx.getQueries().size(), 3);
                Assert.assertEquals(tx.getSavepoints().size(), 1);
                conn.rollback(s);
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 0);

                s = conn.setSavepoint("test");
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 1);
                stmt.execute("select 3; select 4");
                Assert.assertEquals(tx.getQueries().size(), 4);
                Assert.assertEquals(tx.getSavepoints().size(), 1);
                conn.setSavepoint();
                Assert.assertEquals(tx.getQueries().size(), 4);
                Assert.assertEquals(tx.getSavepoints().size(), 2);
                stmt.execute("select 5");
                Assert.assertEquals(tx.getQueries().size(), 5);
                Assert.assertEquals(tx.getSavepoints().size(), 2);
                conn.rollback(s);
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 0);

                conn.setSavepoint();
                conn.setSavepoint("test");
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 2);
            }

            try (ClickHouseStatement stmt = conn.createStatement()) {
                stmt.execute("select 6");
                Assert.assertEquals(tx.getQueries().size(), 3);
                Assert.assertEquals(tx.getSavepoints().size(), 2);
            }
            conn.rollback();
            JdbcTransaction newTx = conn.getJdbcTrasaction();
            Assert.assertNotEquals(newTx, tx);
            Assert.assertNotNull(tx, "Should have transaction");
            Assert.assertEquals(tx.getQueries().size(), 0);
            Assert.assertEquals(tx.getSavepoints().size(), 0);
            Assert.assertNotNull(newTx, "Should have transaction");
            Assert.assertEquals(newTx.getQueries().size(), 0);
            Assert.assertEquals(newTx.getSavepoints().size(), 0);
            Assert.assertEquals(newTx.tx, conn.getTransaction());
            tx = newTx;

            try (ClickHouseStatement stmt = conn.createStatement()) {
                Savepoint s = conn.setSavepoint();
                stmt.execute("select 7; select 8");
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 1);
            }
            conn.rollback();
            newTx = conn.getJdbcTrasaction();
            Assert.assertNotEquals(newTx, tx);
            Assert.assertNotNull(tx, "Should have transaction");
            Assert.assertEquals(tx.getQueries().size(), 0);
            Assert.assertEquals(tx.getSavepoints().size(), 0);
            Assert.assertNotNull(newTx, "Should have transaction");
            Assert.assertEquals(newTx.getQueries().size(), 0);
            Assert.assertEquals(newTx.getSavepoints().size(), 0);
            Assert.assertEquals(newTx.tx, conn.getTransaction());
        }
    }

    @Test(groups = "integration")
    public void testParse() throws SQLException {
        Properties props = new Properties();
        String sql = "delete from table where column=1";
        boolean supportsLightWeightDelete = false;
        try (ClickHouseConnection conn = newConnection(props)) {
            ClickHouseSqlStatement[] stmts = conn.parse(sql, conn.getConfig(), null);
            Assert.assertEquals(stmts.length, 1);
            Assert.assertEquals(stmts[0].getSQL(), conn.getServerVersion().check("[23.3,)") ? sql
                    : "ALTER TABLE `table` DELETE where column=1 SETTINGS mutations_sync=1");
            if (conn.getServerVersion().check("[22.8,)")) {
                supportsLightWeightDelete = true;
            }
        }

        if (!supportsLightWeightDelete) {
            return;
        }

        props.setProperty("custom_settings", "allow_experimental_lightweight_delete=1");
        try (ClickHouseConnection conn = newConnection(props)) {
            ClickHouseSqlStatement[] stmts = conn.parse(sql, conn.getConfig(), null);
            Assert.assertEquals(stmts.length, 1);
            Assert.assertEquals(stmts[0].getSQL(), conn.getServerVersion().check("[23.3,)") ? sql
                    : "ALTER TABLE `table` DELETE where column=1 SETTINGS mutations_sync=1");

            stmts = conn.parse(sql, conn.getConfig(), conn.unwrap(ClickHouseRequest.class).getSettings());
            Assert.assertEquals(stmts.length, 1);
            Assert.assertEquals(stmts[0].getSQL(), sql);
        }
    }

    @Test(groups = "integration")
    public void testSwitchAutoCommit() throws SQLException {
        if (isCloud()) return; //TODO: testSwitchAutoCommit - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        try (ClickHouseConnection conn = newConnection()) {
            Assert.assertEquals(conn.getAutoCommit(), true);
            conn.setAutoCommit(false);
            Assert.assertEquals(conn.getAutoCommit(), false);
            conn.setAutoCommit(true);
            Assert.assertEquals(conn.getAutoCommit(), true);
            conn.setAutoCommit(false);
            Assert.assertEquals(conn.getAutoCommit(), false);
        }
    }
}
