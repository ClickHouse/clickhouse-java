package com.clickhouse.jdbc.internal;

import java.sql.SQLException;
import java.sql.Savepoint;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseStatement;
import com.clickhouse.jdbc.JdbcIntegrationTest;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseConnectionImplTest extends JdbcIntegrationTest {
    @Test(groups = "integration")
    public void testManualCommit() throws SQLException {
        try (ClickHouseConnectionImpl conn = (ClickHouseConnectionImpl) newConnection()) {
            Assert.assertEquals(conn.getAutoCommit(), true);
            Assert.assertNull(conn.getTransaction(), "Should NOT have any transaction");
            conn.setAutoCommit(false);
            Assert.assertEquals(conn.getAutoCommit(), false);
            FakeTransaction tx = conn.getTransaction();
            Assert.assertNotNull(tx, "Should have transaction");
            Assert.assertEquals(tx.getQueries().size(), 0);
            Assert.assertEquals(tx.getSavepoints().size(), 0);
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
            FakeTransaction newTx = conn.getTransaction();
            Assert.assertNotEquals(newTx, tx);
            Assert.assertNotNull(tx, "Should have transaction");
            Assert.assertEquals(tx.getQueries().size(), 0);
            Assert.assertEquals(tx.getSavepoints().size(), 0);
            Assert.assertNotNull(newTx, "Should have transaction");
            Assert.assertEquals(newTx.getQueries().size(), 0);
            Assert.assertEquals(newTx.getSavepoints().size(), 0);
            tx = newTx;

            try (ClickHouseStatement stmt = conn.createStatement()) {
                Savepoint s = conn.setSavepoint();
                stmt.execute("select 7; select 8");
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 1);
            }
            conn.commit();
            newTx = conn.getTransaction();
            Assert.assertNotEquals(newTx, tx);
            Assert.assertNotNull(tx, "Should have transaction");
            Assert.assertEquals(tx.getQueries().size(), 0);
            Assert.assertEquals(tx.getSavepoints().size(), 0);
            Assert.assertNotNull(newTx, "Should have transaction");
            Assert.assertEquals(newTx.getQueries().size(), 0);
            Assert.assertEquals(newTx.getSavepoints().size(), 0);
        }
    }

    @Test(groups = "integration")
    public void testManualRollback() throws SQLException {
        try (ClickHouseConnectionImpl conn = (ClickHouseConnectionImpl) newConnection()) {
            Assert.assertEquals(conn.getAutoCommit(), true);
            Assert.assertNull(conn.getTransaction(), "Should NOT have any transaction");
            conn.setAutoCommit(false);
            Assert.assertEquals(conn.getAutoCommit(), false);
            FakeTransaction tx = conn.getTransaction();
            Assert.assertNotNull(tx, "Should have transaction");
            Assert.assertEquals(tx.getQueries().size(), 0);
            Assert.assertEquals(tx.getSavepoints().size(), 0);
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
            FakeTransaction newTx = conn.getTransaction();
            Assert.assertNotEquals(newTx, tx);
            Assert.assertNotNull(tx, "Should have transaction");
            Assert.assertEquals(tx.getQueries().size(), 0);
            Assert.assertEquals(tx.getSavepoints().size(), 0);
            Assert.assertNotNull(newTx, "Should have transaction");
            Assert.assertEquals(newTx.getQueries().size(), 0);
            Assert.assertEquals(newTx.getSavepoints().size(), 0);
            tx = newTx;

            try (ClickHouseStatement stmt = conn.createStatement()) {
                Savepoint s = conn.setSavepoint();
                stmt.execute("select 7; select 8");
                Assert.assertEquals(tx.getQueries().size(), 2);
                Assert.assertEquals(tx.getSavepoints().size(), 1);
            }
            conn.rollback();
            newTx = conn.getTransaction();
            Assert.assertNotEquals(newTx, tx);
            Assert.assertNotNull(tx, "Should have transaction");
            Assert.assertEquals(tx.getQueries().size(), 0);
            Assert.assertEquals(tx.getSavepoints().size(), 0);
            Assert.assertNotNull(newTx, "Should have transaction");
            Assert.assertEquals(newTx.getQueries().size(), 0);
            Assert.assertEquals(newTx.getSavepoints().size(), 0);
        }
    }

    @Test(groups = "integration")
    public void testSwitchAutoCommit() throws SQLException {
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
