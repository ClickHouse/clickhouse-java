package com.clickhouse.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHousePreparedStatementTest extends JdbcIntegrationTest {
    @Test(groups = "integration")
    public void testBatchInsert() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                PreparedStatement stmt = conn.prepareStatement("insert into test_batch_insert values(?,?)")) {
            conn.createStatement().execute("drop table if exists test_batch_insert;"
                    + "create table test_batch_insert(id Int32, name Nullable(String))engine=Memory");
            stmt.setInt(1, 1);
            stmt.setString(2, "a");
            stmt.addBatch();
            stmt.setInt(1, 2);
            stmt.setString(2, "b");
            stmt.addBatch();
            stmt.setInt(1, 3);
            stmt.setString(2, null);
            stmt.addBatch();
            int[] results = stmt.executeBatch();
            Assert.assertEquals(results, new int[] { 0, 0, 0 });
        }
    }

    @Test(groups = "integration")
    public void testBatchInput() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                PreparedStatement stmt = conn.prepareStatement(
                        "insert into test_batch_input select id, name from input('id Int32, name Nullable(String), desc Nullable(String)')")) {
            conn.createStatement().execute("drop table if exists test_batch_input;"
                    + "create table test_batch_input(id Int32, name Nullable(String))engine=Memory");
            stmt.setInt(1, 1);
            stmt.setString(2, "a");
            stmt.setString(3, "aaaaa");
            stmt.addBatch();
            stmt.setInt(1, 2);
            stmt.setString(2, "b");
            stmt.setString(3, null);
            stmt.addBatch();
            stmt.setInt(1, 3);
            stmt.setString(2, null);
            stmt.setString(3, "33333");
            stmt.addBatch();
            int[] results = stmt.executeBatch();
            Assert.assertEquals(results, new int[] { 0, 0, 0 });
        }
    }

    @Test(groups = "integration")
    public void testBatchQuery() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                PreparedStatement stmt = conn.prepareStatement("select * from numbers(100) where number < ?")) {
            Assert.assertThrows(SQLException.class, () -> stmt.setInt(0, 5));
            Assert.assertThrows(SQLException.class, () -> stmt.setInt(2, 5));
            Assert.assertThrows(SQLException.class, () -> stmt.addBatch());

            stmt.setInt(1, 3);
            stmt.addBatch();
            stmt.setInt(1, 2);
            stmt.addBatch();
            int[] results = stmt.executeBatch();
            Assert.assertEquals(results, new int[] { 0, 0 });
        }
    }
}
