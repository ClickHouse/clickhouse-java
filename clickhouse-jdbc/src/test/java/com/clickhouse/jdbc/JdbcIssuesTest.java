package com.clickhouse.jdbc;

import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JdbcIssuesTest extends JdbcIntegrationTest {
    @BeforeMethod(groups = "integration")
    public void setV1() {
        System.setProperty("clickhouse.jdbc.v1","true");
    }
    @Test(groups = "integration")
    public void test01Decompress() throws SQLException {
        String TABLE_NAME = "decompress_issue_01";
        Properties prop = new Properties();
        prop.setProperty("decompress", "true");
        prop.setProperty("decompress_algorithm", "lz4");
        String url = String.format("jdbc:ch:%s", getEndpointString(true));
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, prop);
        String columnNames = "event_id";
        String columnValues = "('event_id String')";
        String sql = String.format("INSERT INTO %s (%s) SELECT %s FROM input %s", TABLE_NAME, columnNames, columnNames, columnValues);

        Connection conn = dataSource.getConnection("default", getPassword());
        Statement st = conn.createStatement();
        st.execute(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        st.execute(String.format("CREATE TABLE %s (`event_id` String) ENGINE = Memory", TABLE_NAME));

        int count = 1;
        boolean failed = false;
        while (count <= 100000) {
            String content = StringUtils.repeat("*", count);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, content);
                ps.addBatch();
                ps.executeBatch();
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                failed = true;
            }
            Assert.assertFalse(failed, String.format("Failed when content size %d", count));
            count *= 2;
        }
    }


    @Test
    public void test02Decompress() throws SQLException {
        String TABLE_NAME = "decompress_issue_02";
        Properties prop = new Properties();
        prop.setProperty("decompress", "true");
        prop.setProperty("decompress_algorithm", "lz4");
        String url = String.format("jdbc:ch:%s", getEndpointString(true));
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, prop);
        String columnNames = "event_id";
        String columnValues = "('event_id String')";
        String sql = String.format("INSERT INTO %s (%s) SELECT %s FROM input %s", TABLE_NAME, columnNames, columnNames, columnValues);

        Connection conn = dataSource.getConnection("default", getPassword());
        Statement st = conn.createStatement();
        st.execute(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        st.execute(String.format("CREATE TABLE %s (`event_id` String) ENGINE = Memory", TABLE_NAME));

        int count = 1;
        boolean failed = false;

        String content = StringUtils.repeat("*", count);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            while (count <= 100000) {
                ps.setString(1, content);
                ps.addBatch();
                count *= 2;
            }
            ps.executeBatch();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            failed = true;
        }
        Assert.assertFalse(failed, String.format("Failed when content size %d", count));
    }
    @Test
    public void test03Decompress() throws SQLException {
        String TABLE_NAME = "decompress_issue_03";
        Properties prop = new Properties();
        prop.setProperty("decompress", "true");
        prop.setProperty("decompress_algorithm", "lz4");
        String url = String.format("jdbc:ch:%s", getEndpointString(true));
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, prop);
        String columnNames = "event_id, num01,event_id_01 ";
        String columnValues = "('event_id String, num01 Int8, event_id_01 String')";
        String sql = String.format("INSERT INTO %s (%s) SELECT %s FROM input %s", TABLE_NAME, columnNames, columnNames, columnValues);

        Connection conn = dataSource.getConnection("default", getPassword());
        Statement st = conn.createStatement();
        st.execute(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        st.execute(String.format("CREATE TABLE %s (`event_id` String, `num01` Int8, `event_id_01` String) ENGINE = Memory", TABLE_NAME));

        int count = 1;
        boolean failed = false;

        String content = StringUtils.repeat("*", 50000);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            while (count <= 3) {
                ps.setString(1, content);
                ps.setInt(2, 10);
                ps.setString(3, content);
                ps.addBatch();
                count += 1;
            }
            ps.executeBatch();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            failed = true;
        }
        Assert.assertFalse(failed, String.format("Failed when content size %d", count));
    }
    @Test
    public void testIssue1373() throws SQLException {
        String TABLE_NAME = "issue_1373";
        String url = String.format("jdbc:ch:%s", getEndpointString(true));
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, new Properties());
        String columnNames = "event_id, num01,event_id_01 ";
        String columnValues = "('event_id String, num01 Int8, event_id_01 String')";
        String sql = String.format("INSERT INTO %s (%s) SELECT %s FROM input %s", TABLE_NAME, columnNames, columnNames, columnValues);
        Connection conn = dataSource.getConnection("default", getPassword());
        Statement st = conn.createStatement();
        st.execute(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        st.execute(String.format("CREATE TABLE %s (`event_id` String, `num01` Int8, `event_id_01` String) ENGINE = Memory", TABLE_NAME));
        int count = 1;
        boolean failed = false;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            while (count <= 10) {
                try {
                    ps.setString(1, "******");
                    ps.setInt(2, 10);
                    ps.setString( 3, count == 2 ? null : "--------");
                    ps.addBatch();
                } catch (Exception e) {
                    //e.printStackTrace();
                }
                count += 1;
            }
            ps.executeBatch();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            failed = true;
        }
        Assert.assertFalse(failed, String.format("executeBatch got exception"));
    }
}
