package com.clickhouse.examples.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;

import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseWriter;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class Basic {
    static final String TABLE_NAME = "jdbc_example_basic";

    private static Connection getConnection(String url) throws SQLException {
        return getConnection(url, new Properties());
    }

    private static Connection getConnection(String url, Properties properties) throws SQLException {
        final Connection conn;
        // Driver driver = new ClickHouseDriver();
        // conn = driver.connect(url, properties);

        // ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        // conn = dataSource.getConnection();

        conn = DriverManager.getConnection(url, properties);
        System.out.println("Connected to: " + conn.getMetaData().getURL());
        return conn;
    }

    static int dropAndCreateTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("DROP TABLE IF EXISTS %1$s",TABLE_NAME));
            stmt.execute(String.format("CREATE TABLE %1$s(a String, b Nullable(String)) ENGINE=MergeTree ORDER BY ()", TABLE_NAME));
            return stmt.getUpdateCount();
        }
    }

    static int batchInsert(Connection conn) throws SQLException {
        // 1. NOT recommended when inserting lots of rows, because it's based on a large
        // statement
        String sql = String.format("INSERT INTO %s VALUES(? || ' - 1', ?)", TABLE_NAME);
        int count = 0;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, "a");
            ps.setString(2, "b");
            ps.addBatch();
            ps.setString(1, "c");
            ps.setString(2, null);
            ps.addBatch();

            // same as below query:
            // insert into <table> values ('a' || ' - 1', 'b'), ('c' || ' - 1', null)
            for (int i : ps.executeBatch()) {
                if (i > 0) {
                    count += i;
                }
            }
        }

        return count;
    }

    static int connectWithCustomSettings(String url) throws SQLException {
        // comma separated settings
        String customSettings = "session_check=0,max_query_size=3000";
        Properties properties = new Properties();
        // properties.setProperty(ClickHouseClientOption.CUSTOM_SETTINGS.getKey(),
        // customSettings);
        properties.setProperty("custom_settings", customSettings);
        try (Connection conn = getConnection(url, properties);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select 5")) {
            return rs.next() ? rs.getInt(1) : -1;
        }
    }

    static int query(Connection conn) throws SQLException {
        String sql = "select * from " + TABLE_NAME;
        try (Statement stmt = conn.createStatement()) {
            // set max_result_rows = 3, result_overflow_mode = 'break'
            // or simply discard rows after the first 3 in read-only mode
            stmt.setMaxRows(3);
            int count = 0;
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    count++;
                }
            }
            return count;
        }
    }

    static void insertByteArray(Connection conn) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS t_map");
            s.execute("CREATE TABLE t_map"
                    + "("
                    + "    `audit_seq` Int64 CODEC(Delta(8), LZ4),"
                    + "`timestamp` Int64 CODEC(Delta(8), LZ4),"
                    + "`event_type` LowCardinality(String),"
                    + "`event_subtype` LowCardinality(String),"
                    + "`actor_type` LowCardinality(String),"
                    + "`actor_id` String,"
                    + "`actor_tenant_id` LowCardinality(String),"
                    + "`actor_tenant_name` String,"
                    + "`actor_firstname` String,"
                    + "`actor_lastname` String,"
                    + "`resource_type` LowCardinality(String),"
                    + "`resource_id` String,"
                    + "`resource_container` LowCardinality(String),"
                    + "`resource_path` String,"
                    + "`origin_ip` String,"
                    + "`origin_app_name` LowCardinality(String),"
                    + "`origin_app_instance` String,"
                    + "`description` String,"
                    + "`attributes` Map(String, String)"
                    + ")"
                    + "ENGINE = MergeTree "
                    + "ORDER BY (resource_container, event_type, event_subtype) "
                    + "SETTINGS index_granularity = 8192");
            try (PreparedStatement stmt = conn.prepareStatement(
                    "INSERT INTO t_map SETTINGS async_insert=1,wait_for_async_insert=1 VALUES " +
                            "(8481365034795008,1673349039830,'operation-9','a','service', 'bc3e47b8-2b34-4c1a-9004-123656fa0000','b', 'c', 'service-56','d', 'object','e', 'my-value-62', 'mypath', 'some.hostname.address.com', 'app-9', 'instance-6','x', ?)")) {
                stmt.setObject(1, Collections.singletonMap("key1", "value1"));
                stmt.execute();

                try (ResultSet rs = s.executeQuery("select attributes from t_map")) {
                    System.out.println(rs.next());
                    System.out.println(rs.getObject(1));
                }
            }
        }
    }

    static void usedPooledConnection(String url) throws SQLException {
        // connection pooling won't help much in terms of performance,
        // because the underlying implementation has its own pool.
        // for example: HttpURLConnection has a pool for sockets
        HikariConfig poolConfig = new HikariConfig();
        poolConfig.setConnectionTimeout(5000L);
        poolConfig.setMaximumPoolSize(20);
        poolConfig.setMaxLifetime(300_000L);
        poolConfig.setDataSource(new ClickHouseDataSource(url));

        HikariDataSource ds = new HikariDataSource(poolConfig);

        try (Connection conn = ds.getConnection();
                Statement s = conn.createStatement();
                ResultSet rs = s.executeQuery("select 123")) {
            System.out.println(rs.next());
            System.out.println(rs.getInt(1));
        }
    }

    public static void main(String[] args) {
        // jdbc:ch:https://explorer@play.clickhouse.com:443
        // jdbc:ch:https://demo:demo@github.demo.trial.altinity.cloud
        String url = System.getProperty("chUrl", "jdbc:ch://localhost");

        try {
            usedPooledConnection(url);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println(url);
        try (Connection conn = getConnection(url)) {
            connectWithCustomSettings(url);
            insertByteArray(conn);

            System.out.println("Update Count: " + dropAndCreateTable(conn));
            System.out.println("Inserted Rows: " + batchInsert(conn));
            System.out.println("Result Rows: " + query(conn));

        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Done!");
    }
}
