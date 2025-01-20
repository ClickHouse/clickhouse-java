package com.clickhouse.examples.jdbc;


import com.clickhouse.client.api.ClientException;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;

public class Basic {
    static final String TABLE_NAME = "jdbc_example_basic";

    public static void main(String[] args) {
        String url = System.getProperty("chUrl", "jdbc:ch://localhost:18123");

        // Set user and password if needed
        Properties properties = new Properties();
        properties.setProperty("user", System.getProperty("chUser", "default"));
        properties.setProperty("password", System.getProperty("chPassword", ""));

        try (Connection conn = DriverManager.getConnection(url, properties)) {//Grab a connection using the jdbc DriverManager
            try (Statement stmt = conn.createStatement()) {//Create a statement
                stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);//Execute a query to drop the table if it exists
                stmt.execute("CREATE TABLE " + TABLE_NAME + " (`date` Date, `id` UInt32, `name` String, `attributes` Map(String, String)) " +
                        "ENGINE = MergeTree() ORDER BY id");//Create a table with three columns: date, id, and name

                try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + TABLE_NAME + " VALUES(?, ?, ?, ?)")) {//Create a prepared statement
                    pstmt.setDate(1, Date.valueOf("2025-01-01"));//Set the first parameter to '2025-01-01' (using java.sql.Date)
                    pstmt.setInt(2, 1);//Set the second parameter to 1
                    pstmt.setString(3, "Alice");//Set the third parameter to "Alice"
                    pstmt.setObject(4, Collections.singletonMap("key1", "value1"));
                    pstmt.addBatch();//Add the current parameters to the batch

                    pstmt.setDate(1, Date.valueOf("2025-02-01"));//Set the first parameter to '2025-02-01'
                    pstmt.setInt(2, 2);//Set the second parameter to 2
                    pstmt.setString(3, "Bob");//Set the third parameter to "Bob"
                    pstmt.setObject(4, Collections.singletonMap("key2", "value2"));
                    pstmt.addBatch();//Add the current parameters to the batch

                    pstmt.executeBatch();//Execute the batch
                }

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                    while (rs.next()) {
                        System.out.println(rs.getDate(1) + ", " + rs.getInt(2) + ", " + rs.getString(3) + ", " + rs.getObject(4));
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            //Using HikariCP with ClickHouseDataSource
            usedPooledConnection(url, properties);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            //Customizing client settings
            setClientSettings();
        } catch (ClientException e) {
            // Ignore
            System.out.println(e.getMessage());
        }
    }

    static void usedPooledConnection(String url, Properties properties) throws SQLException {
        // connection pooling won't help much in terms of performance,
        // because the underlying implementation has its own pool.
        // for example: HttpURLConnection has a pool for sockets
        HikariConfig poolConfig = new HikariConfig();
        poolConfig.setConnectionTimeout(5000L);
        poolConfig.setMaximumPoolSize(20);
        poolConfig.setMaxLifetime(300_000L);
        poolConfig.setDataSource(new ClickHouseDataSource(url, properties));

        try (HikariDataSource ds = new HikariDataSource(poolConfig);
             Connection conn = ds.getConnection();
             Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 123")) {
            System.out.println(rs.next());
            System.out.println(rs.getInt(1));
        }
    }

    static void setClientSettings() {
        String url = System.getProperty("chUrl", "jdbc:ch://localhost:18123?jdbc_ignore_unsupported_values=true&socket_timeout=1");

        // Set user and password if needed
        Properties properties = new Properties();
        properties.setProperty("user", System.getProperty("chUser", "default"));
        properties.setProperty("password", System.getProperty("chPassword", ""));

        try (Connection conn = DriverManager.getConnection(url, properties)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT sleep(3)")) {
                    // this will throw an exception
                    // because the query takes more than 1 second
                    // and the socket timeout is set to 1 second
                    System.out.println(rs.next());
                }
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
