package com.clickhouse.examples.jdbc;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Properties;

public class Basic {
    private static final Logger log = LoggerFactory.getLogger(Basic.class);
    static final String TABLE_NAME = "jdbc_example_basic";

    public static void main(String[] args) {
        String url = System.getProperty("chUrl", "jdbc:ch://localhost:8123");

        // Set user and password if needed
        Properties properties = new Properties();
        properties.setProperty("user", System.getProperty("chUser", "default"));
        properties.setProperty("password", System.getProperty("chPassword", ""));

        try {
            createTable(url, properties);
            insertDateWithPreparedStatement(url, properties);
            printInsertedData(url, properties);

            //Customizing client settings
            setClientSettings(properties);

            //Using HikariCP with ClickHouseDataSource
            usedPooledConnection(url, properties);
        } catch (SQLException e) {
            log.error("Error", e);
        }
    }

    static void createTable(String url, Properties properties) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, properties)) {//Grab a connection using the jdbc DriverManager
            try (Statement stmt = conn.createStatement()) {//Create a statement
                stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);//Execute a query to drop the table if it exists
                stmt.execute("CREATE TABLE " + TABLE_NAME +
                        " (`date` DateTime64(3), `id` UInt32, `name` String, `attributes` Map(String, String))" +
                        " ENGINE = MergeTree() ORDER BY id");//Create a table with three columns: date, id, and name
            }
        }
    }

    static void insertDateWithPreparedStatement(String url, Properties properties) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, properties)) {//Grab a connection using the jdbc DriverManager
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + TABLE_NAME + " VALUES(?, ?, ?, ?)")) {//Create a prepared statement
                pstmt.setDate(1, Date.valueOf("2025-01-01"));//Set the first parameter to '2025-01-01' (using java.sql.Date)
                pstmt.setInt(2, 1);//Set the second parameter to 1
                pstmt.setString(3, "Alice");//Set the third parameter to "Alice"
                pstmt.setObject(4, Collections.singletonMap("key1", "value1"));
                pstmt.addBatch();//Add the current parameters to the batch

                pstmt.setObject(1, ZonedDateTime.now());
                pstmt.setInt(2, 2);//Set the second parameter to 2
                pstmt.setString(3, "Bob");//Set the third parameter to "Bob"
                pstmt.setObject(4, Collections.singletonMap("key2", "value2"));
                pstmt.addBatch();//Add the current parameters to the batch

                pstmt.executeBatch();//Execute the batch
            }
        }
    }

    static void printInsertedData(String url, Properties properties) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, properties)) {//Grab a connection using the jdbc DriverManager
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                    while (rs.next()) {
                        //Print the values of the current row
                        log.info("DateTime: {}, Int: {}, String: {}, Object: {}",
                                rs.getObject(1, ZonedDateTime.class), rs.getInt(2), rs.getString(3), rs.getObject(4));
                    }
                }
            }
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
             ResultSet rs = s.executeQuery("SELECT * FROM system.numbers LIMIT 3")) {
            while (rs.next()) {
                // handle row
                log.info("Integer: {}, String: {}", rs.getInt(1), rs.getString(1));//Same column but different types
            }
        }
    }

    static void setClientSettings(Properties properties){
        String url = System.getProperty("chUrl", "jdbc:ch://localhost:8123?jdbc_ignore_unsupported_values=true&socket_timeout=10");

        try (Connection conn = DriverManager.getConnection(url, properties)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT 1, 'Hello, world!'")) {
                    while(rs.next()) {
                        log.info("Integer: {}", rs.getInt(1));
                        log.info("String: {}", rs.getString(2));
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
