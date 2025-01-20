package com.clickhouse.examples.jdbc;


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
    }
}
