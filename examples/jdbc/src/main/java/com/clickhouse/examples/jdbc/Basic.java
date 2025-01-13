package com.clickhouse.examples.jdbc;


import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
                stmt.execute("CREATE TABLE " + TABLE_NAME + " (date Date, id UInt32, name String) ENGINE = Memory");

                try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + TABLE_NAME + " VALUES(?, ?, ?)")) {//Create a prepared statement
                    pstmt.setDate(1, new Date(System.currentTimeMillis()));//Set the first parameter to the current date (using java.sql.Date)
                    pstmt.setInt(2, 1);//Set the second parameter to 1
                    pstmt.setString(3, "Alice");//Set the third parameter to "Alice"
                    pstmt.addBatch();//Add the current parameters to the batch

                    pstmt.setDate(1, new Date(System.currentTimeMillis()));//Set the first parameter to the current date (using java.sql.Date)
                    pstmt.setInt(2, 2);//Set the second parameter to 2
                    pstmt.setString(3, "Bob");//Set the third parameter to "Bob"
                    pstmt.addBatch();//Add the current parameters to the batch

                    pstmt.executeBatch();//Execute the batch
                }

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                    while (rs.next()) {
                        System.out.println(rs.getDate(1) + ", " + rs.getInt(2) + ", " + rs.getString(3));
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
