package com.clickhouse.examples.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Basic {
    static void dropAndCreateTable(String url, String user, String password, String table) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, user, password);
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    String.format(
                            "drop table if exists %1$s; create table %1$s(a String, b Nullable(String)) engine=Memory",
                            table));
        }
    }

    static void batchInsert(String url, String user, String password, String table) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            // not that fast as it's based on string substitution and large sql statement
            String sql = String.format("insert into %1$s values(?, ?)", table);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, "a");
                ps.setString(2, "b");
                ps.addBatch();
                ps.setString(1, "c");
                ps.setString(2, null);
                ps.addBatch();
                ps.executeBatch();
            }

            // faster when inserting massive data
            sql = String.format("insert into %1$s", table);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, "a");
                ps.setString(2, "b");
                ps.addBatch();
                ps.setString(1, "c");
                ps.setString(2, null);
                ps.addBatch();
                ps.executeBatch();
            }

            // fastest approach as it does not need to issue additional query for metadata
            sql = String.format("insert into %1$s select a, b from input('a String, b Nullable(String)')", table);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, "a");
                ps.setString(2, "b");
                ps.addBatch();
                ps.setString(1, "c");
                ps.setString(2, null);
                ps.addBatch();
                ps.executeBatch();
            }
        }
    }

    static int query(String url, String user, String password, String table) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select * from " + table)) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            return count;
        }
    }

    public static void main(String[] args) {
        String url = String.format("jdbc:ch://%s:%d/system", System.getProperty("chHost", "localhost"),
                Integer.parseInt(System.getProperty("chPort", "8123")));
        String user = System.getProperty("chUser", "default");
        String password = System.getProperty("chPassword", "");
        String table = "jdbc_example_basic";

        try {
            dropAndCreateTable(url, user, password, table);

            batchInsert(url, user, password, table);

            query(url, user, password, table);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
