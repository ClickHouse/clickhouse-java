package com.clickhouse.jdbc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

public class ResultSetImplTest extends JdbcIntegrationTest {

    @Test(groups = "integration")
    public void shouldReturnColumnIndex() throws SQLException {
        runQuery("CREATE TABLE rs_test_data (id UInt32, val UInt8) ENGINE = MergeTree ORDER BY (id)");
        runQuery("INSERT INTO rs_test_data VALUES (1, 10), (2, 20)");

        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM rs_test_data ORDER BY id")) {
                    assertTrue(rs.next());
                    assertEquals(rs.findColumn("id"), 1);
                    assertEquals(rs.getInt(1), 1);
                    assertEquals(rs.findColumn("val"), 2);
                    assertEquals(rs.getInt(2), 10);

                    assertTrue(rs.next());
                    assertEquals(rs.findColumn("id"), 1);
                    assertEquals(rs.getInt(1), 2);
                    assertEquals(rs.findColumn("val"), 2);
                    assertEquals(rs.getInt(2), 20);
                }
            }
        }
    }
}
