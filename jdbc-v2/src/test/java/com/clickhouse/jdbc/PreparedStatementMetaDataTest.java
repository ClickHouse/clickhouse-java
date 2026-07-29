package com.clickhouse.jdbc;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;

import static org.testng.Assert.assertEquals;

public class PreparedStatementMetaDataTest extends JdbcIntegrationTest {

    @DataProvider(name = "trailingNoiseQueries")
    public static Object[][] trailingNoiseQueries() {
        return new Object[][] {
                { "SELECT 13 AS a WHERE 0" },
                { "SELECT 13 AS a WHERE 0 -- trailing comment" },
                { "SELECT 13 AS a WHERE 0 # trailing comment" },
                { "SELECT 13 AS a WHERE 0;" },
                { "SELECT 13 AS a WHERE 0; -- trailing comment" },
                { "SELECT 13 AS a WHERE 0;\n-- trailing comment" },
        };
    }

    @Test(groups = { "integration" }, dataProvider = "trailingNoiseQueries")
    public void testGetMetaDataBeforeExecution(String sql) throws Exception {
        try (Connection conn = getJdbcConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            ResultSetMetaData md = stmt.getMetaData();
            assertEquals(md.getColumnCount(), 1);
            assertEquals(md.getColumnName(1), "a");
        }
    }
}
