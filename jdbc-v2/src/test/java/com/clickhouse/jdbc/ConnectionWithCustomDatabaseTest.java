/*
 * Copyright (c) 2025 Riege Software. All rights reserved.
 * Use is subject to license terms.
 */
package com.clickhouse.jdbc;

import java.sql.Connection;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ConnectionWithCustomDatabaseTest extends JdbcIntegrationTest {

    @Test(groups = { "integration" })
    public void testConnectionWithDashDatabaseName() throws Exception {
        Connection conn = this.getJdbcConnection();
        java.sql.ResultSet rs = conn.createStatement().executeQuery("SELECT 1");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getInt(1));
    }

    protected static String getDatabase() {
        return "with-dashes";
    }

}
