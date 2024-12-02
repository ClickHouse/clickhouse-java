package com.clickhouse.jdbc;

import com.clickhouse.client.ClickHouseServerForTest;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;


public class DataSourceTest extends JdbcIntegrationTest {
    private DataSourceImpl dataSource;

    @BeforeTest(groups = { "integration" })
    public void setUp() {
        dataSource = new DataSourceImpl();
        dataSource.setUrl(getEndpointString());
        Properties info = new Properties();
        info.setProperty("user", "default");
        info.setProperty("password", ClickHouseServerForTest.getPassword());
        dataSource.setProperties(info);
    }

    @AfterTest
    public void tearDown() {
        dataSource = null;
    }

    @Test(groups = { "integration" })
    public void testGetConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        assertNotNull(connection);
        connection.close();
    }

    @Test(groups = { "integration" })
    public void testGetConnectionWithUserAndPassword() throws SQLException {
        Connection connection = dataSource.getConnection("default", ClickHouseServerForTest.getPassword());
        assertNotNull(connection);
        connection.close();
    }

    @Test(groups = { "integration" })
    public void testGetLogWriter() throws SQLException {
        assertNull(dataSource.getLogWriter());
    }

    @Test(groups = { "integration" })
    public void testSetLogWriter() throws SQLException {
        dataSource.setLogWriter(null);
    }

    @Test(groups = { "integration" })
    public void testSetLoginTimeout() {
        assertThrows(SQLFeatureNotSupportedException.class, () -> dataSource.setLoginTimeout(0));
    }

    @Test(groups = { "integration" })
    public void testGetLoginTimeout() {
        assertThrows(SQLFeatureNotSupportedException.class, () -> dataSource.getLoginTimeout());
    }

}
