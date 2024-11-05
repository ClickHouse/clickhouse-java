package com.clickhouse.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertNotNull;

import com.clickhouse.client.ClickHouseServerForTest;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class DataSourceTest extends JdbcIntegrationTest {
    private DataSourceImpl dataSource;

    @BeforeTest
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

    @Test
    public void testGetConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        assertNotNull(connection);
        connection.close();
    }

    @Test
    public void testGetConnectionWithUserAndPassword() throws SQLException {
        Connection connection = dataSource.getConnection("default", ClickHouseServerForTest.getPassword());
        assertNotNull(connection);
        connection.close();
    }

    @Test
    public void testGetLogWriter() {
        assertThrows(SQLFeatureNotSupportedException.class, () -> dataSource.getLogWriter());
    }

    @Test
    public void testSetLogWriter() {
        assertThrows(SQLFeatureNotSupportedException.class, () -> dataSource.setLogWriter(null));
    }

    @Test
    public void testSetLoginTimeout() {
        assertThrows(SQLFeatureNotSupportedException.class, () -> dataSource.setLoginTimeout(0));
    }

    @Test
    public void testGetLoginTimeout() {
        assertThrows(SQLFeatureNotSupportedException.class, () -> dataSource.getLoginTimeout());
    }

}
