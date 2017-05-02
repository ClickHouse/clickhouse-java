package ru.yandex.clickhouse;

import org.testng.annotations.Test;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.logging.Logger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class ConnectionPoolTest {

    // Replace with Mockito?
    private abstract class FakeSource implements DataSource {
        @Override
        public Connection getConnection(String s, String s1) throws SQLException {
            return null;
        }

        @Override
        public <T> T unwrap(Class<T> aClass) throws SQLException {
            return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> aClass) throws SQLException {
            return false;
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter printWriter) throws SQLException {

        }

        @Override
        public void setLoginTimeout(int i) throws SQLException {

        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return 0;
        }

        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return null;
        }
    }


    @Test
    public void testConnectionPoolCreates() throws Exception {
        DataSource source = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", new ClickHouseProperties());
        ConnectionPool pool = new ConnectionPool(source, 10);

        for (int i = 0; i < 11; ++i) {
            ResultSet rs = pool.getConnection().createStatement().executeQuery("Select 1");
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    public void testConnectionPoolFromUnavailableHost() throws Exception {
        DataSource source = new ClickHouseDataSource("jdbc:clickhouse://deadbeaf:8123", new ClickHouseProperties());
        ConnectionPool pool = new ConnectionPool(source, 10);

        try {
            pool.getConnection();
            fail();
        } catch (Exception e) {
            // There is no enabled connections
        }
    }


    @Test
    public void testRefreshReturnsAwakenConnection() throws Exception {
        // Returned not working connection first 10 times
        DataSource source = new FakeSource() {
            DataSource source =
                    new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", new ClickHouseProperties());
            int index = 0;
            @Override
            public Connection getConnection() throws SQLException {
                index += 1;
                if (index <= 10) {
                    Connection conn = source.getConnection();
                    conn.close();
                    return conn;
                } else {
                    return source.getConnection();
                }
            }
        };


        // Creates pool with bad connections (connections to unawailable services)
        ConnectionPool pool = new ConnectionPool(source, 10);

        for (int i = 0; i < 11; ++i) {
            Statement statement = pool.getConnection().createStatement();
            try {
                statement.executeQuery("Select 1");
                fail();
            } catch (Exception e) {
                // There is no enabled connections
            }
        }

        // Actualize removes broken connections
        pool.actualize();

        try {
            pool.getConnection();
            fail();
        } catch (Exception e) {
            // There is no enabled connections
        }

        // Refresh will fill pool with new connections
        pool.refresh();
        for (int i = 0; i < 11; ++i) {
            ResultSet rs = pool.getConnection().createStatement().executeQuery("Select 1");
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
    }
}
