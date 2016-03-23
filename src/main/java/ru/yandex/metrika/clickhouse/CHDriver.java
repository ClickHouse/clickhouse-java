package ru.yandex.metrika.clickhouse;

import ru.yandex.metrika.clickhouse.util.LogProxy;
import ru.yandex.metrika.clickhouse.util.Logger;

import java.sql.*;
import java.util.Properties;

/**
 *
 * URL Format
 *
 * пока что примитивный
 *
 * jdbc:clickhouse:host:port
 *
 * например, jdbc:clickhouse:localhost:8123
 *
 * Created by jkee on 14.03.15.
 */
public class CHDriver implements Driver {

    private static final Logger logger = Logger.of(CHDriver.class);

    static {
        CHDriver driver = new CHDriver();
        try {
            DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        logger.info("Driver registered");
    }

    @Override
    public CHConnection connect(String url, Properties info) throws SQLException {
        logger.info("Creating connection");
        return LogProxy.wrap(CHConnection.class, new CHConnectionImpl(url));
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith("jdbc:clickhouse:");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
