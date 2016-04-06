package ru.yandex.metrika.clickhouse;

import ru.yandex.metrika.clickhouse.copypaste.CHProperties;
import ru.yandex.metrika.clickhouse.util.LogProxy;
import ru.yandex.metrika.clickhouse.util.Logger;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

    private Map<CHConnectionImpl, Boolean> connections = new WeakHashMap<CHConnectionImpl, Boolean>();

    private ScheduledExecutorService connectionsCleaner = Executors.newSingleThreadScheduledExecutor();

    CHDriver(){
        // https://hc.apache.org/httpcomponents-client-4.5.x/tutorial/html/connmgmt.html#d5e418
        connectionsCleaner.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                for (CHConnectionImpl connection : connections.keySet()) {
                    connection.cleanConnections();
                }
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

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
        CHConnectionImpl connection = new CHConnectionImpl(url, info);
        connections.put(connection, true);
        return LogProxy.wrap(CHConnection.class, connection);
    }

    public CHConnection connect(String url, CHProperties properties) throws SQLException {
        logger.info("Creating connection");
        CHConnectionImpl connection = new CHConnectionImpl(url, properties);
        connections.put(connection, true);
        return LogProxy.wrap(CHConnection.class, connection);
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
