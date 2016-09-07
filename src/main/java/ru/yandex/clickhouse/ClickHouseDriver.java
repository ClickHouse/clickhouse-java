package ru.yandex.clickhouse;

import com.google.common.collect.MapMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.LogProxy;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * URL Format
 *
 * primitive for now
 *
 * jdbc:clickhouse:host:port
 *
 * for example, jdbc:clickhouse:localhost:8123
 *
 */
public class ClickHouseDriver implements Driver {

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseDriver.class);


    private final ConcurrentMap<ClickHouseConnectionImpl, Boolean> connections = new MapMaker().weakKeys().makeMap();

    private final ScheduledExecutorService connectionsCleaner = Executors.newSingleThreadScheduledExecutor();

    static {
        ClickHouseDriver driver = new ClickHouseDriver();
        try {
            DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        logger.info("Driver registered");
    }

    @Override
    public ClickHouseConnection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        logger.info("Creating connection");
        ClickHouseConnectionImpl connection = new ClickHouseConnectionImpl(url, info);
        registerConnection(connection);
        return LogProxy.wrap(ClickHouseConnection.class, connection);
    }

    public ClickHouseConnection connect(String url, ClickHouseProperties properties) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        logger.info("Creating connection");
        ClickHouseConnectionImpl connection = new ClickHouseConnectionImpl(url, properties);
        registerConnection(connection);
        return LogProxy.wrap(ClickHouseConnection.class, connection);
    }

    private synchronized void registerConnection(ClickHouseConnectionImpl connection) {
        connections.put(connection, true);
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

    /**
     * Schedules connections cleaning at a rate. Turned off by default.
     * See https://hc.apache.org/httpcomponents-client-4.5.x/tutorial/html/connmgmt.html#d5e418
     * @param rate
     * @param timeUnit
     */
    public void scheduleConnectionsCleaning(int rate, TimeUnit timeUnit){
        connectionsCleaner.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    for (ClickHouseConnectionImpl connection : connections.keySet()) {
                        connection.cleanConnections();
                    }
                } catch (Exception e){
                    logger.error("error evicting connections: " + e);
                }
            }
        }, 0, rate, timeUnit);
    }
}
