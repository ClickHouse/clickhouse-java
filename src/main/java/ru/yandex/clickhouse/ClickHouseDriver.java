package ru.yandex.clickhouse;

import com.google.common.collect.MapMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.clickhouse.settings.ClickHouseConnectionSettings;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import ru.yandex.clickhouse.settings.DriverPropertyCreator;
import ru.yandex.clickhouse.util.LogProxy;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
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
 * jdbc:clickhouse://host:port
 *
 * for example, jdbc:clickhouse://localhost:8123
 *
 */
public class ClickHouseDriver implements Driver {

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseDriver.class);

    private static final ConcurrentMap<ClickHouseConnectionImpl, Boolean> connections = new MapMaker().weakKeys().makeMap();

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
        return connect(url, new ClickHouseProperties(info));
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

    private void registerConnection(ClickHouseConnectionImpl connection) {
        connections.put(connection, true);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        Properties copy = new Properties(info);
        Properties properties;
        try {
            properties = ClickhouseJdbcUrlParser.parse(url, copy).asProperties();
        } catch (Exception ex) {
            properties = copy;
            logger.error("could not parse url {}", url, ex);
        }
        List<DriverPropertyInfo> result = new ArrayList<DriverPropertyInfo>(ClickHouseQueryParam.values().length
                + ClickHouseConnectionSettings.values().length);
        result.addAll(dumpProperties(ClickHouseQueryParam.values(), properties));
        result.addAll(dumpProperties(ClickHouseConnectionSettings.values(), properties));
        return result.toArray(new DriverPropertyInfo[result.size()]);
    }

    private List<DriverPropertyInfo> dumpProperties(DriverPropertyCreator creators[], Properties info) {
        List<DriverPropertyInfo> result = new ArrayList<DriverPropertyInfo>(creators.length);
        for (int i = 0; i < creators.length; ++i) {
            result.add(creators[i].createDriverPropertyInfo(info));
        }
        return result;
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
        ScheduledConnectionCleaner.INSTANCE.scheduleAtFixedRate(new Runnable() {
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

    private static class ScheduledConnectionCleaner {
        private static final ScheduledExecutorService INSTANCE = Executors.newSingleThreadScheduledExecutor();
    }
}
