package ru.yandex.clickhouse;

import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.settings.ConnectionPoolSourceProperties;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConnectionPool {
    private final static int DEFAULT_NUMBER_OF_CONNECTIONS = 100;
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(ConnectionPool.class);

    private final List<ConnectionPoolSourceProperties> configs;
    private volatile List<Connection> connections;
    private volatile int currentIndex = 0;

    public ConnectionPool(final List<ConnectionPoolSourceProperties> configs) {
        this.configs = configs;
        this.connections = fillConnections();
    }

    public ConnectionPool(final List<DataSource> sources, final int numberOfConnections) {
        final List<ConnectionPoolSourceProperties> localConfig = new ArrayList<ConnectionPoolSourceProperties>(sources.size());
        for (final DataSource source : sources) {
            localConfig.add(new ConnectionPoolSourceProperties(source, numberOfConnections));
        }

        this.configs = localConfig;
        this.connections = fillConnections();
    }

    public ConnectionPool(DataSource source, final int numberOfConnections) {
        this(Collections.singletonList(new ConnectionPoolSourceProperties(source, numberOfConnections)));
    }

    public ConnectionPool(DataSource source) {
        this(source, DEFAULT_NUMBER_OF_CONNECTIONS);
    }

    private List<Connection> fillConnections() {
        List<Connection> connectionList = new ArrayList<Connection>();
        for (final ConnectionPoolSourceProperties conf : configs) {
            try {
                for (int i = 0; i < conf.getNumberOfConnections(); ++i) {
                    connectionList.add(conf.getDataSource().getConnection());
                }
            } catch (Exception e) {
                log.warn("Unable to create connection from data source: {}", conf);
            }
        }

        return connectionList;
    }

    void actualize() {
        List<Connection> aliveConnections = new ArrayList<Connection>();
        List<Connection> localConnectionList = connections;
        for (final Connection conn : localConnectionList) {
            try {
                conn.createStatement().execute("SELECT 1");
                aliveConnections.add(conn);
            } catch (SQLException e) {
                log.info("Found dead connection: ", e);
            }
        }

        this.connections = aliveConnections;
    }

    void refresh() {
        this.connections = fillConnections();
        actualize();
    }

    Connection getConnection() {
        List<Connection> localConnectionList = connections;
        if (localConnectionList.isEmpty()) {
            // ClickHouseException?
            throw new RuntimeException("There are no connections in connection pool");
        }
        currentIndex++;
        int index = currentIndex % localConnectionList.size();
        return localConnectionList.get(index);
    }

    public void scheduleActualization(int rate, TimeUnit timeUnit) {
        ClickHouseDriver.ScheduledConnectionCleaner.INSTANCE.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    actualize();
                } catch (Exception e) {
                    log.error("Unable to actualize urls", e);
                }
            }
        }, 0, rate, timeUnit);
    }

    public void scheduleRefresh(int rate, TimeUnit timeUnit) {
        ClickHouseDriver.ScheduledConnectionCleaner.INSTANCE.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    refresh();
                } catch (Exception e) {
                    log.error("Unable to actualize urls", e);
                }
            }
        }, 0, rate, timeUnit);
    }
}
