package ru.yandex.clickhouse;

import com.google.common.collect.Lists;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConnectionPool {
    private final static int DEFAULT_NUMBER_OF_CONNECTIONS = 100;
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(ConnectionPool.class);

    private final List<ConnectionPoolSourceConfig> configs;
    private List<Connection> connections;
    private int index = 0;

    private List<Connection> fillConnections(final List<ConnectionPoolSourceConfig> configs) {
        List<Connection> res = new ArrayList<Connection>();
        for (final ConnectionPoolSourceConfig conf : configs) {
            try {
                for (int i = 0; i < conf.getNumberOfConnections(); ++i) {
                    res.add(conf.getDataSource().getConnection());
                }
            } catch (Exception e) {
                log.warn("Unable to create connection from data source: {}", conf);
            }
        }
        return res;
    }

    public ConnectionPool(final List<ConnectionPoolSourceConfig> configs) {
        this.configs = configs;
        connections = fillConnections(configs);
    }

    public ConnectionPool(final List<DataSource> sources, final int numberOfConnections) {
        final List<ConnectionPoolSourceConfig> tmp_conf = new ArrayList<ConnectionPoolSourceConfig>(sources.size());
        for (final DataSource source: sources) {
            tmp_conf.add(new ConnectionPoolSourceConfig(source, numberOfConnections));
        }
        configs = tmp_conf;
        connections = fillConnections(configs);
    }

    public ConnectionPool(DataSource source, final int numberOfConnections) {
        this(Lists.newArrayList(new ConnectionPoolSourceConfig(source, numberOfConnections)));
    }

    public ConnectionPool(DataSource source) {
        this(source, DEFAULT_NUMBER_OF_CONNECTIONS);
    }

    public void actualize() {
        List<Connection> aliveConnections = new ArrayList<Connection>();
        for (final Connection conn: connections) {
            try {
                conn.createStatement().execute("SELECT 1");
                aliveConnections.add(conn);
            } catch (SQLException e) {
                log.info("Found dead connection: ", e);
            }
        }
        connections = aliveConnections;
    }

    public void refresh() {
        connections = fillConnections(configs);
        actualize();
    }

    Connection getConnection() {
        if (connections.size() <= 0) {
            // ClickHouseException?
            throw new RuntimeException("There is no connections in connection pool");
        }
        Connection res;
        synchronized(this) {
            if (index >= connections.size()) {
                index = 0;
            }
            res = connections.get(index);
            index = (index + 1) % connections.size();
        }
        return res;
    }

    public void scheduleActualization(int rate, TimeUnit timeUnit){
        ClickHouseDriver.ScheduledConnectionCleaner.INSTANCE.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    actualize();
                } catch (Exception e){
                    log.error("Unable to actualize urls: " + e);
                }
            }
        }, 0, rate, timeUnit);
    }

    public void scheduleRefresh(int rate, TimeUnit timeUnit){
        ClickHouseDriver.ScheduledConnectionCleaner.INSTANCE.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    refresh();
                } catch (Exception e){
                    log.error("Unable to actualize urls: " + e);
                }
            }
        }, 0, rate, timeUnit);
    }
}
