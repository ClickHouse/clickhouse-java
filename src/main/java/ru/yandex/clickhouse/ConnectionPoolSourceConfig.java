package ru.yandex.clickhouse;


import javax.sql.DataSource;

public class ConnectionPoolSourceConfig {
    private final DataSource dataSource;
    private final int numberOfConnections;

    public ConnectionPoolSourceConfig(DataSource dataSource, int numberOfConnections) {
        this.dataSource = dataSource;
        this.numberOfConnections = numberOfConnections;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public int getNumberOfConnections() {
        return numberOfConnections;
    }
}
