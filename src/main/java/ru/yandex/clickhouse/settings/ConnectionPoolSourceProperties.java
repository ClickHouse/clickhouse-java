package ru.yandex.clickhouse.settings;


import javax.sql.DataSource;

public class ConnectionPoolSourceProperties {
    private final DataSource dataSource;
    private final int numberOfConnections;

    public ConnectionPoolSourceProperties(DataSource dataSource, int numberOfConnections) {
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
