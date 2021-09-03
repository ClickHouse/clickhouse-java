package ru.yandex.clickhouse;

import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.TimeZone;


public interface ClickHouseConnection extends Connection {
    TimeZone getServerTimeZone();
    
    TimeZone getTimeZone();

    ClickHouseProperties getProperties();

    @Override
    ClickHouseStatement createStatement() throws SQLException;

    @Override
    ClickHouseStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException;

    String getServerVersion() throws SQLException;
}
