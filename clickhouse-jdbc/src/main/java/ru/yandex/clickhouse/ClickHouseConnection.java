package ru.yandex.clickhouse;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.TimeZone;


public interface ClickHouseConnection extends Connection {
    TimeZone getServerTimeZone();
    
    TimeZone getTimeZone();

    @Override
    ClickHouseStatement createStatement() throws SQLException;

    @Override
    ClickHouseStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException;

    String getServerVersion() throws SQLException;
}
