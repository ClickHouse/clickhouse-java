package ru.yandex.clickhouse;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.TimeZone;


public interface ClickHouseConnection extends Connection {
    ClickHouseStatement createClickHouseStatement() throws SQLException;
    TimeZone getTimeZone();
}
