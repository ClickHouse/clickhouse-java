package ru.yandex.clickhouse;

import java.sql.Connection;
import java.sql.SQLException;


public interface ClickHouseConnection extends Connection {
    ClickHouseStatement createClickHouseStatement() throws SQLException;
}
