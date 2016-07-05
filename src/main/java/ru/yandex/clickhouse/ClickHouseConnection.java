package ru.yandex.clickhouse;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author serebrserg
 * @since 22.03.16
 */
public interface ClickHouseConnection extends Connection {
    ClickHouseStatement createClickHouseStatement() throws SQLException;
}
