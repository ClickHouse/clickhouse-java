package ru.yandex.clickhouse;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author serebrserg
 * @since 22.03.16
 */
public interface CHConnection extends Connection {
    CHStatement createCHStatement() throws SQLException;
}
