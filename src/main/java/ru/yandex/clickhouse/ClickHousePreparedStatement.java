package ru.yandex.clickhouse;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;


public interface ClickHousePreparedStatement extends PreparedStatement {

    void setArray(int parameterIndex, Collection collection) throws SQLException;

    void setArray(int parameterIndex, Object[] array) throws SQLException;
}
