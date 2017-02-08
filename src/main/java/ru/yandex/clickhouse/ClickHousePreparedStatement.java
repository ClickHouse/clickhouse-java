package ru.yandex.clickhouse;

import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;


public interface ClickHousePreparedStatement extends PreparedStatement, ClickHouseStatement {

    void setArray(int parameterIndex, Collection collection) throws SQLException;

    void setArray(int parameterIndex, Object[] array) throws SQLException;

    ResultSet executeQuery(Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException;

    ResultSet executeQuery(Map<ClickHouseQueryParam, String> additionalDBParams, List<ClickHouseExternalData> externalData) throws SQLException;
}
