package ru.yandex.clickhouse;

import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import ru.yandex.clickhouse.response.ClickHouseResponse;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;


public interface ClickHouseStatement extends Statement {
    ClickHouseResponse executeQueryClickhouseResponse(String sql) throws SQLException;
    ClickHouseResponse executeQueryClickhouseResponse(String sql, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException;
    ResultSet executeQuery(String sql, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException;
    void sendStream(InputStream content, String table) throws SQLException;
}
