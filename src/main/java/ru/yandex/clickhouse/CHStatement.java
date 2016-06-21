package ru.yandex.clickhouse;

import ru.yandex.clickhouse.settings.CHQueryParam;
import ru.yandex.clickhouse.response.CHResponse;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * @author serebrserg
 * @since 22.03.16
 */
public interface CHStatement extends Statement {
    CHResponse executeQueryClickhouseResponse(String sql) throws SQLException;
    CHResponse executeQueryClickhouseResponse(String sql, Map<CHQueryParam, String> additionalDBParams) throws SQLException;
    ResultSet executeQuery(String sql, Map<CHQueryParam, String> additionalDBParams) throws SQLException;
    void sendStream(InputStream content, String table) throws SQLException;
}
