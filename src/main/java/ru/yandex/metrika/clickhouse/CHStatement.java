package ru.yandex.metrika.clickhouse;

import ru.yandex.metrika.clickhouse.copypaste.ClickhouseResponse;

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
    ClickhouseResponse executeQueryClickhouseResponse(String sql) throws SQLException;
    ClickhouseResponse executeQueryClickhouseResponse(String sql, Map<String, String> additionalDBParams) throws SQLException;
    ClickhouseResponse executeQueryClickhouseResponse(String sql, Map<String, String> additionalDBParams, boolean ignoreDatabase) throws SQLException;
    ResultSet executeQuery(String sql, Map<String, String> additionalDBParams) throws SQLException;
    void sendStream(InputStream content, String table) throws SQLException;
}
