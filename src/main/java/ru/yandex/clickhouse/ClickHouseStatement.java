package ru.yandex.clickhouse;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import ru.yandex.clickhouse.response.ClickHouseResponse;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryInputStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;


public interface ClickHouseStatement extends Statement {

    ClickHouseResponse executeQueryClickhouseResponse(String sql) throws SQLException;

    ClickHouseResponse executeQueryClickhouseResponse(String sql, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException;

    ClickHouseResponse executeQueryClickhouseResponse(String sql,
                                                      Map<ClickHouseQueryParam, String> additionalDBParams,
                                                      Map<String, String> additionalRequestParams) throws SQLException;

    ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(String sql) throws SQLException;

    ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(String sql,
                                                                         Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException;

    ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(String sql,
                                                                         Map<ClickHouseQueryParam, String> additionalDBParams,
                                                                         Map<String, String> additionalRequestParams) throws SQLException;

    ResultSet executeQuery(String sql, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException;

    ResultSet executeQuery(String sql, Map<ClickHouseQueryParam, String> additionalDBParams, List<ClickHouseExternalData> externalData) throws SQLException;

    ResultSet executeQuery(String sql,
                           Map<ClickHouseQueryParam, String> additionalDBParams,
                           List<ClickHouseExternalData> externalData,
                           Map<String, String> additionalRequestParams) throws SQLException;

    @Deprecated
    void sendStream(InputStream content, String table, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException;

    @Deprecated
    void sendStream(InputStream content, String table) throws SQLException;

    @Deprecated
    void sendRowBinaryStream(String sql, Map<ClickHouseQueryParam, String> additionalDBParams, ClickHouseStreamCallback callback) throws SQLException;

    @Deprecated
    void sendRowBinaryStream(String sql, ClickHouseStreamCallback callback) throws SQLException;

    @Deprecated
    void sendNativeStream(String sql, Map<ClickHouseQueryParam, String> additionalDBParams, ClickHouseStreamCallback callback) throws SQLException;

    @Deprecated
    void sendNativeStream(String sql, ClickHouseStreamCallback callback) throws SQLException;

    @Deprecated
    void sendCSVStream(InputStream content, String table, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException;

    @Deprecated
    void sendCSVStream(InputStream content, String table) throws SQLException;

    @Deprecated
    void sendStreamSQL(InputStream content, String sql, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException;

    @Deprecated
    void sendStreamSQL(InputStream content, String sql) throws SQLException;

    /**
     * Returns extended write-API, which simplifies uploading larger files or
     * data streams
     *
     * @return a new {@link Writer} builder object which can be used to
     *         construct a request to the server
     */
    Writer write();
}
