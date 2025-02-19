package com.clickhouse.jdbc;

import java.io.Serializable;
import java.net.URI;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseTransaction;
import com.clickhouse.client.ClickHouseSimpleResponse;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseVersion;
import com.clickhouse.jdbc.parser.ClickHouseSqlStatement;

@Deprecated
public interface ClickHouseConnection extends Connection {
    static final String COLUMN_ELEMENT = "element";
    static final String COLUMN_ARRAY = "array";

    // The name of the application currently utilizing the connection
    static final String PROP_APPLICATION_NAME = "ApplicationName";
    static final String PROP_CUSTOM_HTTP_HEADERS = "CustomHttpHeaders";
    static final String PROP_CUSTOM_HTTP_PARAMS = "CustomHttpParameters";
    // The name of the user that the application using the connection is performing
    // work for. This may not be the same as the user name that was used in
    // establishing the connection.
    // private static final String PROP_CLIENT_USER = "ClientUser";
    // The hostname of the computer the application using the connection is running
    // on.
    // private static final String PROP_CLIENT_HOST = "ClientHostname";

    @Override
    default ClickHouseArray createArrayOf(String typeName, Object[] elements) throws SQLException {
        ClickHouseConfig config = getConfig();
        ClickHouseColumn col = ClickHouseColumn.of(COLUMN_ELEMENT, typeName);
        ClickHouseColumn arrCol = ClickHouseColumn.of(COLUMN_ARRAY, ClickHouseDataType.Array, false, col);
        ClickHouseValue val = arrCol.newValue(config);
        if (elements == null && !col.isNestedType() && !col.isNullable()) {
            int nullAsDefault = getJdbcConfig().getNullAsDefault();
            if (nullAsDefault > 1) {
                val.resetToDefault();
            } else if (nullAsDefault < 1) {
                throw SqlExceptionUtils
                        .clientError(ClickHouseUtils.format("Cannot set null to non-nullable column [%s]", col));
            }
        } else {
            val.update(elements);
        }
        ClickHouseResultSet rs = new ClickHouseResultSet(getCurrentDatabase(), ClickHouseSqlStatement.DEFAULT_TABLE,
                createStatement(), ClickHouseSimpleResponse.of(config, Collections.singletonList(arrCol),
                        new Object[][] { new Object[] { val.asObject() } }));
        rs.next();
        return new ClickHouseArray(rs, 1);
    }

    @Override
    default ClickHouseBlob createBlob() throws SQLException {
        return new ClickHouseBlob();
    }

    @Override
    default ClickHouseClob createClob() throws SQLException {
        return new ClickHouseClob();
    }

    @Override
    default ClickHouseStruct createStruct(String typeName, Object[] attributes) throws SQLException {
        return new ClickHouseStruct(typeName, attributes);
    }

    @Override
    default ClickHouseXml createSQLXML() throws SQLException {
        return new ClickHouseXml();
    }

    @Override
    default ClickHouseStatement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    default ClickHouseStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    ClickHouseStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException;

    @Override
    default CallableStatement prepareCall(String sql) throws SQLException {
        return prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    default CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareCall(sql, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    default CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        throw SqlExceptionUtils.unsupportedError("prepareCall not implemented");
    }

    @Override
    default PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    default PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            // not entirely true, what if the table engine is JDBC?
            throw SqlExceptionUtils.unsupportedError("Only NO_GENERATED_KEYS is supported");
        }

        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    default PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        // not entirely true, what if the table engine is JDBC?
        throw SqlExceptionUtils.unsupportedError("ClickHouse does not support auto generated keys");
    }

    @Override
    default PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        // not entirely true, what if the table engine is JDBC?
        throw SqlExceptionUtils.unsupportedError("ClickHouse does not support auto generated keys");
    }

    @Override
    default PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Starts a new transaction. It's no-op for a newly started transaction.
     *
     * @throws SQLException when current transaction is active state or not able to
     *                      start new transaction
     */
    void begin() throws SQLException;

    /**
     * Gets configuration tied to this connection.
     *
     * @return non-null configuration
     */
    ClickHouseConfig getConfig();

    /**
     * Checks whether custom setting is allowed or not.
     *
     * @return true if custom setting is allowed; false otherwise
     */
    boolean allowCustomSetting();

    /**
     * Gets current database. {@link #getSchema()} is similar but it will check if
     * connection is closed or not hence may throw {@link SQLException}.
     *
     * @return non-null database name
     */
    String getCurrentDatabase();

    /**
     * Sets current database.
     *
     * @param database non-empty database name
     * @param check    whether to check if the database exists or not
     * @throws SQLException when failed to change current database
     */
    void setCurrentDatabase(String database, boolean check) throws SQLException;

    /**
     * Gets current user.
     *
     * @return non-null user name
     */
    String getCurrentUser();

    /**
     * Gets default calendar which can be used to create timestamp.
     *
     * @return non-null calendar
     */
    Calendar getDefaultCalendar();

    /**
     * Gets effective time zone. When
     * {@link com.clickhouse.client.ClickHouseConfig#isUseServerTimeZone()} returns
     * {@code false},
     * {@link com.clickhouse.client.ClickHouseConfig#getUseTimeZone()}
     * will be used as effective time zone, which will be used for reading and
     * writing timestamp values.
     *
     * @return effective time zone
     */
    Optional<TimeZone> getEffectiveTimeZone();

    /**
     * Gets cached value of {@code TimeZone.getDefault()}.
     *
     * @return non-null cached JVM time zone
     */
    TimeZone getJvmTimeZone();

    /**
     * Gets server time zone, which is either same as result of
     * {@code select timezone()}, or the overrided value from
     * {@link com.clickhouse.client.ClickHouseConfig#getServerTimeZone()}.
     *
     * @return non-null server time zone
     */
    TimeZone getServerTimeZone();

    /**
     * Gets server version.
     *
     * @return non-null server version
     */
    ClickHouseVersion getServerVersion();

    /**
     * Gets current transaction.
     *
     * @return current transaction, which could be null
     */
    ClickHouseTransaction getTransaction();

    /**
     * Gets URI of the connection.
     *
     * @return URI of the connection
     */
    URI getUri();

    /**
     * Gets JDBC-specific configuration.
     *
     * @return non-null JDBC-specific configuration
     */
    JdbcConfig getJdbcConfig();

    /**
     * Gets JDBC type mapping. Same as {@code getJdbcConfig().getMapper()}.
     *
     * @return non-null JDBC type mapping
     */
    default JdbcTypeMapping getJdbcTypeMapping() {
        return getJdbcConfig().getDialect();
    }

    /**
     * Gets max insert block size. Pay attention that INSERT into one partition in
     * one table of MergeTree family up to max_insert_block_size rows is
     * transactional.
     *
     * @return value of max_insert_block_size
     */
    long getMaxInsertBlockSize();

    /**
     * Checks whether transaction is supported.
     *
     * @return true if transaction is supported; false otherwise
     */
    boolean isTransactionSupported();

    /**
     * Checks whether implicit transaction is supported.
     *
     * @return true if implicit transaction is supported; false otherwise
     */
    boolean isImplicitTransactionSupported();

    /**
     * Creates a new query ID.
     *
     * @return universal unique query ID
     */
    String newQueryId();

    /**
     * Parses the given sql.
     *
     * @param sql    sql to parse
     * @param config configuration which might be used for parsing, could be null
     * @return non-null parsed sql statements
     * @deprecated will be removed in 0.5, please use
     *             {@link #parse(String, ClickHouseConfig, Map)} instead
     */
    @Deprecated
    default ClickHouseSqlStatement[] parse(String sql, ClickHouseConfig config) {
        return parse(sql, config, null);
    }

    /**
     * Parses the given sql.
     *
     * @param sql      sql to parse
     * @param config   configuration which might be used for parsing, could be null
     * @param settings server settings
     * @return non-null parsed sql statements
     */
    ClickHouseSqlStatement[] parse(String sql, ClickHouseConfig config, Map<String, Serializable> settings);
}
