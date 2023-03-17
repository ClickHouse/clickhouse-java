package com.clickhouse.jdbc.internal;

import java.io.Serializable;
import java.net.URI;
import java.sql.ClientInfoStatus;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseClientBuilder;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseNodes;
import com.clickhouse.client.ClickHouseParameterizedQuery;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseTransaction;
import com.clickhouse.client.ClickHouseRequest.Mutation;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.config.ClickHouseRenameMethod;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.data.ClickHouseVersion;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDatabaseMetaData;
import com.clickhouse.jdbc.ClickHouseDriver;
import com.clickhouse.jdbc.ClickHouseStatement;
import com.clickhouse.jdbc.JdbcConfig;
import com.clickhouse.jdbc.JdbcParameterizedQuery;
import com.clickhouse.jdbc.JdbcParseHandler;
import com.clickhouse.jdbc.SqlExceptionUtils;
import com.clickhouse.jdbc.JdbcWrapper;
import com.clickhouse.jdbc.internal.ClickHouseJdbcUrlParser.ConnectionInfo;
import com.clickhouse.jdbc.parser.ClickHouseSqlParser;
import com.clickhouse.jdbc.parser.ClickHouseSqlStatement;
import com.clickhouse.jdbc.parser.ParseHandler;
import com.clickhouse.jdbc.parser.StatementType;

public class ClickHouseConnectionImpl extends JdbcWrapper implements ClickHouseConnection {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseConnectionImpl.class);

    static final String SETTING_READONLY = "readonly";
    static final String SETTING_MAX_INSERT_BLOCK = "max_insert_block_size";
    static final String SETTING_LW_DELETE = "allow_experimental_lightweight_delete";

    private static final String SQL_GET_SERVER_INFO = "select currentUser() user, timezone() timezone, version() version, "
            + getSetting(SETTING_READONLY, ClickHouseDataType.UInt8) + ", "
            + getSetting(ClickHouseTransaction.SETTING_THROW_ON_UNSUPPORTED_QUERY_INSIDE_TRANSACTION,
                    ClickHouseDataType.Int8)
            + ", "
            + getSetting(ClickHouseTransaction.SETTING_WAIT_CHANGES_BECOME_VISIBLE_AFTER_COMMIT_MODE,
                    ClickHouseDataType.String)
            + ","
            + getSetting(ClickHouseTransaction.SETTING_IMPLICIT_TRANSACTION, ClickHouseDataType.Int8) + ", "
            + getSetting(SETTING_MAX_INSERT_BLOCK, ClickHouseDataType.UInt64) + ", "
            + getSetting(SETTING_LW_DELETE, ClickHouseDataType.Int8)
            + " FORMAT RowBinaryWithNamesAndTypes";

    private static String getSetting(String setting, ClickHouseDataType type) {
        return getSetting(setting, type, null);
    }

    private static String getSetting(String setting, ClickHouseDataType type, String defaultValue) {
        StringBuilder builder = new StringBuilder();
        if (type == ClickHouseDataType.String) {
            builder.append("lower(ifnull((select value from system.settings where name = '").append(setting)
                    .append("'), ");
        } else {
            builder.append("to").append(type.name())
                    .append("(ifnull((select value from system.settings where name = '").append(setting)
                    .append("'), ");
        }
        if (ClickHouseChecker.isNullOrEmpty(defaultValue)) {
            builder.append(type.getMaxPrecision() > 0 ? (type.isSigned() ? "'-1'" : "'0'") : "''");
        } else {
            builder.append('\'').append(defaultValue).append('\'');
        }
        return builder.append(")) as ").append(setting).toString();
    }

    protected static ClickHouseRecord getServerInfo(ClickHouseNode node, ClickHouseRequest<?> request,
            boolean createDbIfNotExist) throws SQLException {
        ClickHouseRequest<?> newReq = request.copy().option(ClickHouseClientOption.RENAME_RESPONSE_COLUMN,
                ClickHouseRenameMethod.NONE);
        if (!createDbIfNotExist) { // in case the database does not exist
            newReq.option(ClickHouseClientOption.DATABASE, "");
        }
        try (ClickHouseResponse response = newReq.option(ClickHouseClientOption.ASYNC, false)
                .option(ClickHouseClientOption.COMPRESS, false)
                .option(ClickHouseClientOption.DECOMPRESS, false)
                .option(ClickHouseClientOption.FORMAT, ClickHouseFormat.RowBinaryWithNamesAndTypes)
                .query(SQL_GET_SERVER_INFO).executeAndWait()) {
            return response.firstRecord();
        } catch (Exception e) {
            SQLException sqlExp = SqlExceptionUtils.handle(e);
            if (createDbIfNotExist && sqlExp.getErrorCode() == 81) {
                String db = node.getDatabase(request.getConfig());
                try (ClickHouseResponse resp = newReq.use("")
                        .query(new StringBuilder("CREATE DATABASE IF NOT EXISTS `")
                                .append(ClickHouseUtils.escape(db, '`')).append('`').toString())
                        .executeAndWait()) {
                    return getServerInfo(node, request, false);
                } catch (SQLException ex) {
                    throw ex;
                } catch (Exception ex) {
                    throw SqlExceptionUtils.handle(ex);
                }
            } else {
                throw sqlExp;
            }
        }
    }

    private final JdbcConfig jdbcConf;

    private final ClickHouseClient client;
    private final ClickHouseRequest<?> clientRequest;

    private boolean autoCommit;
    private boolean closed;
    private String database;
    private boolean readOnly;
    private int networkTimeout;
    private int rsHoldability;
    private int txIsolation;

    private final Optional<TimeZone> clientTimeZone;
    private final Calendar defaultCalendar;
    private final TimeZone jvmTimeZone;
    private final TimeZone serverTimeZone;
    private final ClickHouseVersion serverVersion;
    private final String user;
    private final int initialReadOnly;
    private final int initialNonTxQuerySupport;
    private final String initialTxCommitWaitMode;
    private final int initialImplicitTx;
    private final long initialMaxInsertBlockSize;
    private final int initialDeleteSupport;

    private final Map<String, Class<?>> typeMap;

    private final AtomicReference<JdbcTransaction> txRef;

    protected JdbcTransaction createTransaction() throws SQLException {
        if (!isTransactionSupported()) {
            return new JdbcTransaction(null);
        }

        try {
            ClickHouseTransaction tx = clientRequest.getManager().createTransaction(clientRequest);
            tx.begin();
            // if (txIsolation == Connection.TRANSACTION_READ_UNCOMMITTED) {
            // tx.snapshot(ClickHouseTransaction.CSN_EVERYTHING_VISIBLE);
            // }
            clientRequest.transaction(tx);
            return new JdbcTransaction(tx);
        } catch (ClickHouseException e) {
            throw SqlExceptionUtils.handle(e);
        }
    }

    protected JdbcSavepoint createSavepoint() {
        return new JdbcSavepoint(1, "name");
    }

    /**
     * Checks if the connection is open or not.
     *
     * @throws SQLException when the connection is closed
     */
    protected void ensureOpen() throws SQLException {
        if (closed) {
            throw SqlExceptionUtils.clientError("Cannot operate on a closed connection");
        }
    }

    /**
     * Checks if a feature can be supported or not.
     *
     * @param feature non-empty feature name
     * @param silent  whether to show warning in log or throw unsupported exception
     * @throws SQLException when the feature is not supported and silent is
     *                      {@code false}
     */
    protected void ensureSupport(String feature, boolean silent) throws SQLException {
        String msg = feature + " is not supported";

        if (jdbcConf.isJdbcCompliant()) {
            if (silent) {
                log.debug("[JDBC Compliant Mode] %s. You may change %s to false to throw SQLException instead.", msg,
                        JdbcConfig.PROP_JDBC_COMPLIANT);
            } else {
                log.warn("[JDBC Compliant Mode] %s. You may change %s to false to throw SQLException instead.", msg,
                        JdbcConfig.PROP_JDBC_COMPLIANT);
            }
        } else if (!silent) {
            throw SqlExceptionUtils.unsupportedError(msg);
        }
    }

    protected void ensureTransactionSupport() throws SQLException {
        if (!isTransactionSupported()) {
            ensureSupport("Transaction", false);
        }
    }

    protected List<ClickHouseColumn> getTableColumns(String dbName, String tableName, String columns)
            throws SQLException {
        if (tableName == null || columns == null) {
            throw SqlExceptionUtils.clientError("Failed to extract table and columns from the query");
        }

        if (columns.isEmpty()) {
            columns = "*";
        } else {
            columns = columns.substring(1); // remove the leading bracket
        }
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ").append(columns).append(" FROM ");
        if (!ClickHouseChecker.isNullOrEmpty(dbName)) {
            builder.append('`').append(ClickHouseUtils.escape(dbName, '`')).append('`').append('.');
        }
        builder.append('`').append(ClickHouseUtils.escape(tableName, '`')).append('`').append(" WHERE 0");
        List<ClickHouseColumn> list;
        try (ClickHouseResponse resp = clientRequest.copy().format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                .option(ClickHouseClientOption.RENAME_RESPONSE_COLUMN, ClickHouseRenameMethod.NONE)
                .query(builder.toString()).executeAndWait()) {
            list = resp.getColumns();
        } catch (Exception e) {
            throw SqlExceptionUtils.handle(e);
        }
        return list;
    }

    protected String getDatabase() throws SQLException {
        ensureOpen();

        return getCurrentDatabase();
    }

    // for testing purpose
    final JdbcTransaction getJdbcTrasaction() {
        return txRef.get();
    }

    public ClickHouseConnectionImpl(String url) throws SQLException {
        this(url, new Properties());
    }

    public ClickHouseConnectionImpl(String url, Properties properties) throws SQLException {
        this(ClickHouseJdbcUrlParser.parse(url, properties));
    }

    public ClickHouseConnectionImpl(ConnectionInfo connInfo) throws SQLException {
        jdbcConf = connInfo.getJdbcConfig();

        jvmTimeZone = TimeZone.getDefault();

        ClickHouseClientBuilder clientBuilder = ClickHouseClient.builder()
                .options(ClickHouseDriver.toClientOptions(connInfo.getProperties()))
                .defaultCredentials(connInfo.getDefaultCredentials());
        ClickHouseNodes nodes = connInfo.getNodes();
        final ClickHouseNode node;
        if (nodes.isSingleNode()) {
            try {
                node = nodes.apply(nodes.getNodeSelector());
            } catch (Exception e) {
                throw SqlExceptionUtils.clientError("Failed to get single-node", e);
            }
            client = clientBuilder.nodeSelector(ClickHouseNodeSelector.of(node.getProtocol())).build();
            clientRequest = client.connect(node);
        } else {
            log.debug("Selecting node from: %s", nodes);
            client = clientBuilder.build(); // use dummy client
            clientRequest = client.connect(nodes);
            try {
                node = clientRequest.getServer();
            } catch (Exception e) {
                throw SqlExceptionUtils.clientError("No healthy node available", e);
            }
        }

        log.debug("Connecting to: %s", node);
        ClickHouseConfig config = clientRequest.getConfig();
        String currentUser = null;
        TimeZone timeZone = null;
        ClickHouseVersion version = null;
        if (config.hasServerInfo()) { // when both serverTimeZone and serverVersion are configured
            timeZone = config.getServerTimeZone();
            version = config.getServerVersion();
            if (jdbcConf.isCreateDbIfNotExist()) {
                ClickHouseRecord r = getServerInfo(node, clientRequest, true);
                initialReadOnly = r.getValue(3).asInteger();
                initialNonTxQuerySupport = r.getValue(4).asInteger();
                initialTxCommitWaitMode = r.getValue(5).asString();
                initialImplicitTx = r.getValue(6).asInteger();
                initialMaxInsertBlockSize = r.getValue(7).asLong();
                initialDeleteSupport = r.getValue(8).asInteger();
            } else {
                initialReadOnly = clientRequest.getSetting(SETTING_READONLY, 0);
                initialNonTxQuerySupport = clientRequest
                        .getSetting(ClickHouseTransaction.SETTING_THROW_ON_UNSUPPORTED_QUERY_INSIDE_TRANSACTION, 1);
                initialTxCommitWaitMode = clientRequest.getSetting(
                        ClickHouseTransaction.SETTING_WAIT_CHANGES_BECOME_VISIBLE_AFTER_COMMIT_MODE, "wait_unknown");
                initialImplicitTx = clientRequest.getSetting(ClickHouseTransaction.SETTING_IMPLICIT_TRANSACTION, 0);
                initialMaxInsertBlockSize = clientRequest.getSetting(SETTING_MAX_INSERT_BLOCK, 0L);
                initialDeleteSupport = clientRequest.getSetting(SETTING_LW_DELETE, 0);
            }
        } else {
            ClickHouseRecord r = getServerInfo(node, clientRequest, jdbcConf.isCreateDbIfNotExist());
            currentUser = r.getValue(0).asString();
            String tz = r.getValue(1).asString();
            String ver = r.getValue(2).asString();
            version = ClickHouseVersion.of(ver);
            // https://github.com/ClickHouse/ClickHouse/commit/486d63864bcc6e15695cd3e9f9a3f83a84ec4009
            if (version.check("(,20.7)")) {
                throw SqlExceptionUtils
                        .unsupportedError("Sorry this driver only supports ClickHouse server 20.7 or above");
            }
            if (ClickHouseChecker.isNullOrBlank(tz)) {
                tz = "UTC";
            }
            // tsTimeZone.hasSameRules(ClickHouseValues.UTC_TIMEZONE)
            timeZone = "UTC".equals(tz) ? ClickHouseValues.UTC_TIMEZONE : TimeZone.getTimeZone(tz);
            initialReadOnly = r.getValue(3).asInteger();
            initialNonTxQuerySupport = r.getValue(4).asInteger();
            initialTxCommitWaitMode = r.getValue(5).asString();
            initialImplicitTx = r.getValue(6).asInteger();
            initialMaxInsertBlockSize = r.getValue(7).asLong();
            initialDeleteSupport = r.getValue(8).asInteger();

            // update request and corresponding config
            clientRequest.option(ClickHouseClientOption.SERVER_TIME_ZONE, tz)
                    .option(ClickHouseClientOption.SERVER_VERSION, ver);
        }

        this.autoCommit = !jdbcConf.isJdbcCompliant() || jdbcConf.isAutoCommit();
        this.closed = false;
        this.database = config.getDatabase();
        this.clientRequest.use(this.database);
        this.readOnly = clientRequest.getSetting(SETTING_READONLY, initialReadOnly) != 0;
        this.networkTimeout = 0;
        this.rsHoldability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        if (isTransactionSupported()) {
            this.txIsolation = Connection.TRANSACTION_REPEATABLE_READ;
            if (jdbcConf.isJdbcCompliant() && !this.readOnly) {
                if (!this.clientRequest
                        .hasSetting(ClickHouseTransaction.SETTING_THROW_ON_UNSUPPORTED_QUERY_INSIDE_TRANSACTION)) {
                    this.clientRequest.set(ClickHouseTransaction.SETTING_THROW_ON_UNSUPPORTED_QUERY_INSIDE_TRANSACTION,
                            0);
                }
                // .set(ClickHouseTransaction.SETTING_WAIT_CHANGES_BECOME_VISIBLE_AFTER_COMMIT_MODE,
                // "wait_unknown");
            }
        } else {
            this.txIsolation = jdbcConf.isJdbcCompliant() ? Connection.TRANSACTION_READ_COMMITTED
                    : Connection.TRANSACTION_NONE;
        }

        this.user = currentUser != null ? currentUser : node.getCredentials(config).getUserName();
        this.serverTimeZone = timeZone;
        if (config.isUseServerTimeZone()) {
            clientTimeZone = Optional.empty();
            // with respect of default locale
            defaultCalendar = new GregorianCalendar();
        } else {
            clientTimeZone = Optional.of(config.getUseTimeZone());
            defaultCalendar = new GregorianCalendar(clientTimeZone.get());
        }
        this.serverVersion = version;
        this.typeMap = new HashMap<>(jdbcConf.getTypeMap());
        this.txRef = new AtomicReference<>(this.autoCommit ? null : createTransaction());
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        ensureOpen();

        // get rewritten query?
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        ensureOpen();

        if (this.autoCommit == autoCommit) {
            return;
        }

        ensureTransactionSupport();
        if (this.autoCommit = autoCommit) { // commit
            JdbcTransaction tx = txRef.getAndSet(null);
            if (tx != null) {
                tx.commit(log);
            }
        } else { // start new transaction
            if (!txRef.compareAndSet(null, createTransaction())) {
                log.warn("Not able to start a new transaction, reuse the exist one: %s", txRef.get());
            }
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        ensureOpen();

        return autoCommit;
    }

    @Override
    public void begin() throws SQLException {
        if (getAutoCommit()) {
            throw SqlExceptionUtils.clientError("Cannot start new transaction in auto-commit mode");
        }

        ensureTransactionSupport();

        JdbcTransaction tx = txRef.get();
        if (tx == null || !tx.isNew()) {
            // invalid transaction state
            throw new SQLException(JdbcTransaction.ERROR_TX_STARTED, SqlExceptionUtils.SQL_STATE_INVALID_TX_STATE);
        }
    }

    @Override
    public void commit() throws SQLException {
        if (getAutoCommit()) {
            throw SqlExceptionUtils.clientError("Cannot commit in auto-commit mode");
        }

        ensureTransactionSupport();

        JdbcTransaction tx = txRef.get();
        if (tx == null) {
            // invalid transaction state
            throw new SQLException(JdbcTransaction.ERROR_TX_NOT_STARTED, SqlExceptionUtils.SQL_STATE_INVALID_TX_STATE);
        } else {
            try {
                tx.commit(log);
            } finally {
                if (!txRef.compareAndSet(tx, createTransaction())) {
                    log.warn("Transaction was set to %s unexpectedly", txRef.get());
                }
            }
        }
    }

    @Override
    public void rollback() throws SQLException {
        if (getAutoCommit()) {
            throw SqlExceptionUtils.clientError("Cannot rollback in auto-commit mode");
        }

        ensureTransactionSupport();

        JdbcTransaction tx = txRef.get();
        if (tx == null) {
            // invalid transaction state
            throw new SQLException(JdbcTransaction.ERROR_TX_NOT_STARTED, SqlExceptionUtils.SQL_STATE_INVALID_TX_STATE);
        } else {
            try {
                tx.rollback(log);
            } finally {
                if (!txRef.compareAndSet(tx, createTransaction())) {
                    log.warn("Transaction was set to %s unexpectedly", txRef.get());
                }
            }
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            this.client.close();
        } catch (Exception e) {
            log.warn("Failed to close connection due to %s", e.getMessage());
            throw SqlExceptionUtils.handle(e);
        } finally {
            this.closed = true;
        }

        JdbcTransaction tx = txRef.get();
        if (tx != null) {
            try {
                tx.commit(log);
            } finally {
                if (!txRef.compareAndSet(tx, null)) {
                    log.warn("Transaction was set to %s unexpectedly", txRef.get());
                }
            }
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new ClickHouseDatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        ensureOpen();

        if (initialReadOnly != 0) {
            if (!readOnly) {
                throw SqlExceptionUtils.clientError("Cannot change the setting on a read-only connection");
            }
        } else {
            if (readOnly) {
                clientRequest.set(SETTING_READONLY, 2);
            } else {
                clientRequest.removeSetting(SETTING_READONLY);
            }
            this.readOnly = readOnly;
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        ensureOpen();

        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        if (jdbcConf.useCatalog()) {
            setCurrentDatabase(catalog, true);
        } else {
            log.warn(
                    "setCatalog method is no-op. Please either change databaseTerm to catalog or use setSchema method instead");
        }
    }

    @Override
    public String getCatalog() throws SQLException {
        return jdbcConf.useCatalog() ? getDatabase() : null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        ensureOpen();

        if (Connection.TRANSACTION_NONE != level && Connection.TRANSACTION_READ_UNCOMMITTED != level
                && Connection.TRANSACTION_READ_COMMITTED != level && Connection.TRANSACTION_REPEATABLE_READ != level
                && Connection.TRANSACTION_SERIALIZABLE != level) {
            throw new SQLException("Invalid transaction isolation level: " + level);
        } else if (isTransactionSupported()) {
            txIsolation = Connection.TRANSACTION_REPEATABLE_READ;
        } else if (jdbcConf.isJdbcCompliant()) {
            txIsolation = level;
        } else {
            txIsolation = Connection.TRANSACTION_NONE;
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        ensureOpen();

        return txIsolation;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        ensureOpen();

        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        ensureOpen();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        ensureOpen();

        return new HashMap<>(typeMap);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        ensureOpen();

        if (map != null) {
            typeMap.putAll(map);
        }
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        ensureOpen();

        if (holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT || holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            rsHoldability = holdability;
        } else {
            throw new SQLException("Invalid holdability: " + holdability);
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        ensureOpen();

        return rsHoldability;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return setSavepoint(null);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        ensureOpen();

        if (getAutoCommit()) {
            throw SqlExceptionUtils.clientError("Cannot set savepoint in auto-commit mode");
        }

        if (!jdbcConf.isJdbcCompliant()) {
            throw SqlExceptionUtils.unsupportedError("setSavepoint not implemented");
        }

        JdbcTransaction tx = txRef.get();
        if (tx == null) {
            tx = createTransaction();
            if (!txRef.compareAndSet(null, tx)) {
                tx = txRef.get();
            }
        }
        return tx.newSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        ensureOpen();

        if (getAutoCommit()) {
            throw SqlExceptionUtils.clientError("Cannot rollback to savepoint in auto-commit mode");
        }

        if (!jdbcConf.isJdbcCompliant()) {
            throw SqlExceptionUtils.unsupportedError("rollback not implemented");
        }

        if (!(savepoint instanceof JdbcSavepoint)) {
            throw SqlExceptionUtils.clientError("Unsupported type of savepoint: " + savepoint);
        }

        JdbcTransaction tx = txRef.get();
        if (tx == null) {
            // invalid transaction state
            throw new SQLException(JdbcTransaction.ERROR_TX_NOT_STARTED, SqlExceptionUtils.SQL_STATE_INVALID_TX_STATE);
        } else {
            JdbcSavepoint s = (JdbcSavepoint) savepoint;
            tx.logSavepointDetails(log, s, JdbcTransaction.ACTION_ROLLBACK);
            tx.toSavepoint(s);
        }
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        ensureOpen();

        if (getAutoCommit()) {
            throw SqlExceptionUtils.clientError("Cannot release savepoint in auto-commit mode");
        }

        if (!jdbcConf.isJdbcCompliant()) {
            throw SqlExceptionUtils.unsupportedError("rollback not implemented");
        }

        if (!(savepoint instanceof JdbcSavepoint)) {
            throw SqlExceptionUtils.clientError("Unsupported type of savepoint: " + savepoint);
        }

        JdbcTransaction tx = txRef.get();
        if (tx == null) {
            // invalid transaction state
            throw new SQLException(JdbcTransaction.ERROR_TX_NOT_STARTED, SqlExceptionUtils.SQL_STATE_INVALID_TX_STATE);
        } else {
            JdbcSavepoint s = (JdbcSavepoint) savepoint;
            tx.logSavepointDetails(log, s, "released");
            tx.toSavepoint(s);
        }
    }

    @Override
    public ClickHouseStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        ensureOpen();

        return new ClickHouseStatementImpl(this, clientRequest.copy(), resultSetType, resultSetConcurrency,
                resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        ensureOpen();

        ClickHouseConfig config = clientRequest.getConfig();
        // TODO remove the extra parsing
        ClickHouseSqlStatement[] stmts = parse(sql, config, clientRequest.getSettings());
        if (stmts.length != 1) {
            throw SqlExceptionUtils
                    .clientError("Prepared statement only supports one query but we got: " + stmts.length);
        }
        ClickHouseSqlStatement parsedStmt = stmts[0];

        ClickHouseParameterizedQuery preparedQuery;
        try {
            preparedQuery = jdbcConf.useNamedParameter()
                    ? ClickHouseParameterizedQuery.of(clientRequest.getConfig(), parsedStmt.getSQL())
                    : JdbcParameterizedQuery.of(config, parsedStmt.getSQL());
        } catch (RuntimeException e) {
            throw SqlExceptionUtils.clientError(e);
        }

        PreparedStatement ps = null;
        if (preparedQuery.hasParameter()) {
            if (parsedStmt.hasTempTable() || parsedStmt.hasInput()) {
                throw SqlExceptionUtils
                        .clientError(
                                "External table, input function, and query parameter cannot be used together in PreparedStatement.");
            } else if (parsedStmt.getStatementType() == StatementType.INSERT &&
                    !parsedStmt.containsKeyword("SELECT") && parsedStmt.hasValues() &&
                    (!parsedStmt.hasFormat() || clientRequest.getFormat().name().equals(parsedStmt.getFormat()))) {
                String query = parsedStmt.getSQL();
                boolean useStream = false;
                Integer startIndex = parsedStmt.getPositions().get(ClickHouseSqlStatement.KEYWORD_VALUES_START);
                if (startIndex != null) {
                    useStream = true;
                    int endIndex = parsedStmt.getPositions().get(ClickHouseSqlStatement.KEYWORD_VALUES_END);
                    for (int i = startIndex + 1; i < endIndex; i++) {
                        char ch = query.charAt(i);
                        if (ch != '?' && ch != ',' && !Character.isWhitespace(ch)) {
                            useStream = false;
                            break;
                        }
                    }
                }

                if (useStream) {
                    ps = new InputBasedPreparedStatement(this,
                            clientRequest.write().query(query.substring(0, parsedStmt.getStartPosition("VALUES")),
                                    newQueryId()),
                            getTableColumns(parsedStmt.getDatabase(), parsedStmt.getTable(),
                                    parsedStmt.getContentBetweenKeywords(
                                            ClickHouseSqlStatement.KEYWORD_TABLE_COLUMNS_START,
                                            ClickHouseSqlStatement.KEYWORD_TABLE_COLUMNS_END)),
                            resultSetType, resultSetConcurrency, resultSetHoldability);
                }
            }
        } else {
            if (parsedStmt.hasTempTable()) {
                // queries using external/temporary table
                ps = new TableBasedPreparedStatement(this,
                        clientRequest.copy().query(parsedStmt.getSQL(), newQueryId()), parsedStmt,
                        resultSetType, resultSetConcurrency, resultSetHoldability);
            } else if (parsedStmt.getStatementType() == StatementType.INSERT) {
                if (!ClickHouseChecker.isNullOrBlank(parsedStmt.getInput())) {
                    // an ugly workaround of https://github.com/ClickHouse/ClickHouse/issues/39866
                    // would be replace JSON and Object('json') types in the query to String

                    Mutation m = clientRequest.write();
                    if (parsedStmt.hasFormat()) {
                        m.format(ClickHouseFormat.valueOf(parsedStmt.getFormat()));
                    }
                    // insert query using input function
                    ps = new InputBasedPreparedStatement(this, m.query(parsedStmt.getSQL(), newQueryId()),
                            ClickHouseColumn.parse(parsedStmt.getInput()), resultSetType, resultSetConcurrency,
                            resultSetHoldability);
                } else if (!parsedStmt.containsKeyword("SELECT") && !parsedStmt.hasValues()) {
                    ps = parsedStmt.hasFormat()
                            ? new StreamBasedPreparedStatement(this,
                                    clientRequest.write().query(parsedStmt.getSQL(), newQueryId()), parsedStmt,
                                    resultSetType, resultSetConcurrency, resultSetHoldability)
                            : new InputBasedPreparedStatement(this,
                                    clientRequest.write().query(parsedStmt.getSQL(), newQueryId()),
                                    getTableColumns(parsedStmt.getDatabase(), parsedStmt.getTable(),
                                            parsedStmt.getContentBetweenKeywords(
                                                    ClickHouseSqlStatement.KEYWORD_TABLE_COLUMNS_START,
                                                    ClickHouseSqlStatement.KEYWORD_TABLE_COLUMNS_END)),
                                    resultSetType, resultSetConcurrency, resultSetHoldability);
                }
            }
        }

        return ps != null ? ps
                : new SqlBasedPreparedStatement(this, clientRequest.copy().query(preparedQuery, newQueryId()),
                        stmts[0], resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public NClob createNClob() throws SQLException {
        ensureOpen();

        return createClob();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw SqlExceptionUtils.clientError("Negative milliseconds is not allowed");
        } else if (timeout == 0) {
            timeout = clientRequest.getConfig().getConnectionTimeout();
        } else {
            timeout = (int) TimeUnit.SECONDS.toMillis(timeout);
        }

        if (isClosed()) {
            return false;
        }

        return client.ping(clientRequest.getServer(), timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            ensureOpen();
        } catch (SQLException e) {
            Map<String, ClientInfoStatus> failedProps = new HashMap<>();
            failedProps.put(PROP_APPLICATION_NAME, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            failedProps.put(PROP_CUSTOM_HTTP_HEADERS, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            failedProps.put(PROP_CUSTOM_HTTP_PARAMS, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            throw new SQLClientInfoException(e.getMessage(), failedProps);
        }

        if (PROP_APPLICATION_NAME.equals(name)) {
            if (ClickHouseChecker.isNullOrBlank(value)) {
                clientRequest.removeOption(ClickHouseClientOption.CLIENT_NAME);
            } else {
                clientRequest.option(ClickHouseClientOption.CLIENT_NAME, value);
            }
        } else if (PROP_CUSTOM_HTTP_HEADERS.equals(name)) {
            if (ClickHouseChecker.isNullOrBlank(value)) {
                clientRequest.removeOption(ClickHouseHttpOption.CUSTOM_HEADERS);
            } else {
                clientRequest.option(ClickHouseHttpOption.CUSTOM_HEADERS, value);
            }
        } else if (PROP_CUSTOM_HTTP_PARAMS.equals(name)) {
            if (ClickHouseChecker.isNullOrBlank(value)) {
                clientRequest.removeOption(ClickHouseHttpOption.CUSTOM_PARAMS);
            } else {
                clientRequest.option(ClickHouseHttpOption.CUSTOM_PARAMS, value);
            }
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            ensureOpen();
        } catch (SQLException e) {
            Map<String, ClientInfoStatus> failedProps = new HashMap<>();
            failedProps.put(PROP_APPLICATION_NAME, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            failedProps.put(PROP_CUSTOM_HTTP_HEADERS, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            failedProps.put(PROP_CUSTOM_HTTP_PARAMS, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            throw new SQLClientInfoException(e.getMessage(), failedProps);
        }

        if (properties != null) {
            String value = properties.getProperty(PROP_APPLICATION_NAME);
            if (ClickHouseChecker.isNullOrBlank(value)) {
                clientRequest.removeOption(ClickHouseClientOption.CLIENT_NAME);
            } else {
                clientRequest.option(ClickHouseClientOption.CLIENT_NAME, value);
            }

            value = properties.getProperty(PROP_CUSTOM_HTTP_HEADERS);
            if (ClickHouseChecker.isNullOrBlank(value)) {
                clientRequest.removeOption(ClickHouseHttpOption.CUSTOM_HEADERS);
            } else {
                clientRequest.option(ClickHouseHttpOption.CUSTOM_HEADERS, value);
            }

            value = properties.getProperty(PROP_CUSTOM_HTTP_PARAMS);
            if (ClickHouseChecker.isNullOrBlank(value)) {
                clientRequest.removeOption(ClickHouseHttpOption.CUSTOM_PARAMS);
            } else {
                clientRequest.option(ClickHouseHttpOption.CUSTOM_PARAMS, value);
            }
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        ensureOpen();

        ClickHouseConfig config = clientRequest.getConfig();
        String value = null;
        if (PROP_APPLICATION_NAME.equals(name)) {
            value = config.getClientName();
        } else if (PROP_CUSTOM_HTTP_HEADERS.equals(name)) {
            value = config.getStrOption(ClickHouseHttpOption.CUSTOM_HEADERS);
        } else if (PROP_CUSTOM_HTTP_PARAMS.equals(name)) {
            value = config.getStrOption(ClickHouseHttpOption.CUSTOM_PARAMS);
        }
        return value;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        ensureOpen();

        ClickHouseConfig config = clientRequest.getConfig();
        Properties props = new Properties();
        props.setProperty(PROP_APPLICATION_NAME, config.getClientName());
        props.setProperty(PROP_CUSTOM_HTTP_HEADERS, config.getStrOption(ClickHouseHttpOption.CUSTOM_HEADERS));
        props.setProperty(PROP_CUSTOM_HTTP_PARAMS, config.getStrOption(ClickHouseHttpOption.CUSTOM_PARAMS));
        return props;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        if (jdbcConf.useSchema()) {
            setCurrentDatabase(schema, true);
        } else {
            log.warn(
                    "setSchema method is no-op. Please either change databaseTerm to schema or use setCatalog method instead");
        }
    }

    @Override
    public String getSchema() throws SQLException {
        return jdbcConf.useSchema() ? getDatabase() : null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (executor == null) {
            throw SqlExceptionUtils.clientError("Non-null executor is required");
        }

        executor.execute(() -> {
            try {
                // try harder please
                this.client.close();
            } finally {
                this.closed = true;
            }
        });
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        ensureOpen();

        if (executor == null) {
            throw SqlExceptionUtils.clientError("Non-null executor is required");
        }

        if (milliseconds < 0) {
            throw SqlExceptionUtils.clientError("Negative milliseconds is not allowed");
        }

        executor.execute(() -> {
            // TODO close this connection when any statement timed out after this amount of
            // time
            networkTimeout = milliseconds;
        });
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        ensureOpen();

        return networkTimeout;
    }

    @Override
    public ClickHouseConfig getConfig() {
        return clientRequest.getConfig();
    }

    @Override
    public boolean allowCustomSetting() {
        return initialReadOnly != 1;
    }

    @Override
    public String getCurrentDatabase() {
        return database;
    }

    @Override
    public void setCurrentDatabase(String db, boolean check) throws SQLException {
        ensureOpen();

        if (db == null || db.isEmpty()) {
            throw new SQLException("Non-empty database name is required", SqlExceptionUtils.SQL_STATE_INVALID_SCHEMA);
        } else {
            clientRequest.use(db);
            if (check) {
                try (ClickHouseResponse response = clientRequest.query("select 1").executeAndWait()) {
                    database = db;
                } catch (ClickHouseException e) {
                    throw SqlExceptionUtils.handle(e);
                } finally {
                    if (!db.equals(database)) {
                        clientRequest.use(database);
                    }
                }
            } else {
                database = db;
            }
        }
    }

    @Override
    public String getCurrentUser() {
        return user;
    }

    @Override
    public Calendar getDefaultCalendar() {
        return defaultCalendar;
    }

    @Override
    public Optional<TimeZone> getEffectiveTimeZone() {
        return clientTimeZone;
    }

    @Override
    public TimeZone getJvmTimeZone() {
        return jvmTimeZone;
    }

    @Override
    public TimeZone getServerTimeZone() {
        return serverTimeZone;
    }

    @Override
    public ClickHouseVersion getServerVersion() {
        return serverVersion;
    }

    @Override
    public ClickHouseTransaction getTransaction() {
        return clientRequest.getTransaction();
    }

    @Override
    public URI getUri() {
        return clientRequest.getServer().toUri(ClickHouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX);
    }

    @Override
    public JdbcConfig getJdbcConfig() {
        return jdbcConf;
    }

    @Override
    public long getMaxInsertBlockSize() {
        return initialMaxInsertBlockSize;
    }

    @Override
    public boolean isTransactionSupported() {
        return jdbcConf.isTransactionSupported() && initialNonTxQuerySupport >= 0
                && !ClickHouseChecker.isNullOrEmpty(initialTxCommitWaitMode);
    }

    @Override
    public boolean isImplicitTransactionSupported() {
        return jdbcConf.isTransactionSupported() && initialImplicitTx >= 0;
    }

    @Override
    public String newQueryId() {
        String queryId = clientRequest.getManager().createQueryId();
        JdbcTransaction tx = txRef.get();
        return tx != null ? tx.newQuery(queryId) : queryId;
    }

    @Override
    public ClickHouseSqlStatement[] parse(String sql, ClickHouseConfig config, Map<String, Serializable> settings) {
        ParseHandler handler = null;
        if (jdbcConf.isJdbcCompliant()) {
            handler = JdbcParseHandler.INSTANCE;
            if (settings != null) {
                Serializable value = settings.get(SETTING_LW_DELETE);
                if (value == null ? initialDeleteSupport == 1
                        : ClickHouseOption.fromString(value.toString(), Boolean.class)) {
                    handler = JdbcParseHandler.WITHOUT_DELETE;
                }

            }
        }
        return ClickHouseSqlParser.parse(sql, config != null ? config : clientRequest.getConfig(), handler);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface == ClickHouseClient.class || iface == ClickHouseRequest.class
                || super.isWrapperFor(iface);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface == ClickHouseClient.class) {
            return iface.cast(client);
        } else if (iface == ClickHouseRequest.class) {
            return iface.cast(clientRequest);
        }

        return super.unwrap(iface);
    }
}
