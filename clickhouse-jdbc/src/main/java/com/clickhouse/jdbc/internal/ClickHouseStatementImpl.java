package com.clickhouse.jdbc.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.ClickHouseTransaction;
import com.clickhouse.client.ClickHouseRequest.Mutation;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.config.ClickHouseConfigChangeListener;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseExternalTable;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseResultSet;
import com.clickhouse.jdbc.ClickHouseStatement;
import com.clickhouse.jdbc.JdbcTypeMapping;
import com.clickhouse.jdbc.SqlExceptionUtils;
import com.clickhouse.jdbc.JdbcWrapper;
import com.clickhouse.jdbc.parser.ClickHouseSqlStatement;
import com.clickhouse.jdbc.parser.StatementType;

public class ClickHouseStatementImpl extends JdbcWrapper
        implements ClickHouseConfigChangeListener<ClickHouseRequest<?>>, ClickHouseStatement {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseStatementImpl.class);

    private final ClickHouseConnection connection;
    private final ClickHouseRequest<?> request;

    private final int resultSetType;
    private final int resultSetConcurrency;
    private final int resultSetHoldability;

    private final List<ClickHouseSqlStatement> batchStmts;

    private boolean closed;
    private boolean closeOnCompletion;

    private String cursorName;
    private boolean escapeScan;
    private int fetchSize;
    private int maxFieldSize;
    private long maxRows;
    private int nullAsDefault;
    private boolean poolable;
    private volatile String queryId;
    private int queryTimeout;

    private ClickHouseResultSet currentResult;
    private long currentUpdateCount;

    private ClickHouseDataProcessor processor;

    protected final JdbcTypeMapping mapper;

    protected ClickHouseSqlStatement[] parsedStmts;

    private ClickHouseResponse getLastResponse(Map<ClickHouseOption, Serializable> options,
            List<ClickHouseExternalTable> tables, Map<String, String> settings) throws SQLException {
        boolean autoTx = connection.getAutoCommit() && connection.isTransactionSupported();

        // disable extremes
        if (parsedStmts.length > 1 && !request.getSessionId().isPresent()) {
            request.session(request.getManager().createSessionId());
        }
        ClickHouseResponse response = null;
        for (int i = 0, len = parsedStmts.length; i < len; i++) {
            ClickHouseSqlStatement stmt = parsedStmts[i];
            response = processSqlStatement(stmt);
            if (response != null) {
                updateResult(stmt, response);
                continue;
            }

            if (stmt.hasFormat()) {
                request.format(ClickHouseFormat.valueOf(stmt.getFormat()));
            }
            request.query(stmt.getSQL(), queryId = connection.newQueryId());
            // TODO skip useless queries to reduce network calls and server load
            try {
                response = autoTx ? request.executeWithinTransaction(connection.isImplicitTransactionSupported())
                        : request.transaction(connection.getTransaction()).executeAndWait();
            } catch (Exception e) {
                throw SqlExceptionUtils.handle(e);
            } finally {
                if (response == null) {
                    // something went wrong
                } else if (i + 1 < len) {
                    response.close();
                    response = null;
                } else {
                    updateResult(stmt, response);
                }
            }
        }

        return response;
    }

    protected void ensureOpen() throws SQLException {
        if (closed) {
            throw SqlExceptionUtils.clientError("Cannot operate on a closed statement");
        }
    }

    protected ClickHouseResponse processSqlStatement(ClickHouseSqlStatement stmt) throws SQLException {
        if (stmt.getStatementType() == StatementType.USE) {
            String dbName = connection.getCurrentDatabase();
            final String newDb = stmt.getDatabaseOrDefault(dbName);
            final boolean hasSession = request.getSessionId().isPresent();
            if (!hasSession) {
                request.session(request.getManager().createSessionId());
            }
            // execute the query to ensure 1) it's valid; and 2) the database exists
            try (ClickHouseResponse response = request.use(newDb).query(stmt.getSQL()).executeAndWait()) {
                connection.setCurrentDatabase(dbName = newDb, false);
            } catch (ClickHouseException e) {
                throw SqlExceptionUtils.handle(e);
            } finally {
                if (!dbName.equals(newDb)) {
                    request.use(dbName);
                }
                if (!hasSession) {
                    request.clearSession();
                }
            }
            return ClickHouseResponse.EMPTY;
        } else if (stmt.isTCL()) {
            if (stmt.containsKeyword(ClickHouseTransaction.COMMAND_BEGIN)) {
                connection.begin();
            } else if (stmt.containsKeyword(ClickHouseTransaction.COMMAND_COMMIT)) {
                connection.commit();
            } else if (stmt.containsKeyword(ClickHouseTransaction.COMMAND_ROLLBACK)) {
                connection.rollback();
            } else {
                throw new SQLFeatureNotSupportedException("Unsupported TCL: " + stmt.getSQL());
            }
            return ClickHouseResponse.EMPTY;
        }

        return null;
    }

    protected ClickHouseResponse executeStatement(String stmt, Map<ClickHouseOption, Serializable> options,
            List<ClickHouseExternalTable> tables, Map<String, String> settings) throws SQLException {
        boolean autoTx = connection.getAutoCommit() && connection.isTransactionSupported();
        try {
            if (options != null) {
                request.options(options);
            }
            if (settings != null && !settings.isEmpty()) {
                if (!request.getSessionId().isPresent()) {
                    request.session(request.getManager().createSessionId());
                }
                for (Entry<String, String> e : settings.entrySet()) {
                    request.set(e.getKey(), e.getValue());
                }
            }
            if (tables != null && !tables.isEmpty()) {
                List<ClickHouseExternalTable> list = new ArrayList<>(tables.size());
                char quote = '`';
                for (ClickHouseExternalTable t : tables) {
                    if (t.isTempTable()) {
                        if (!request.getSessionId().isPresent()) {
                            request.session(request.getManager().createSessionId());
                        }
                        String tableName = new StringBuilder().append(quote)
                                .append(ClickHouseUtils.escape(t.getName(), quote)).append(quote).toString();
                        try (ClickHouseResponse dropResp = request
                                .query("DROP TEMPORARY TABLE IF EXISTS ".concat(tableName)).executeAndWait();
                                ClickHouseResponse createResp = request
                                        .query("CREATE TEMPORARY TABLE " + tableName + "(" + t.getStructure() + ")")
                                        .executeAndWait();
                                ClickHouseResponse writeResp = request.write().table(tableName).data(t.getContent())
                                        .executeAndWait()) {
                            // ignore
                        }
                    } else {
                        list.add(t);
                    }
                }
                request.external(list);
            }
            request.query(stmt, queryId = connection.newQueryId());
            return autoTx ? request.executeWithinTransaction(connection.isImplicitTransactionSupported())
                    : request.transaction(connection.getTransaction()).executeAndWait();
        } catch (Exception e) {
            throw SqlExceptionUtils.handle(e);
        }
    }

    protected ClickHouseResponse executeStatement(ClickHouseSqlStatement stmt,
            Map<ClickHouseOption, Serializable> options, List<ClickHouseExternalTable> tables,
            Map<String, String> settings) throws SQLException {
        ClickHouseResponse resp = processSqlStatement(stmt);
        if (resp != null) {
            return resp;
        }
        return executeStatement(stmt.getSQL(), options, tables, settings);
    }

    protected int executeInsert(String sql, InputStream input) throws SQLException {
        boolean autoTx = connection.getAutoCommit() && connection.isTransactionSupported();
        Mutation req = request.write().query(sql, queryId = connection.newQueryId()).data(input);
        try (ClickHouseResponse resp = autoTx
                ? req.executeWithinTransaction(connection.isImplicitTransactionSupported())
                : req.transaction(connection.getTransaction()).executeAndWait();
                ResultSet rs = updateResult(new ClickHouseSqlStatement(sql, StatementType.INSERT), resp)) {
            // ignore
        } catch (Exception e) {
            throw SqlExceptionUtils.handle(e);
        }

        return (int) currentUpdateCount;
    }

    protected ClickHouseDataProcessor getDataProcessor(ClickHouseInputStream input, Map<String, Serializable> settings,
            ClickHouseColumn[] columns) throws SQLException {
        if (processor == null) {
            try {
                processor = ClickHouseDataStreamFactory.getInstance().getProcessor(getConfig(), input, null, settings,
                        Arrays.asList(columns));
            } catch (IOException e) {
                throw SqlExceptionUtils.clientError(e);
            }
        }
        return processor;
    }

    protected ClickHouseDataProcessor getDataProcessor(ClickHouseOutputStream output,
            Map<String, Serializable> settings, ClickHouseColumn[] columns) throws SQLException {
        if (processor == null) {
            try {
                processor = ClickHouseDataStreamFactory.getInstance().getProcessor(getConfig(), null, output, settings,
                        Arrays.asList(columns));
            } catch (IOException e) {
                throw SqlExceptionUtils.clientError(e);
            }
        }
        return processor;
    }

    protected void resetDataProcessor() {
        this.processor = null;
    }

    protected ClickHouseSqlStatement getLastStatement() {
        ClickHouseSqlStatement stmt = null;

        if (parsedStmts != null && parsedStmts.length > 0) {
            stmt = parsedStmts[parsedStmts.length - 1];
        }

        return ClickHouseChecker.nonNull(stmt, "ParsedStatement"); // NOSONAR
    }

    protected void setLastStatement(ClickHouseSqlStatement stmt) {
        if (parsedStmts != null && parsedStmts.length > 0) {
            parsedStmts[parsedStmts.length - 1] = ClickHouseChecker.nonNull(stmt, "ParsedStatement");
        }
    }

    protected ClickHouseSqlStatement parseSqlStatements(String sql) {
        parsedStmts = connection.parse(sql, getConfig(), request.getSettings());

        if (parsedStmts == null || parsedStmts.length == 0) {
            // should never happen
            throw new IllegalArgumentException("Failed to parse given SQL: " + sql);
        }

        return getLastStatement();
    }

    protected ClickHouseResultSet newEmptyResultSet() throws SQLException {
        return new ClickHouseResultSet("", "", this, ClickHouseResponse.EMPTY);
    }

    protected ResultSet updateResult(ClickHouseSqlStatement stmt, ClickHouseResponse response) throws SQLException {
        if (stmt.isQuery() || !response.getColumns().isEmpty()) {
            currentUpdateCount = -1L;
            currentResult = new ClickHouseResultSet(stmt.getDatabaseOrDefault(getConnection().getCurrentDatabase()),
                    stmt.getTable(), this, response);
        } else {
            response.close();
            currentUpdateCount = stmt.isDDL() || stmt.isTCL() ? 0L
                    : (response.getSummary().isEmpty() ? 1L : response.getSummary().getWrittenRows());
            currentResult = null;
        }
        return currentResult;
    }

    protected ClickHouseStatementImpl(ClickHouseConnectionImpl connection, ClickHouseRequest<?> request,
            int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (connection == null || request == null) {
            throw SqlExceptionUtils.clientError("Non-null connection and request are required");
        }

        this.connection = connection;
        this.request = request.setChangeListener(this);

        // TODO validate resultSet attributes
        this.resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        this.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        this.resultSetHoldability = ResultSet.CLOSE_CURSORS_AT_COMMIT;

        this.closed = false;
        this.closeOnCompletion = true;

        this.fetchSize = connection.getJdbcConfig().getFetchSize();
        this.maxFieldSize = 0;
        this.maxRows = 0L;
        this.nullAsDefault = connection.getJdbcConfig().getNullAsDefault();
        this.poolable = false;
        this.queryId = null;

        this.queryTimeout = 0;

        this.currentResult = null;
        this.currentUpdateCount = -1L;

        this.mapper = connection.getJdbcTypeMapping();

        this.batchStmts = new LinkedList<>();

        ClickHouseConfig c = request.getConfig();
        setLargeMaxRows(c.getMaxResultRows());
        setQueryTimeout(c.getMaxExecutionTime());

        optionChanged(this.request, ClickHouseClientOption.FORMAT, null, null);
    }

    @Override
    public void optionChanged(ClickHouseRequest<?> source, ClickHouseOption option, Serializable oldValue,
            Serializable newValue) {
        if (source != request) {
            return;
        }

        if (option == ClickHouseClientOption.FORMAT) {
            this.processor = null;
        }
    }

    @Override
    public void settingChanged(ClickHouseRequest<?> source, String setting, Serializable oldValue,
            Serializable newValue) {
        // ClickHouseConfigChangeListener.super.settingChanged(source, setting,
        // oldValue, newValue);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        executeQuery(sql);
        return currentResult != null;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        ensureOpen();
        if (!batchStmts.isEmpty()) {
            throw SqlExceptionUtils.undeterminedExecutionError();
        }

        parseSqlStatements(sql);
        getLastResponse(null, null, null);
        return currentResult != null ? currentResult : newEmptyResultSet();
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        ensureOpen();
        if (!batchStmts.isEmpty()) {
            throw SqlExceptionUtils.undeterminedExecutionError();
        }

        parseSqlStatements(sql);

        try (ClickHouseResponse response = getLastResponse(null, null, null)) {
            return currentUpdateCount;
        } catch (Exception e) {
            throw SqlExceptionUtils.handle(e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return (int) executeLargeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
        if (currentResult != null) {
            currentResult.close();
        }

        this.closed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        ensureOpen();

        return maxFieldSize;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        if (max < 0) {
            throw SqlExceptionUtils.clientError("Max field size cannot be set to negative number");
        }
        ensureOpen();

        maxFieldSize = max;
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        ensureOpen();

        return maxRows;
    }

    @Override
    public int getMaxRows() throws SQLException {
        return (int) getLargeMaxRows();
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        if (max < 0L) {
            throw SqlExceptionUtils.clientError("Max rows cannot be set to negative number");
        }
        ensureOpen();

        if (this.maxRows != max) {
            if (max == 0L || !connection.allowCustomSetting()) {
                request.removeSetting(ClickHouseClientOption.MAX_RESULT_ROWS.getKey());
                request.removeSetting("result_overflow_mode");
            } else {
                request.set(ClickHouseClientOption.MAX_RESULT_ROWS.getKey(), max);
                request.set("result_overflow_mode", "break");
            }
            this.maxRows = max;
        }
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        setLargeMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        ensureOpen();

        this.escapeScan = enable;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        ensureOpen();

        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) {
            throw SqlExceptionUtils.clientError("Query timeout cannot be set to negative seconds");
        }
        ensureOpen();

        if (this.queryTimeout != seconds) {
            if (seconds == 0) {
                request.removeSetting("max_execution_time");
            } else {
                request.set("max_execution_time", seconds);
            }
            this.queryTimeout = seconds;
        }
    }

    @Override
    public void cancel() throws SQLException {
        if (isClosed()) {
            return;
        }

        final String qid;
        if ((qid = this.queryId) != null) {
            String sessionIdKey = ClickHouseClientOption.SESSION_ID.getKey();
            ClickHouseNode server = request.getServer();
            if (server.getOptions().containsKey(sessionIdKey)) {
                server = ClickHouseNode.builder(request.getServer()).removeOption(sessionIdKey)
                        .removeOption(ClickHouseClientOption.SESSION_CHECK.getKey())
                        .removeOption(ClickHouseClientOption.SESSION_TIMEOUT.getKey()).build();
            }
            try {
                List<ClickHouseResponseSummary> summaries = ClickHouseClient
                        .send(server, String.format("KILL QUERY WHERE query_id='%s'", qid))
                        .get(request.getConfig().getConnectionTimeout(), TimeUnit.MILLISECONDS);
                log.info("Killed query [%s]: %s", qid, summaries.get(0));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Cancellation of query [%s] was interrupted", qid);
            } catch (TimeoutException e) {
                log.warn("Timed out after waiting %d ms for killing query [%s]",
                        request.getConfig().getConnectionTimeout(), qid);
            } catch (Exception e) { // unexpected
                throw SqlExceptionUtils.handle(e.getCause());
            }
        }
        if (request.getTransaction() != null) {
            request.getTransaction().abort();
        }
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
    public void setCursorName(String name) throws SQLException {
        ensureOpen();

        cursorName = name;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        ensureOpen();

        return currentResult;
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        ensureOpen();

        return currentUpdateCount;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return (int) getLargeUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        ensureOpen();

        if (currentResult != null) {
            currentResult.close();
            currentResult = null;
        }
        currentUpdateCount = -1L;
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        ensureOpen();

        if (direction != ResultSet.FETCH_FORWARD) {
            throw SqlExceptionUtils.unsupportedError("only FETCH_FORWARD is supported in setFetchDirection");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        ensureOpen();

        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows < 0) {
            log.warn("Negative fetch size is treated as 0.");
            rows = 0;
        }

        ensureOpen();

        if (fetchSize != rows) {
            fetchSize = rows;

            if (rows == 0) {
                request.removeOption(ClickHouseClientOption.READ_BUFFER_SIZE);
            } else {
                request.option(ClickHouseClientOption.READ_BUFFER_SIZE, rows * 1024);
            }
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        ensureOpen();

        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        ensureOpen();

        return resultSetConcurrency;
    }

    @Override
    public int getResultSetType() throws SQLException {
        ensureOpen();

        return resultSetType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        ensureOpen();

        for (ClickHouseSqlStatement s : connection.parse(sql, getConfig(), request.getSettings())) {
            this.batchStmts.add(s);
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        ensureOpen();

        this.batchStmts.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        long[] largeUpdateCounts = executeLargeBatch();

        int len = largeUpdateCounts.length;
        int[] results = new int[len];
        for (int i = 0; i < len; i++) {
            results[i] = (int) largeUpdateCounts[i];
        }
        return results;
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        ensureOpen();
        if (batchStmts.isEmpty()) {
            return ClickHouseValues.EMPTY_LONG_ARRAY;
        }

        boolean continueOnError = getConnection().getJdbcConfig().isContinueBatchOnError();
        long[] results = new long[batchStmts.size()];
        try {
            int i = 0;
            for (ClickHouseSqlStatement s : batchStmts) {
                try (ClickHouseResponse r = executeStatement(s, null, null, null); ResultSet rs = updateResult(s, r)) {
                    if (rs != null) {
                        throw SqlExceptionUtils.queryInBatchError(results);
                    }
                    results[i] = currentUpdateCount <= 0L ? 0L : currentUpdateCount;
                } catch (Exception e) {
                    results[i] = EXECUTE_FAILED;
                    if (!continueOnError) {
                        throw SqlExceptionUtils.batchUpdateError(e, results);
                    }
                    log.error("Faled to execute task %d of %d", i + 1, batchStmts.size(), e);
                } finally {
                    i++;
                }
            }
        } finally {
            clearBatch();
        }

        return results;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        ensureOpen();

        switch (current) {
            case Statement.KEEP_CURRENT_RESULT:
                break;
            case Statement.CLOSE_CURRENT_RESULT:
            case Statement.CLOSE_ALL_RESULTS:
                if (currentResult != null) {
                    currentResult.close();
                }
                break;
            default:
                throw SqlExceptionUtils.clientError("Unknown statement constants: " + current);
        }
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        ensureOpen();

        return new ClickHouseResultSet(request.getConfig().getDatabase(), ClickHouseSqlStatement.DEFAULT_TABLE, this,
                ClickHouseResponse.EMPTY);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        ensureOpen();

        return resultSetHoldability;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        ensureOpen();

        this.poolable = poolable;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        ensureOpen();

        return poolable;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        ensureOpen();

        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        ensureOpen();

        return closeOnCompletion;
    }

    @Override
    public ClickHouseConnection getConnection() throws SQLException {
        ensureOpen();

        return connection;
    }

    @Override
    public ClickHouseConfig getConfig() {
        return request.getConfig();
    }

    @Override
    public int getNullAsDefault() {
        return nullAsDefault;
    }

    @Override
    public void setNullAsDefault(int level) {
        this.nullAsDefault = level;
    }

    @Override
    public ClickHouseRequest<?> getRequest() {
        return request;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface == ClickHouseRequest.class || super.isWrapperFor(iface);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface == ClickHouseRequest.class ? iface.cast(request) : super.unwrap(iface);
    }
}
