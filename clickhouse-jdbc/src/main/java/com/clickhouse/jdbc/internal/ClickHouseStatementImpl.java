package com.clickhouse.jdbc.internal;

import java.io.InputStream;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseOption;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.client.data.ClickHouseSimpleResponse;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseResultSet;
import com.clickhouse.jdbc.ClickHouseStatement;
import com.clickhouse.jdbc.SqlExceptionUtils;
import com.clickhouse.jdbc.JdbcWrapper;
import com.clickhouse.jdbc.parser.ClickHouseSqlStatement;

public class ClickHouseStatementImpl extends JdbcWrapper implements ClickHouseStatement {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseStatementImpl.class);

    private final ClickHouseConnection connection;
    private final ClickHouseRequest<?> request;

    private final int resultSetType;
    private final int resultSetConcurrency;
    private final int resultSetHoldability;

    private boolean closed;
    private boolean closeOnCompletion;

    private String cursorName;
    private boolean escapeScan;
    private int fetchSize;
    private int maxFieldSize;
    private int maxRows;
    private boolean poolable;
    private String queryId;
    private int queryTimeout;

    private ClickHouseResultSet currentResult;
    private int currentUpdateCount;

    protected ClickHouseSqlStatement[] parsedStmts;
    protected List<ClickHouseSqlStatement> batchStmts;

    private ClickHouseResponse getLastResponse(Map<ClickHouseOption, Serializable> options,
            List<ClickHouseExternalTable> tables, Map<String, String> settings) throws SQLException {
        // disable extremes
        if (parsedStmts.length > 1) {
            request.session(UUID.randomUUID().toString());
        }
        ClickHouseResponse response = null;
        for (int i = 0, len = parsedStmts.length; i < len; i++) {
            ClickHouseSqlStatement stmt = parsedStmts[i];
            // TODO skip useless queries to reduce network calls and server load
            try {
                response = request.query(stmt.getSQL(), queryId = connection.newQueryId()).execute().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw SqlExceptionUtils.forCancellation(e);
            } catch (Exception e) {
                throw SqlExceptionUtils.handle(e);
            } finally {
                if (i + 1 < len && response != null) {
                    response.close();
                }
            }
        }

        return response;
    }

    protected ClickHouseSqlStatement applyFormat(ClickHouseSqlStatement stmt, ClickHouseFormat preferredFormat) {
        /*
         * if (ClickHouseChecker.nonNull(stmt, "ParsedStatement").isQuery() &&
         * !stmt.hasFormat()) { String sql = stmt.getSQL(); String format =
         * ClickHouseChecker.nonNull(preferredFormat, "Format").name();
         * 
         * Map<String, Integer> positions = new HashMap<>();
         * positions.putAll(stmt.getPositions());
         * positions.put(ClickHouseSqlStatement.KEYWORD_FORMAT, sql.length());
         * 
         * sql = new StringBuilder(sql).append("\nFORMAT ").append(format).toString();
         * stmt = new ClickHouseSqlStatement(sql, stmt.getStatementType(),
         * stmt.getCluster(), stmt.getDatabase(), stmt.getTable(), format,
         * stmt.getOutfile(), stmt.getParameters(), positions); }
         */

        return stmt;
    }

    protected void ensureOpen() throws SQLException {
        if (closed) {
            throw SqlExceptionUtils.clientError("Cannot operate on a closed statement");
        }
    }

    protected ClickHouseResponse executeStatement(String stmt,
            Map<ClickHouseOption, Serializable> options, List<ClickHouseExternalTable> tables,
            Map<String, String> settings) throws SQLException {
        try {
            if (options != null) {
                request.options(options);
            }
            if (settings != null && !settings.isEmpty()) {
                if (!request.getSessionId().isPresent()) {
                    request.session(UUID.randomUUID().toString());
                }
                for (Entry<String, String> e : settings.entrySet()) {
                    request.set(e.getKey(), e.getValue());
                }
            }
            if (tables != null && !tables.isEmpty()) {
                List<ClickHouseExternalTable> list = new ArrayList<>(tables.size());
                for (ClickHouseExternalTable t : tables) {
                    if (t.isTempTable()) {
                        if (!request.getSessionId().isPresent()) {
                            request.session(UUID.randomUUID().toString());
                        }
                        request.query("drop temporary table if exists `" + t.getName() + "`").execute().get();
                        request.query("create temporary table `" + t.getName() + "`(" + t.getStructure() + ")")
                                .execute().get();
                        request.write()
                                .table(t.getName())
                                .format(t.getFormat() != null ? t.getFormat() : ClickHouseFormat.RowBinary)
                                .data(t.getContent()).send().get();
                    } else {
                        list.add(t);
                    }
                }
                request.external(list);
            }

            return request.query(stmt, queryId = connection.newQueryId()).execute().get();
        } catch (InterruptedException e) {
            log.error("can not close stream: %s", e.getMessage());
            Thread.currentThread().interrupt();
            throw SqlExceptionUtils.forCancellation(e);
        } catch (Exception e) {
            throw SqlExceptionUtils.handle(e);
        }
    }

    protected ClickHouseResponse executeStatement(ClickHouseSqlStatement stmt,
            Map<ClickHouseOption, Serializable> options, List<ClickHouseExternalTable> tables,
            Map<String, String> settings) throws SQLException {
        // stmt = applyFormat(stmt, request.getFormat());
        return executeStatement(stmt.getSQL(), options, tables, settings);
    }

    protected int executeInsert(String sql, InputStream input) throws SQLException {
        ClickHouseResponseSummary summary = null;
        try (ClickHouseResponse resp = request.write().query(sql, queryId = connection.newQueryId())
                .format(ClickHouseFormat.RowBinary).data(input).execute()
                .get()) {
            summary = resp.getSummary();
        } catch (InterruptedException e) {
            log.error("can not close stream: %s", e.getMessage());
            Thread.currentThread().interrupt();
            throw SqlExceptionUtils.forCancellation(e);
        } catch (Exception e) {
            throw SqlExceptionUtils.handle(e);
        }

        return summary != null ? (int) summary.getWrittenRows() : 1;
    }

    protected ClickHouseSqlStatement getLastStatement() {
        ClickHouseSqlStatement stmt = null;

        if (parsedStmts != null && parsedStmts.length > 0) {
            stmt = parsedStmts[parsedStmts.length - 1];
        }

        return ClickHouseChecker.nonNull(stmt, "ParsedStatement");
    }

    protected void setLastStatement(ClickHouseSqlStatement stmt) {
        if (parsedStmts != null && parsedStmts.length > 0) {
            parsedStmts[parsedStmts.length - 1] = ClickHouseChecker.nonNull(stmt, "ParsedStatement");
        }
    }

    protected ClickHouseSqlStatement parseSqlStatements(String sql) {
        parsedStmts = connection.parse(sql, getConfig());

        if (parsedStmts == null || parsedStmts.length == 0) {
            // should never happen
            throw new IllegalArgumentException("Failed to parse given SQL: " + sql);
        }

        ClickHouseSqlStatement lastStmt = getLastStatement();
        ClickHouseSqlStatement formattedStmt = applyFormat(lastStmt, request.getFormat());
        if (formattedStmt != lastStmt) {
            setLastStatement(lastStmt = formattedStmt);
        }

        return lastStmt;
    }

    protected ResultSet updateResult(ClickHouseSqlStatement stmt, ClickHouseResponse response) throws SQLException {
        ResultSet rs = null;
        if (stmt.isQuery() || !response.getColumns().isEmpty()) {
            currentUpdateCount = -1;
            currentResult = new ClickHouseResultSet(stmt.getDatabaseOrDefault(getConnection().getCurrentDatabase()),
                    stmt.getTable(), this, response);
            rs = currentResult;
        } else {
            currentUpdateCount = response.getSummary().getUpdateCount();
            response.close();
        }

        return rs;
    }

    protected ClickHouseStatementImpl(ClickHouseConnectionImpl connection, ClickHouseRequest<?> request,
            int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (connection == null || request == null) {
            throw SqlExceptionUtils.clientError("Non-null connection and request are required");
        }

        this.connection = connection;
        this.request = request;

        // TODO validate resultSet attributes
        this.resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        this.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        this.resultSetHoldability = ResultSet.CLOSE_CURSORS_AT_COMMIT;

        this.closed = false;
        this.closeOnCompletion = true;

        this.fetchSize = connection.getJdbcConfig().getFetchSize();
        this.maxFieldSize = 0;
        this.maxRows = 0;
        this.poolable = false;
        this.queryId = null;

        this.queryTimeout = 0;

        this.currentResult = null;
        this.currentUpdateCount = -1;

        this.batchStmts = new ArrayList<>();

        ClickHouseConfig c = request.getConfig();
        setMaxRows(c.getMaxResultRows());
        setQueryTimeout(c.getMaxExecutionTime());
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        ensureOpen();
        // forcibly disable extremes for ResultSet queries
        // additionalDBParams = importAdditionalDBParameters(additionalDBParams);
        // FIXME respect the value set in additionalDBParams?
        // additionalDBParams.put(ClickHouseQueryParam.EXTREMES, "0");

        parseSqlStatements(sql);

        ClickHouseResponse response = getLastResponse(null, null, null);

        try {
            return updateResult(getLastStatement(), response);
        } catch (Exception e) {
            if (response != null) {
                response.close();
            }

            throw SqlExceptionUtils.handle(e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        ensureOpen();

        parseSqlStatements(sql);

        ClickHouseResponseSummary summary = null;
        try (ClickHouseResponse response = getLastResponse(null, null, null)) {
            summary = response.getSummary();
        } catch (Exception e) {
            log.error("can not close stream: %s", e.getMessage());
        }

        return summary != null ? (int) summary.getWrittenRows() : 1;
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
    public int getMaxRows() throws SQLException {
        ensureOpen();

        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (max < 0) {
            throw SqlExceptionUtils.clientError("Max rows cannot be set to negative number");
        }
        ensureOpen();

        if (this.maxRows != max) {
            if (max == 0) {
                request.removeSetting("max_result_rows");
                request.removeSetting("result_overflow_mode");
            } else {
                request.set("max_result_rows", max);
                request.set("result_overflow_mode", "break");
            }
            this.maxRows = max;
        }
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
        if (this.queryId == null || isClosed()) {
            return;
        }

        executeQuery(String.format("KILL QUERY WHERE query_id='%s'", queryId));
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
    public boolean execute(String sql) throws SQLException {
        // currentResult is stored here. InputString and currentResult will be closed on
        // this.close()
        return executeQuery(sql) != null;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        ensureOpen();

        return currentResult;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        ensureOpen();

        return currentUpdateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        ensureOpen();

        if (currentResult != null) {
            currentResult.close();
            currentResult = null;
        }
        currentUpdateCount = -1;
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
            throw SqlExceptionUtils.clientError("Fetch size cannot be negative number");
        }

        ensureOpen();

        if (fetchSize != rows) {
            fetchSize = rows;

            if (rows == 0) {
                request.removeOption(ClickHouseClientOption.MAX_BUFFER_SIZE);
            } else {
                request.option(ClickHouseClientOption.MAX_BUFFER_SIZE, rows * 1024);
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

        for (ClickHouseSqlStatement s : connection.parse(sql, getConfig())) {
            this.batchStmts.add(s);
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        ensureOpen();

        this.batchStmts = new ArrayList<>();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        ensureOpen();

        int len = batchStmts.size();
        int[] results = new int[len];
        for (int i = 0; i < len; i++) {
            try (ClickHouseResponse r = executeStatement(batchStmts.get(i), null, null, null)) {
                results[i] = (int) r.getSummary().getWrittenRows();
            } catch (Exception e) {
                results[i] = EXECUTE_FAILED;
                log.error("Faled to execute task %d of %d", i + 1, len, e);
            }
        }

        clearBatch();

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

        return new ClickHouseResultSet(request.getConfig().getDatabase(), "unknown", this,
                ClickHouseSimpleResponse.EMPTY);
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
