package com.clickhouse.jdbc;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import com.clickhouse.jdbc.internal.ParsedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class StatementImpl implements Statement, JdbcV2Wrapper {
    private static final Logger LOG = LoggerFactory.getLogger(StatementImpl.class);

    // Attributes
    ConnectionImpl connection;
    protected int queryTimeout;
    protected boolean isPoolable = false; // Statement is not poolable by default

    // State
    private volatile boolean closed;
    private final ConcurrentLinkedQueue<WeakReference<ResultSetImpl>> resultSets;
    protected ResultSetImpl currentResultSet;
    protected OperationMetrics metrics;
    protected List<String> batch;
    private String lastStatementSql;
    private ParsedStatement parsedStatement;
    protected volatile String lastQueryId;
    private long maxRows;
    protected QuerySettings localSettings;

    public StatementImpl(ConnectionImpl connection) throws SQLException {
        this.connection = connection;
        this.queryTimeout = 0;
        this.closed = false;
        this.metrics = null;
        this.batch = new ArrayList<>();
        this.maxRows = 0;
        this.localSettings = QuerySettings.merge(connection.getDefaultQuerySettings(), new QuerySettings());
        this.resultSets=  new ConcurrentLinkedQueue<>();
    }

    protected void ensureOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed", ExceptionUtils.SQL_STATE_CONNECTION_EXCEPTION);
        }
    }


    protected static String parseJdbcEscapeSyntax(String sql) {
        LOG.trace("Original SQL: {}", sql);
        // Replace {d 'YYYY-MM-DD'} with corresponding SQL date format
        sql = sql.replaceAll("\\{d '([^']*)'\\}", "toDate('$1')");

        // Replace {ts 'YYYY-MM-DD HH:mm:ss'} with corresponding SQL timestamp format
        sql = sql.replaceAll("\\{ts '([^']*)'\\}", "timestamp('$1')");

        // Replace function escape syntax {fn <function>} (e.g., {fn UCASE(name)})
        sql = sql.replaceAll("\\{fn ([^\\}]*)\\}", "$1");

        // Handle outer escape syntax
        //sql = sql.replaceAll("\\{escape '([^']*)'\\}", "'$1'");

        // Clean new empty lines in sql
        sql = sql.replaceAll("(?m)^\\s*$\\n?", "");
        // Add more replacements as needed for other JDBC escape sequences
        LOG.trace("Parsed SQL: {}", sql);
        return sql;
    }

    protected String getLastStatementSql() {
        return lastStatementSql;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        ensureOpen();
        return executeQueryImpl(sql, localSettings);
    }

    private void closeCurrentResultSet() {
        if (currentResultSet != null) {
            LOG.debug("Previous result set is open [resultSet = " + currentResultSet + "]");
            // Closing request blindly assuming that user do not care about it anymore (DDL request for example)
            try {
                currentResultSet.close();
            } catch (Exception e) {
                LOG.error("Failed to close previous result set", e);
            } finally {
                currentResultSet = null; // no need to remember we have closed it already
            }
        }
    }

    protected ResultSetImpl executeQueryImpl(String sql, QuerySettings settings) throws SQLException {
        ensureOpen();

        // TODO: method should throw exception if no result set returned

        // Closing before trying to do next request. Otherwise, deadlock because previous connection will not be
        // release before this one completes.
        closeCurrentResultSet();

        QuerySettings mergedSettings = QuerySettings.merge(connection.getDefaultQuerySettings(), settings);
        if (maxRows > 0) {
            mergedSettings.setOption(ClientConfigProperties.serverSetting(ServerSettings.MAX_RESULT_ROWS), maxRows);
            mergedSettings.setOption(ClientConfigProperties.serverSetting(ServerSettings.RESULT_OVERFLOW_MODE), "break");
        }

        if (mergedSettings.getQueryId() != null) {
            lastQueryId = mergedSettings.getQueryId();
        } else {
            lastQueryId = UUID.randomUUID().toString();
            mergedSettings.setQueryId(lastQueryId);
        }
        LOG.debug("Query ID: {}", lastQueryId);

        try {
            lastStatementSql = parseJdbcEscapeSyntax(sql);
            LOG.trace("SQL Query: {}", lastStatementSql); // this is not secure for create statements because of passwords
            QueryResponse response;
            if (queryTimeout == 0) {
                response = connection.client.query(lastStatementSql, mergedSettings).get();
            } else {
                response = connection.client.query(lastStatementSql, mergedSettings).get(queryTimeout, TimeUnit.SECONDS);
            }

            if (response.getFormat().isText()) {
                throw new SQLException("Only RowBinaryWithNameAndTypes is supported for output format. Please check your query.",
                        ExceptionUtils.SQL_STATE_CLIENT_ERROR);
            }
            ClickHouseBinaryFormatReader reader = connection.client.newBinaryFormatReader(response);

            metrics = response.getMetrics();
            setCurrentResultSet(new ResultSetImpl(this, response, reader));
            return currentResultSet;
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        ensureOpen();
        parsedStatement = connection.getSqlParser().parsedStatement(sql);
        int updateCount = executeUpdateImpl(sql, localSettings);
        postUpdateActions();
        return updateCount;
    }

    protected int executeUpdateImpl(String sql, QuerySettings settings) throws SQLException {
        ensureOpen();

        // TODO: method should throw exception if result set returned
        // Closing before trying to do next request. Otherwise, deadlock because previous connection will not be
        // release before this one completes.
        closeCurrentResultSet();

        QuerySettings mergedSettings = QuerySettings.merge(connection.getDefaultQuerySettings(), settings);

        if (mergedSettings.getQueryId() != null) {
            lastQueryId = mergedSettings.getQueryId();
        } else {
            lastQueryId = UUID.randomUUID().toString();
            mergedSettings.setQueryId(lastQueryId);
        }

        lastStatementSql = parseJdbcEscapeSyntax(sql);
        LOG.trace("SQL Query: {}", lastStatementSql);
        int updateCount = 0;
        try (QueryResponse response = queryTimeout == 0 ? connection.client.query(lastStatementSql, mergedSettings).get()
                : connection.client.query(lastStatementSql, mergedSettings).get(queryTimeout, TimeUnit.SECONDS)) {
            setCurrentResultSet(null);
            updateCount = Math.max(0, (int) response.getWrittenRows()); // when statement alters schema no result rows returned.
            metrics = response.getMetrics();
            lastQueryId = response.getQueryId();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }

        return updateCount;
    }

    protected void postUpdateActions() {
        if (parsedStatement.getUseDatabase() != null) {
            this.localSettings.setDatabase(parsedStatement.getUseDatabase());
        }

        if (parsedStatement.getRoles() != null) {
            this.connection.getClient().setDBRoles(parsedStatement.getRoles());
            this.localSettings.setDBRoles(parsedStatement.getRoles());
        }
    }

    @Override
    public void close() throws SQLException {
        closed = true;
        closeCurrentResultSet();
        for (WeakReference<ResultSetImpl> refRs : resultSets) {
            ResultSetImpl resultSet = refRs.get();
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception e) {
                    LOG.error("Failed to close result set", e);
                }
            }
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        ensureOpen();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        ensureOpen();
        if (!connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Set max field size is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public int getMaxRows() throws SQLException {
        ensureOpen();
        return (int) getLargeMaxRows(); // skip overflow check. 
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        setLargeMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        ensureOpen();
        //TODO: Should we support this?
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        ensureOpen();
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        ensureOpen();
        queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        if (closed) {
            return;
        }

        try (QueryResponse response = connection.client.query(String.format("KILL QUERY%sWHERE query_id = '%s'",
                connection.onCluster ? " ON CLUSTER " + connection.cluster + " " : " ",
                lastQueryId), connection.getDefaultQuerySettings()).get()){
            LOG.debug("Query {} was killed by {}", lastQueryId, response.getQueryId());
        } catch (Exception e) {
            throw new SQLException(e);
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
    }

    /**
     * Remembers current result set to be able to close it later.
     * Sets current resultset to a new value
     * @param resultSet new current resultset
     */
    protected void setCurrentResultSet(ResultSetImpl resultSet) {
        ResultSetImpl tmp = currentResultSet;
        currentResultSet = resultSet;
        if (tmp != null) {
            resultSets.add(new WeakReference<>(tmp));
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        ensureOpen();
        parsedStatement = connection.getSqlParser().parsedStatement(sql);
        if (parsedStatement.isHasResultSet()) {
            executeQueryImpl(sql, localSettings); // keep open to allow getResultSet()
            return true;
        } else {
            executeUpdateImpl(sql, localSettings);
            postUpdateActions();
            return false;
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        ensureOpen();

        ResultSet resultSet = currentResultSet;
        setCurrentResultSet(null);
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        ensureOpen();
        if (currentResultSet == null && metrics != null) {
            int updateCount = (int) metrics.getMetric(ServerMetrics.NUM_ROWS_WRITTEN).getLong();
            metrics = null;// clear metrics
            return updateCount;
        }

        return -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        ensureOpen();
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        ensureOpen();
        if (!connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Set fetch direction is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        ensureOpen();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        ensureOpen();
    }

    @Override
    public int getFetchSize() throws SQLException {
        ensureOpen();
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        ensureOpen();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        ensureOpen();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        ensureOpen();
        batch.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        ensureOpen();
        batch.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return executeBatchImpl().stream().mapToInt(i -> i).toArray();
    }

    private List<Integer> executeBatchImpl() throws SQLException {
        ensureOpen();
        List<Integer> results = new ArrayList<>();
        for (String sql : batch) {
            results.add(executeUpdate(sql));
        }
        return results;
    }

    @Override
    public ConnectionImpl getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        // TODO: implement query batches. When multiple selects in the batch.
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        // TODO: return empty result set or throw exception
        return null;
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
        return ResultSet.HOLD_CURSORS_OVER_COMMIT; // we do not have transactions and result must be closed by app.
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        ensureOpen();
        this.isPoolable = poolable;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return isPoolable;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        ensureOpen();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        ensureOpen();
        return getUpdateCount();
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        ensureOpen();
        maxRows = max;
        if (max > 0) {
            localSettings.setOption(ClientConfigProperties.serverSetting(ServerSettings.MAX_RESULT_ROWS), maxRows);
            localSettings.setOption(ClientConfigProperties.serverSetting(ServerSettings.RESULT_OVERFLOW_MODE), "break");
        } else {
            localSettings.resetOption(ClientConfigProperties.serverSetting(ServerSettings.MAX_RESULT_ROWS));
            localSettings.resetOption(ClientConfigProperties.serverSetting(ServerSettings.RESULT_OVERFLOW_MODE));
        }
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        ensureOpen();
        return this.maxRows;
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        return executeBatchImpl().stream().mapToLong(Integer::longValue).toArray();
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql, columnIndexes);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql, columnNames);
    }

    /**
     * Return query ID of last executed statement. It is not guaranteed when statements is used concurrently.
     * @return query ID
     */
    public String getLastQueryId() {
        return lastQueryId;
    }
}
