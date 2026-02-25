package com.clickhouse.jdbc;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.sql.SQLUtils;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import com.clickhouse.jdbc.internal.FeatureManager;
import com.clickhouse.jdbc.internal.ParsedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class StatementImpl implements Statement, JdbcV2Wrapper {
    private static final Logger LOG = LoggerFactory.getLogger(StatementImpl.class);

    // Attributes
    ConnectionImpl connection;
    protected int queryTimeout;
    protected boolean isPoolable = false; // Statement is not poolable by default
    private final FeatureManager featureManager;

    // State
    private volatile boolean closed;
    private final ConcurrentLinkedQueue<ResultSetImpl> resultSets; // all result sets linked to this statement
    protected ResultSetImpl currentResultSet;
    protected long currentUpdateCount = -1;
    protected List<String> batch;
    private String lastStatementSql;
    private ParsedStatement parsedStatement;
    protected volatile String lastQueryId;
    private long maxRows;
    private boolean closeOnCompletion;
    private final boolean resultSetAutoClose;
    private int maxFieldSize;
    private boolean escapeProcessingEnabled;
    private final Supplier<String> queryIdGenerator;

    private int fetchSize = 1;

    // settings local to a statement
    protected QuerySettings localSettings;


    public StatementImpl(ConnectionImpl connection) throws SQLException {
        this.connection = connection;
        this.queryTimeout = 0;
        this.closed = false;
        this.batch = new ArrayList<>();
        this.maxRows = 0;
        this.localSettings = QuerySettings.merge(connection.getDefaultQuerySettings(), new QuerySettings());
        this.resultSets=  new ConcurrentLinkedQueue<>();
        this.resultSetAutoClose = connection.getJdbcConfig().isSet(DriverProperties.RESULTSET_AUTO_CLOSE);
        this.escapeProcessingEnabled = true;
        this.featureManager = new FeatureManager(connection.getJdbcConfig());
        this.queryIdGenerator = connection.getJdbcConfig().getQueryIdGenerator();
    }

    protected void ensureOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed", ExceptionUtils.SQL_STATE_CONNECTION_EXCEPTION);
        }
    }

    private String parseJdbcEscapeSyntax(String sql) {
        LOG.trace("Original SQL: {}", sql);
        if (escapeProcessingEnabled) {
            sql = escapedSQLToNative(sql);
        }
        LOG.trace("Escaped SQL: {}", sql);
        return sql;
    }

    public static String escapedSQLToNative(String sql) {
        if (sql == null) {
            throw new IllegalArgumentException("SQL may not be null");
        }
        // Replace {d 'YYYY-MM-DD'} with corresponding SQL date format
        sql = sql.replaceAll("\\{d '([^']*)'\\}", "toDate('$1')");

        // Replace {ts 'YYYY-MM-DD HH:mm:ss'} with corresponding SQL timestamp format
        sql = sql.replaceAll("\\{ts '([^']*)'\\}", "timestamp('$1')");

        // Replace function escape syntax {fn <function>} (e.g., {fn UCASE(name)})
        sql = sql.replaceAll("\\{fn ([^\\}]*)\\}", "$1");

        // Handle outer escape syntax
        //sql = sql.replaceAll("\\{escape '([^']*)'\\}", "'$1'");

        // Note: do not remove new lines because they may be used to delimit comments
        // Add more replacements as needed for other JDBC escape sequences

        return sql;
    }

    protected String getLastStatementSql() {
        return lastStatementSql;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        ensureOpen();
        currentUpdateCount = -1;
        currentResultSet = executeQueryImpl(sql, localSettings);
        return currentResultSet;
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

    /**
     * Sets last queryId and returns actual query Id
     * Accepts null
     * @param queryId
     * @return
     */
    protected String setLastQueryID(String queryId) {
        if (queryId == null) {
            queryId = queryIdGenerator == null ? UUID.randomUUID().toString() : queryIdGenerator.get();
        }
        lastQueryId = queryId;
        LOG.debug("Query ID: {}", lastQueryId);
        return queryId;
    }

    protected ResultSetImpl executeQueryImpl(String sql, QuerySettings settings) throws SQLException {
        ensureOpen();

        // Closing before trying to do next request. Otherwise, deadlock because previous connection will not be
        // release before this one completes.
        if (resultSetAutoClose) {
            closeCurrentResultSet();
            // There is a feature `closeOnComplete` that dictates closing the statement when all
            // result sets are closed. Call to `closeCurrentResultSet` will trigger this statement
            // closure. But it should not happen because this was introduces instead of spec and will be removed in the future.
            // So we need to make this statement open again because we're going to create a new result set.
            this.closed = false;
        }

        QuerySettings mergedSettings = QuerySettings.merge(settings, new  QuerySettings());
        mergedSettings.setQueryId(setLastQueryID(mergedSettings.getQueryId()));
        QueryResponse response = null;
        try {
            lastStatementSql = parseJdbcEscapeSyntax(sql);
            LOG.trace("SQL Query: {}", lastStatementSql); // this is not secure for create statements because of passwords
            if (queryTimeout == 0) {
                response = connection.getClient().query(lastStatementSql, mergedSettings).get();
            } else {
                response = connection.getClient().query(lastStatementSql, mergedSettings).get(queryTimeout, TimeUnit.SECONDS);
            }

            if (response.getFormat().isText()) {
                throw new SQLException("Only RowBinaryWithNameAndTypes is supported for output format. Please check your query.",
                        ExceptionUtils.SQL_STATE_CLIENT_ERROR);
            }
            ClickHouseBinaryFormatReader reader = connection.getClient().newBinaryFormatReader(response);
            if (reader.getSchema() == null) {
                reader.close();
                throw new SQLException("Called method expects empty or filled result set but query has returned none. Consider using `java.sql.Statement.execute(java.lang.String)`", ExceptionUtils.SQL_STATE_CLIENT_ERROR);
            }
            return new ResultSetImpl(this, response, reader, this::handleSocketTimeoutException);
        } catch (Exception e) {
            if (response != null) {
                try {
                    response.close();
                } catch (Exception ex) {
                    LOG.warn("Failed to close response after exception", e);
                }
            }
            handleSocketTimeoutException(e);
            onResultSetClosed(null);
            throw ExceptionUtils.toSqlState(e);
        }
    }

    protected void handleSocketTimeoutException(Exception e) {
        if (e.getCause() instanceof SocketTimeoutException || e instanceof SocketTimeoutException) {
            this.connection.onNetworkTimeout();
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        ensureOpen();
        return (int)executeLargeUpdate(sql);
    }

    protected long executeUpdateImpl(String sql, QuerySettings settings) throws SQLException {
        ensureOpen();

        // Closing before trying to do next request. Otherwise, deadlock because previous connection will not be
        // release before this one completes.
        if (resultSetAutoClose) {
            closeCurrentResultSet();
        }

        QuerySettings mergedSettings = QuerySettings.merge(connection.getDefaultQuerySettings(), settings);
        mergedSettings.setQueryId(setLastQueryID(mergedSettings.getQueryId()));
        lastStatementSql = parseJdbcEscapeSyntax(sql);
        LOG.trace("SQL Query: {}", lastStatementSql);
        int updateCount = 0;
        try (QueryResponse response = queryTimeout == 0 ? connection.getClient().query(lastStatementSql, mergedSettings).get()
                : connection.getClient().query(lastStatementSql, mergedSettings).get(queryTimeout, TimeUnit.SECONDS)) {
            updateCount = Math.max(0, (int) response.getWrittenRows()); // when statement alters schema no result rows returned.
            lastQueryId = response.getQueryId();
        } catch (Exception e) {
            handleSocketTimeoutException(e);
            throw ExceptionUtils.toSqlState(e);
        }

        return updateCount;
    }

    private void postUpdateActions() throws SQLException {
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
        for (ResultSetImpl resultSet : resultSets) {
            if (resultSet != null &&  !resultSet.isClosed()) {
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
        return this.maxFieldSize;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        ensureOpen();
        if (max < 0) {
            throw new SQLException("max should be a  positive integer.");
        }
        this.maxFieldSize = max;
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
        this.escapeProcessingEnabled = enable;
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

        try (QueryResponse response = connection.getClient().query(String.format("KILL QUERY%sWHERE query_id = '%s'",
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
        featureManager.unsupportedFeatureThrow("setCursorName(String)", true);
        ensureOpen();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        ensureOpen();
        parsedStatement = connection.getSqlParser().parsedStatement(sql);
        currentUpdateCount = -1;
        currentResultSet = null;
        if (parsedStatement.isHasResultSet()) {
            currentResultSet = executeQueryImpl(sql, localSettings);
            return true;
        } else {
            currentUpdateCount = executeUpdateImpl(sql, localSettings);
            postUpdateActions();
            return false;
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        ensureOpen();

        return currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        ensureOpen();
        return (int) getLargeUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        ensureOpen();
        return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        ensureOpen();
        if (direction != ResultSet.FETCH_FORWARD && direction != ResultSet.FETCH_REVERSE && direction != ResultSet.FETCH_UNKNOWN) {
            throw new SQLException("Invalid fetch direction: " + direction + ". Should be one of ResultSet.FETCH_FORWARD, ResultSet.FETCH_REVERSE, or ResultSet.FETCH_UNKNOWN");
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
        if (rows < 0) {
            throw new SQLException("rows should be greater or equal to 0.");
        }
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        ensureOpen();
        return fetchSize;
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
        clearBatch();
        return results;
    }

    @Override
    public ConnectionImpl getConnection() throws SQLException {
        return connection;
    }

    /**
     * Returns instance of local settings. Can be used to override settings.
     *
     * @return QuerySettings that is used as base for each request.
     */
    public QuerySettings getLocalSettings() {
        return localSettings;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        // This method designed to iterate over multiple resultsets after "execute(sql)" method is called
        // But we have at most only one always
        // Then we should close any existing and return false to indicate that no more result are present

        if (currentResultSet != null && current != Statement.KEEP_CURRENT_RESULT) {
            currentResultSet.close();
        }

        currentResultSet = null;
        currentUpdateCount = -1;
        return false; // false indicates that no more results (or it is an update count)
    }

//    @Override -- because doesn't exist in Java 8
    public String enquoteLiteral(String val) throws SQLException {
        return SQLUtils.enquoteLiteral(val);
    }

//    @Override -- because doesn't exist in Java 8
    public String enquoteIdentifier(String identifier, boolean alwaysQuote) throws SQLException {
        return SQLUtils.enquoteIdentifier(identifier, alwaysQuote);
    }

//    @Override -- because doesn't exist in Java 8
    public boolean isSimpleIdentifier(String identifier) throws SQLException {
        return SQLUtils.isSimpleIdentifier(identifier);
    }

//    @Override -- because doesn't exist in Java 8
    public String enquoteNCharLiteral(String val) throws SQLException {
        if (val == null) {
            throw new NullPointerException();
        }
        return "N" + SQLUtils.enquoteLiteral(val);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        // TODO: return empty result set or throw exception
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        featureManager.unsupportedFeatureThrow("executeUpdate(String, int)", autoGeneratedKeys != Statement.NO_GENERATED_KEYS);
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        featureManager.unsupportedFeatureThrow("executeUpdate(String, int[])");
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        featureManager.unsupportedFeatureThrow("executeUpdate(String, String[])");
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        featureManager.unsupportedFeatureThrow("execute(String, int)", autoGeneratedKeys != Statement.NO_GENERATED_KEYS);
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        featureManager.unsupportedFeatureThrow("execute(String, int[])");
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        featureManager.unsupportedFeatureThrow("execute(String, String[])");
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
        this.closeOnCompletion = true;
    }

    // called each time query is complete or result set is closed
    public void onResultSetClosed(ResultSetImpl resultSet) throws SQLException {
        if (resultSet != null) {
            this.resultSets.remove(resultSet);
        }

        if (this.closeOnCompletion) {
            if ((resultSets.isEmpty()) && (currentResultSet == null || currentResultSet.isClosed())) {
                // last result set is closed.
                this.closed = true;
            }
        }
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return this.closeOnCompletion;
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        ensureOpen();
        return currentUpdateCount;
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        ensureOpen();
        maxRows = max;

        if (connection.getJdbcConfig().isFlagSet(DriverProperties.USE_MAX_RESULT_ROWS)) {
            // This method override user set overflow mode on purpose:
            // 1. Spec clearly states that after calling this method with a limit > 0 all rows over limit are dropped.
            // 2. Calling this method should not cause throwing exception for future queries what only `break` can guarantee
            // 3. If user wants different behavior then they are can use connection properties.
            if (max > 0) {
                localSettings.setOption(ClientConfigProperties.serverSetting(ServerSettings.MAX_RESULT_ROWS), maxRows);
                localSettings.setOption(ClientConfigProperties.serverSetting(ServerSettings.RESULT_OVERFLOW_MODE),
                        ServerSettings.RESULT_OVERFLOW_MODE_BREAK);
            } else {
                // overriding potential client settings (set thru connection setup)
                // there is no no limit value so we use very large limit.
                localSettings.setOption(ClientConfigProperties.serverSetting(ServerSettings.MAX_RESULT_ROWS), Long.MAX_VALUE);
                localSettings.setOption(ClientConfigProperties.serverSetting(ServerSettings.RESULT_OVERFLOW_MODE),
                        ServerSettings.RESULT_OVERFLOW_MODE_BREAK);
            }
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
        parsedStatement = connection.getSqlParser().parsedStatement(sql);
        long updateCount = executeUpdateImpl(sql, localSettings);
        postUpdateActions();
        return updateCount;
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        featureManager.unsupportedFeatureThrow("executeLargeUpdate(String, int)", autoGeneratedKeys != Statement.NO_GENERATED_KEYS);
        return executeLargeUpdate(sql);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        featureManager.unsupportedFeatureThrow("executeLargeUpdate(String, int[])");
        return executeLargeUpdate(sql);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        featureManager.unsupportedFeatureThrow("executeLargeUpdate(String, String[])");
        return executeLargeUpdate(sql);
    }

    /**
     * Return query ID of last executed statement. It is not guaranteed when statements is used concurrently.
     * @return query ID
     */
    public String getLastQueryId() {
        return lastQueryId;
    }
}
