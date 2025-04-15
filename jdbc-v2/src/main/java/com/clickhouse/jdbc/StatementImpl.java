package com.clickhouse.jdbc;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.jdbc.internal.JdbcUtils;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class StatementImpl implements Statement, JdbcV2Wrapper {
    private static final Logger LOG = LoggerFactory.getLogger(StatementImpl.class);

    ConnectionImpl connection;
    private int queryTimeout;
    protected boolean closed;
    protected ResultSetImpl currentResultSet;
    private OperationMetrics metrics;
    protected List<String> batch;
    private String lastSql;
    private volatile String lastQueryId;
    private String schema;
    private int maxRows;
    public StatementImpl(ConnectionImpl connection) throws SQLException {
        this.connection = connection;
        this.queryTimeout = 0;
        this.closed = false;
        this.currentResultSet = null;
        this.metrics = null;
        this.batch = new ArrayList<>();
        this.schema = connection.getSchema();// remember DB name
        this.maxRows = 0;
    }

    protected void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed", ExceptionUtils.SQL_STATE_CONNECTION_EXCEPTION);
        }
    }

    protected enum StatementType {
        SELECT, INSERT, DELETE, UPDATE, CREATE, DROP, ALTER, TRUNCATE, USE, SHOW, DESCRIBE, EXPLAIN, SET, KILL, OTHER, INSERT_INTO_SELECT
    }

    protected static StatementType parseStatementType(String sql) {
        if (sql == null) {
            return StatementType.OTHER;
        }

        String trimmedSql = sql.trim();
        if (trimmedSql.isEmpty()) {
            return StatementType.OTHER;
        }

        trimmedSql = BLOCK_COMMENT.matcher(trimmedSql).replaceAll("").trim(); // remove comments
        String[] lines = trimmedSql.split("\n");
        for (String line : lines) {
            String trimmedLine = line.trim();
            //https://clickhouse.com/docs/en/sql-reference/syntax#comments
            if (!trimmedLine.startsWith("--") && !trimmedLine.startsWith("#!") && !trimmedLine.startsWith("#")) {
                String[] tokens = trimmedLine.split("\\s+");
                if (tokens.length == 0) {
                    continue;
                }

                switch (tokens[0].toUpperCase()) {
                    case "SELECT": return StatementType.SELECT;
                    case "WITH": return StatementType.SELECT;
                    case "INSERT":
                        for (String token : tokens) {
                            if (token.equalsIgnoreCase("SELECT")) {
                                return StatementType.INSERT_INTO_SELECT;
                            }
                        }
                        return StatementType.INSERT;
                    case "DELETE": return StatementType.DELETE;
                    case "UPDATE": return StatementType.UPDATE;
                    case "CREATE": return StatementType.CREATE;
                    case "DROP": return StatementType.DROP;
                    case "ALTER": return StatementType.ALTER;
                    case "TRUNCATE": return StatementType.TRUNCATE;
                    case "USE": return StatementType.USE;
                    case "SHOW": return StatementType.SHOW;
                    case "DESCRIBE": return StatementType.DESCRIBE;
                    case "EXPLAIN": return StatementType.EXPLAIN;
                    case "SET": return StatementType.SET;
                    case "KILL": return StatementType.KILL;
                    default: return StatementType.OTHER;
                }
            }
        }

        return StatementType.OTHER;
    }

    protected static String parseTableName(String sql) {
        String[] tokens = sql.trim().split("\\s+");
        if (tokens.length < 3) {
            return null;
        }

        return tokens[2];
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

    protected String getLastSql() {
        return lastSql;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        return executeQuery(sql, new QuerySettings().setDatabase(schema));
    }

    private void closePreviousResultSet() {
        if (currentResultSet != null) {
            LOG.debug("Previous result set is open [resultSet = " + currentResultSet + "]");
            // Closing request blindly assuming that user do not care about it anymore (DDL request for example)
            try {
                currentResultSet.close();
            } catch (Exception e) {
                LOG.error("Failed to close previous result set", e);
            } finally {
                currentResultSet = null;
            }
        }
    }

    public ResultSetImpl executeQuery(String sql, QuerySettings settings) throws SQLException {
        checkClosed();
        // Closing before trying to do next request. Otherwise, deadlock because previous connection will not be
        // release before this one completes.
        closePreviousResultSet();

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
            lastSql = parseJdbcEscapeSyntax(sql);
            QueryResponse response;
            if (queryTimeout == 0) {
                response = connection.client.query(lastSql, mergedSettings).get();
            } else {
                response = connection.client.query(lastSql, mergedSettings).get(queryTimeout, TimeUnit.SECONDS);
            }

            if (response.getFormat().isText()) {
                throw new SQLException("Only RowBinaryWithNameAndTypes is supported for output format. Please check your query.",
                        ExceptionUtils.SQL_STATE_CLIENT_ERROR);
            }
            ClickHouseBinaryFormatReader reader = connection.client.newBinaryFormatReader(response);

            currentResultSet = new ResultSetImpl(this, response, reader);
            metrics = response.getMetrics();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }

        return currentResultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        return executeUpdate(sql, new QuerySettings().setDatabase(schema));
    }

    public int executeUpdate(String sql, QuerySettings settings) throws SQLException {
        // TODO: close current result set?
        checkClosed();
        StatementType type = parseStatementType(sql);
        if (type == StatementType.SELECT || type == StatementType.SHOW || type == StatementType.DESCRIBE || type == StatementType.EXPLAIN) {
            throw new SQLException("executeUpdate() cannot be called with a SELECT/SHOW/DESCRIBE/EXPLAIN statement", ExceptionUtils.SQL_STATE_SQL_ERROR);
        }

        // Closing before trying to do next request. Otherwise, deadlock because previous connection will not be
        // release before this one completes.
        closePreviousResultSet();

        QuerySettings mergedSettings = QuerySettings.merge(connection.getDefaultQuerySettings(), settings);

        if (mergedSettings.getQueryId() != null) {
            lastQueryId = mergedSettings.getQueryId();
        } else {
            lastQueryId = UUID.randomUUID().toString();
            mergedSettings.setQueryId(lastQueryId);
        }

        lastSql = parseJdbcEscapeSyntax(sql);
        int updateCount = 0;
        try (QueryResponse response = queryTimeout == 0 ? connection.client.query(lastSql, mergedSettings).get()
                : connection.client.query(lastSql, mergedSettings).get(queryTimeout, TimeUnit.SECONDS)) {
            currentResultSet = null;
            updateCount = Math.max(0, (int) response.getWrittenRows()); // when statement alters schema no result rows returned.
            metrics = response.getMetrics();
            lastQueryId = response.getQueryId();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }

        return updateCount;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
        if (currentResultSet != null) {
            try {
                currentResultSet.close();
            } catch (Exception e) {
                LOG.debug("Failed to close current result set", e);
            } finally {
                currentResultSet = null;
            }
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkClosed();
        if (!connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Set max field size is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkClosed();
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        checkClosed();
        maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
        //TODO: Should we support this?
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkClosed();
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkClosed();
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
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        checkClosed();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        return execute(sql, new QuerySettings().setDatabase(schema));
    }

    public boolean execute(String sql, QuerySettings settings) throws SQLException {
        checkClosed();
        StatementType type = parseStatementType(sql);

        if (type == StatementType.SELECT || type == StatementType.SHOW || type == StatementType.DESCRIBE || type == StatementType.EXPLAIN) {
            executeQuery(sql, settings); // keep open to allow getResultSet()
            return true;
        } else if(type == StatementType.SET) {
            executeUpdate(sql, settings);
            //SET ROLE
            List<String> tokens = JdbcUtils.tokenizeSQL(sql);
            if (JdbcUtils.containsIgnoresCase(tokens, "ROLE")) {
                List<String> roles = new ArrayList<>();
                int roleIndex = JdbcUtils.indexOfIgnoresCase(tokens, "ROLE");
                if (roleIndex == 1) {
                    for (int i = 2; i < tokens.size(); i++) {
                        String token = tokens.get(i);
                        String[] roleTokens = token.split(",");
                        for (String roleToken : roleTokens) {
                            roles.add(roleToken.replace("\"", ""));//Remove double quotes
                        }
                    }

                    if (JdbcUtils.containsIgnoresCase(roles, "NONE")) {
                        connection.client.setDBRoles(Collections.emptyList());
                    } else {
                        connection.client.setDBRoles(roles);
                    }
                }
            }
            return false;
        } else if (type == StatementType.USE) {
            executeUpdate(sql, settings);
            //USE Database
            List<String> tokens = JdbcUtils.tokenizeSQL(sql);
            this.schema = tokens.get(1).replace("\"", "");
            LOG.info("Changed statement schema " + schema);
            return false;
        } else {
            executeUpdate(sql, settings);
            return false;
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();

        ResultSet resultSet = currentResultSet;
        currentResultSet = null;
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        if (currentResultSet == null && metrics != null) {
            int updateCount = (int) metrics.getMetric(ServerMetrics.NUM_ROWS_WRITTEN).getLong();
            metrics = null;// clear metrics
            return updateCount;
        }

        return -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        if (!connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Set fetch direction is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        batch.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        batch.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        List<Integer> results = new ArrayList<>();
        for (String sql : batch) {
            results.add(executeUpdate(sql));
        }
        return results.stream().mapToInt(i -> i).toArray();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
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
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkClosed();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkClosed();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        checkClosed();
        return Statement.super.getLargeUpdateCount();
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        checkClosed();
        Statement.super.setLargeMaxRows(max);
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        checkClosed();
        return Statement.super.getLargeMaxRows();
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        checkClosed();
        return Statement.super.executeLargeBatch();
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        checkClosed();
        return Statement.super.executeLargeUpdate(sql);
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        return Statement.super.executeLargeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        return Statement.super.executeLargeUpdate(sql, columnIndexes);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        return Statement.super.executeLargeUpdate(sql, columnNames);
    }

    @Override
    public String enquoteLiteral(String val) throws SQLException {
        checkClosed();
        return Statement.super.enquoteLiteral(val);
    }

    @Override
    public String enquoteIdentifier(String identifier, boolean alwaysQuote) throws SQLException {
        checkClosed();
        return Statement.super.enquoteIdentifier(identifier, alwaysQuote);
    }

    @Override
    public boolean isSimpleIdentifier(String identifier) throws SQLException {
        checkClosed();
        return Statement.super.isSimpleIdentifier(identifier);
    }

    @Override
    public String enquoteNCharLiteral(String val) throws SQLException {
        checkClosed();
        return Statement.super.enquoteNCharLiteral(val);
    }

    /**
     * Return query ID of last executed statement. It is not guaranteed when statements is used concurrently.
     * @return query ID
     */
    public String getLastQueryId() {
        return lastQueryId;
    }

    private static final Pattern BLOCK_COMMENT = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);
}
