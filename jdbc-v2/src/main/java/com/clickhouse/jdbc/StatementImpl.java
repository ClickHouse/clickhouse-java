package com.clickhouse.jdbc;

import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.api.query.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class StatementImpl implements Statement, JdbcV2Wrapper {
    private static final Logger LOG = LoggerFactory.getLogger(StatementImpl.class);

    ConnectionImpl connection;
    private int queryTimeout;
    protected boolean closed;
    private ResultSetImpl currentResultSet;
    private OperationMetrics metrics;
    private List<String> batch;

    public StatementImpl(ConnectionImpl connection) {
        this.connection = connection;
        this.queryTimeout = 0;
        this.closed = false;
        this.currentResultSet = null;
        this.metrics = null;
        this.batch = new ArrayList<>();
    }

    protected void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
    }

    protected enum StatementType {
        SELECT, INSERT, DELETE, UPDATE, CREATE, DROP, ALTER, TRUNCATE, USE, SHOW, DESCRIBE, EXPLAIN, SET, KILL, OTHER
    }

    protected StatementType parseStatementType(String sql) {
        String[] tokens = sql.trim().split("\\s+");
        if (tokens.length == 0) {
            return StatementType.OTHER;
        }

        switch (tokens[0].toUpperCase()) {
            case "SELECT": return StatementType.SELECT;
            case "INSERT": return StatementType.INSERT;
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

    protected String parseTableName(String sql) {
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

        // Add more replacements as needed for other JDBC escape sequences
        LOG.trace("Parsed SQL: {}", sql);
        return sql;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();

        try {
            sql = parseJdbcEscapeSyntax(sql);
            QueryResponse response = connection.client.query(sql).get(queryTimeout, TimeUnit.SECONDS);
            ClickHouseBinaryFormatReader reader = connection.client.newBinaryFormatReader(response);
            currentResultSet = new ResultSetImpl(response, reader);
            metrics = response.getMetrics();
        } catch (Exception e) {
            throw new SQLException(e);
        }

        return currentResultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        
        if (parseStatementType(sql) == StatementType.SELECT) {
            throw new SQLException("executeUpdate() cannot be called with a SELECT statement");
        }

        try {
            sql = parseJdbcEscapeSyntax(sql);
            QueryResponse response = connection.client.query(sql).get(queryTimeout, TimeUnit.SECONDS);
            currentResultSet = null;
            metrics = response.getMetrics();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return (int) metrics.getMetric(ServerMetrics.NUM_ROWS_WRITTEN).getLong();
    }

    @Override
    public void close() throws SQLException {
        closed = true;
        connection.close();
        if (currentResultSet != null) {
            currentResultSet.close();
            currentResultSet = null;
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
        throw new SQLFeatureNotSupportedException("Set max field size is not supported.");
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

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
        checkClosed();
        throw new UnsupportedOperationException("Cancel is not supported.");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        List<String> statements = List.of(sql.split(";"));
        boolean firstIsResult = false;

        int index = 0;
        for (String statement : statements) {
            StatementType type = parseStatementType(statement);

            if (type == StatementType.SELECT) {
                executeQuery(statement);
                if (index == 0) {
                    firstIsResult = true;
                }
            } else {
                executeUpdate(statement);
            }

            index++;
        }

        return firstIsResult;
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
        if (currentResultSet == null) {
            return (int) metrics.getMetric(ServerMetrics.NUM_ROWS_WRITTEN).getLong();
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
        throw new UnsupportedOperationException("Fetch direction is not supported.");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
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

        for(String sql : batch) {
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

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

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
}
