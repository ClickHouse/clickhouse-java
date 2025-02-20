package com.clickhouse.jdbc.internal;

import java.io.File;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.data.ClickHouseWriter;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;
import com.clickhouse.jdbc.ClickHousePreparedStatement;
import com.clickhouse.jdbc.SqlExceptionUtils;
import com.clickhouse.jdbc.parser.ClickHouseSqlStatement;

@Deprecated
public class StreamBasedPreparedStatement extends AbstractPreparedStatement implements ClickHousePreparedStatement {
    private static final Logger log = LoggerFactory.getLogger(StreamBasedPreparedStatement.class);

    private static final String ERROR_SET_PARAM = "Please use setString()/setBytes()/setInputStream() or pass String/InputStream/ClickHouseInputStream to setObject() method instead";
    private static final String DEFAULT_KEY = "pipe";
    private static final List<ClickHouseColumn> DEFAULT_PARAMS = Collections
            .singletonList(ClickHouseColumn.of("data", ClickHouseDataType.String, false));

    private final ClickHouseSqlStatement parsedStmt;
    private final ClickHouseParameterMetaData paramMetaData;

    private final List<ClickHouseInputStream> batch;

    private ClickHouseInputStream value;

    protected StreamBasedPreparedStatement(ClickHouseConnectionImpl connection, ClickHouseRequest<?> request,
            ClickHouseSqlStatement parsedStmt, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        super(connection, request, resultSetType, resultSetConcurrency, resultSetHoldability);

        this.parsedStmt = parsedStmt;
        this.value = null;
        paramMetaData = new ClickHouseParameterMetaData(DEFAULT_PARAMS, mapper, connection.getTypeMap());
        batch = new LinkedList<>();
    }

    protected void ensureParams() throws SQLException {
        if (value == null) {
            throw SqlExceptionUtils.clientError("Missing input stream");
        }
    }

    @Override
    protected long[] executeAny(boolean asBatch) throws SQLException {
        ensureOpen();
        boolean continueOnError = false;
        if (asBatch) {
            if (batch.isEmpty()) {
                return ClickHouseValues.EMPTY_LONG_ARRAY;
            }
            continueOnError = getConnection().getJdbcConfig().isContinueBatchOnError();
        } else {
            try {
                if (!batch.isEmpty()) {
                    throw SqlExceptionUtils.undeterminedExecutionError();
                }
                addBatch();
            } catch (SQLException e) {
                clearBatch();
                throw e;
            }
        }

        long[] results = new long[batch.size()];
        int count = 0;
        String sql = getRequest().getStatements(false).get(0);
        try {
            for (ClickHouseInputStream in : batch) {
                @SuppressWarnings("unchecked")
                final CompletableFuture<Boolean> future = (CompletableFuture<Boolean>) in.removeUserData(DEFAULT_KEY);
                results[count++] = executeInsert(sql, in);
                if (future != null) {
                    future.get();
                }
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            if (!asBatch) {
                throw SqlExceptionUtils.handle(e);
            }

            if (!continueOnError) {
                throw SqlExceptionUtils.batchUpdateError(e, results);
            }
            log.error("Failed to execute batch insert of %d records", count + 1, e);
        } finally {
            clearBatch();
        }

        return results;
    }

    @Override
    protected int getMaxParameterIndex() {
        return 1;
    }

    protected String getSql() {
        // why? because request can be modified so it might not always same as
        // parsedStmt.getSQL()
        return getRequest().getStatements(false).get(0);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        ensureParams();
        try {
            executeAny(false);
        } catch (SQLException e) {
            if (e.getSQLState() != null) {
                throw e;
            } else {
                throw new SQLException("Query failed", SqlExceptionUtils.SQL_STATE_SQL_ERROR, e.getCause());
            }
        }

        ResultSet rs = getResultSet();
        if (rs != null) { // should not happen
            try {
                rs.close();
            } catch (Exception e) {
                // ignore
            }
        }
        return newEmptyResultSet();
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        ensureParams();
        try {
            executeAny(false);
        } catch (SQLException e) {
            if (e.getSQLState() != null) {
                throw e;
            } else {
                throw new SQLException("Update failed", SqlExceptionUtils.SQL_STATE_SQL_ERROR, e.getCause());
            }
        }
        long row = getLargeUpdateCount();
        return row > 0L ? row : 0L;
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_PARAM);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_PARAM);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_PARAM);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_PARAM);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_PARAM);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_PARAM);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_PARAM);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        ensureOpen();

        value = ClickHouseInputStream.of(x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        ensureOpen();

        value = ClickHouseInputStream.of(x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        ensureOpen();

        value = ClickHouseInputStream.of(x);
    }

    @Override
    public void clearParameters() throws SQLException {
        ensureOpen();

        value = null;
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        ensureOpen();

        if (x instanceof ClickHousePassThruStream) {
            ClickHousePassThruStream stream = (ClickHousePassThruStream) x;
            if (!stream.hasInput()) {
                throw SqlExceptionUtils.clientError("No input available in the given pass-thru stream");
            }
            value = stream.newInputStream(getConfig().getWriteBufferSize(), null);
        } else if (x instanceof ClickHouseWriter) {
            final ClickHouseWriter writer = (ClickHouseWriter) x;
            final ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance() // NOSONAR
                    .createPipedOutputStream(getConfig());
            value = stream.getInputStream();

            // always run in async mode or it will not work
            value.setUserData(DEFAULT_KEY, ClickHouseClient.submit(() -> {
                try (ClickHouseOutputStream out = stream) {
                    writer.write(out);
                }
                return true;
            }));
        } else if (x instanceof InputStream) {
            value = ClickHouseInputStream.of((InputStream) x);
        } else if (x instanceof String) {
            value = ClickHouseInputStream.of((String) x);
        } else if (x instanceof byte[]) {
            value = ClickHouseInputStream.of((byte[]) x);
        } else if (x instanceof File) {
            value = ClickHouseInputStream.of((File) x);
        } else {
            throw SqlExceptionUtils
                    .clientError(
                            "Only byte[], String, File, InputStream, ClickHousePassThruStream, and ClickHouseWriter are supported");
        }
    }

    @Override
    public boolean execute() throws SQLException {
        ensureParams();
        if (!batch.isEmpty()) {
            throw SqlExceptionUtils.undeterminedExecutionError();
        }

        final String sql = getSql();
        @SuppressWarnings("unchecked")
        final CompletableFuture<Boolean> future = (CompletableFuture<Boolean>) value.removeUserData(DEFAULT_KEY);
        executeInsert(sql, value);
        if (future != null) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Execution of query was interrupted: %s", sql);
            } catch (ExecutionException e) {
                throw SqlExceptionUtils.handle(e.getCause());
            }
        }
        return false;
    }

    @Override
    public void addBatch() throws SQLException {
        ensureOpen();

        ensureParams();
        batch.add(value);
        clearParameters();
    }

    @Override
    public void clearBatch() throws SQLException {
        ensureOpen();

        this.batch.clear();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_PARAM);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_PARAM);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_PARAM);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_PARAM);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        ensureOpen();

        value = ClickHouseInputStream.empty();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return paramMetaData;
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }
}
