package com.clickhouse.jdbc.internal;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseExternalTable;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;
import com.clickhouse.jdbc.ClickHousePreparedStatement;
import com.clickhouse.jdbc.SqlExceptionUtils;
import com.clickhouse.jdbc.parser.ClickHouseSqlStatement;

@Deprecated
public class TableBasedPreparedStatement extends AbstractPreparedStatement implements ClickHousePreparedStatement {
    private static final Logger log = LoggerFactory.getLogger(TableBasedPreparedStatement.class);

    private static final String ERROR_SET_TABLE = "Please use setObject(ClickHouseExternalTable) method instead";

    private final ClickHouseSqlStatement parsedStmt;
    private final List<String> tables;
    private final ClickHouseExternalTable[] values;
    private final ClickHouseParameterMetaData paramMetaData;

    private final List<List<ClickHouseExternalTable>> batch;

    protected TableBasedPreparedStatement(ClickHouseConnectionImpl connection, ClickHouseRequest<?> request,
            ClickHouseSqlStatement parsedStmt, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        super(connection, request, resultSetType, resultSetConcurrency, resultSetHoldability);

        Set<String> set = parsedStmt != null ? parsedStmt.getTempTables() : null;
        if (set == null) {
            throw SqlExceptionUtils.clientError("Non-null table list is required");
        }

        this.parsedStmt = parsedStmt;
        int size = set.size();
        this.tables = new ArrayList<>(size);
        this.tables.addAll(set);
        values = new ClickHouseExternalTable[size];
        List<ClickHouseColumn> list = new ArrayList<>(size);
        for (String name : set) {
            list.add(ClickHouseColumn.of(name, ClickHouseDataType.JSON, false));
        }
        paramMetaData = new ClickHouseParameterMetaData(Collections.unmodifiableList(list), mapper,
                connection.getTypeMap());
        batch = new LinkedList<>();
    }

    protected void ensureParams() throws SQLException {
        List<String> list = new ArrayList<>();
        for (int i = 0, len = values.length; i < len; i++) {
            if (values[i] == null) {
                list.add(tables.get(i));
            }
        }

        if (!list.isEmpty()) {
            throw SqlExceptionUtils.clientError(ClickHouseUtils.format("Missing table(s): %s", list));
        }
    }

    @Override
    public long[] executeAny(boolean asBatch) throws SQLException {
        ensureOpen();
        boolean continueOnError = false;
        if (asBatch) {
            if (batch.isEmpty()) {
                return ClickHouseValues.EMPTY_LONG_ARRAY;
            }
            continueOnError = getConnection().getJdbcConfig().isContinueBatchOnError();
        } else {
            if (!batch.isEmpty()) {
                throw SqlExceptionUtils.undeterminedExecutionError();
            }
            addBatch();
        }

        long[] results = new long[batch.size()];
        int index = 0;
        try {
            String sql = getSql();
            for (List<ClickHouseExternalTable> list : batch) {
                try (ClickHouseResponse r = executeStatement(sql, null, list, null);
                        ResultSet rs = updateResult(parsedStmt, r)) {
                    if (asBatch && rs != null && parsedStmt.isQuery()) {
                        throw SqlExceptionUtils.queryInBatchError(results);
                    }
                    long rows = getLargeUpdateCount();
                    results[index] = rows > 0L ? rows : 0L;
                } catch (Exception e) {
                    if (!asBatch) {
                        throw SqlExceptionUtils.handle(e);
                    }

                    results[index] = EXECUTE_FAILED;
                    if (!continueOnError) {
                        throw SqlExceptionUtils.batchUpdateError(e, results);
                    }
                    log.error("Failed to execute batch insert at %d of %d", index + 1, batch.size(), e);
                }
                index++;
            }
        } finally {
            clearBatch();
        }

        return results;
    }

    @Override
    protected int getMaxParameterIndex() {
        return values.length;
    }

    protected String getSql() {
        // why? because request can be modified so it might not always same as
        // parsedStmt.getSQL()
        return getRequest().getStatements(false).get(0);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        ensureParams();
        if (!batch.isEmpty()) {
            throw SqlExceptionUtils.undeterminedExecutionError();
        }

        ClickHouseSqlStatement stmt = new ClickHouseSqlStatement(getSql());
        return updateResult(parsedStmt, executeStatement(stmt, null, Arrays.asList(values), null));
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        ensureParams();
        if (!batch.isEmpty()) {
            throw SqlExceptionUtils.undeterminedExecutionError();
        }

        try (ClickHouseResponse r = executeStatement(getSql(), null, Arrays.asList(values), null)) {
            updateResult(parsedStmt, r);
            return getLargeUpdateCount();
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void clearParameters() throws SQLException {
        ensureOpen();

        for (int i = 0, len = values.length; i < len; i++) {
            values[i] = null;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        ensureOpen();

        if (x instanceof ClickHouseExternalTable) {
            int idx = toArrayIndex(parameterIndex);
            values[idx] = (ClickHouseExternalTable) x;
        } else {
            throw SqlExceptionUtils.clientError("Only ClickHouseExternalTable is allowed");
        }
    }

    @Override
    public boolean execute() throws SQLException {
        ensureParams();
        if (!batch.isEmpty()) {
            throw SqlExceptionUtils.undeterminedExecutionError();
        }

        ClickHouseSqlStatement stmt = new ClickHouseSqlStatement(getSql());
        return updateResult(parsedStmt, executeStatement(stmt, null, Arrays.asList(values), null)) != null;
    }

    @Override
    public void addBatch() throws SQLException {
        ensureOpen();

        ensureParams();
        batch.add(Collections.unmodifiableList(Arrays.asList(values)));
        clearParameters();
    }

    @Override
    public void clearBatch() throws SQLException {
        ensureOpen();

        this.batch.clear();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
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
