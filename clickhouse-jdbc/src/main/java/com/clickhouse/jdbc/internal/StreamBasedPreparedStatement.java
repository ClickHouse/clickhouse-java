package com.clickhouse.jdbc.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;
import com.clickhouse.client.data.BinaryStreamUtils;
import com.clickhouse.client.data.ClickHousePipedStream;
import com.clickhouse.client.data.ClickHouseRowBinaryProcessor;
import com.clickhouse.client.data.ClickHouseRowBinaryProcessor.MappedFunctions;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.SqlExceptionUtils;

public class StreamBasedPreparedStatement extends ClickHouseStatementImpl implements PreparedStatement {
    private static final Logger log = LoggerFactory.getLogger(StreamBasedPreparedStatement.class);

    private final Calendar defaultCalendar;
    private final ZoneId jvmZoneId;

    private final List<ClickHouseColumn> columns;
    private final ClickHouseValue[] values;
    private final boolean[] flags;

    private ClickHousePipedStream stream;
    private final List<InputStream> batch;

    protected StreamBasedPreparedStatement(ClickHouseConnection connection, ClickHouseRequest<?> request,
            List<ClickHouseColumn> columns, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        super(connection, request, resultSetType, resultSetConcurrency, resultSetHoldability);

        defaultCalendar = connection.getDefaultCalendar();
        jvmZoneId = connection.getJvmTimeZone().toZoneId();

        this.columns = columns;
        int size = columns.size();
        int i = 0;
        values = new ClickHouseValue[size];
        for (ClickHouseColumn col : columns) {
            values[i++] = ClickHouseValues.newValue(col);
        }
        flags = new boolean[size];

        ClickHouseConfig config = request.getConfig();
        stream = new ClickHousePipedStream(config.getMaxBufferSize(), 0, config.getSocketTimeout());
        batch = new LinkedList<>();
    }

    protected void ensureParams() throws SQLException {
        List<String> list = new ArrayList<>();
        for (int i = 0, len = values.length; i < len; i++) {
            if (!flags[i]) {
                list.add(String.valueOf(i + 1));
            }
        }

        if (!list.isEmpty()) {
            throw SqlExceptionUtils.clientError(ClickHouseUtils.format("Missing parameter(s): %s", list));
        }
    }

    protected int toArrayIndex(int parameterIndex) throws SQLException {
        if (parameterIndex < 1 || parameterIndex > values.length) {
            throw SqlExceptionUtils.clientError(ClickHouseUtils
                    .format("Parameter index must between 1 and %d but we got %d", values.length, parameterIndex));
        }

        return parameterIndex - 1;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        ensureParams();

        return null; // return executeQuery(preparedQuery.apply(values));
    }

    @Override
    public int executeUpdate() throws SQLException {
        ensureParams();

        addBatch();
        return executeBatch()[0];
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setNull(parameterIndex, sqlType, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(new String(x, StandardCharsets.UTF_8));
        flags[idx] = true;
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setDate(parameterIndex, x, null);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setTime(parameterIndex, x, null);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setTimestamp(parameterIndex, x, null);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        String s = null;
        if (x != null) {
            try {
                s = BinaryStreamUtils.readFixedString(ClickHouseInputStream.of(x), length, StandardCharsets.US_ASCII);
            } catch (Throwable e) { // IOException and potentially OOM error
                throw SqlExceptionUtils.clientError(e);
            }
        }

        setString(parameterIndex, s);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        String s = null;
        if (x != null) {
            try {
                s = BinaryStreamUtils.readFixedString(ClickHouseInputStream.of(x), length, StandardCharsets.UTF_8);
            } catch (Throwable e) { // IOException and potentially OOM error
                throw SqlExceptionUtils.clientError(e);
            }
        }

        setString(parameterIndex, s);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setUnicodeStream(parameterIndex, x, length);
    }

    @Override
    public void clearParameters() throws SQLException {
        ensureOpen();

        ClickHouseConfig config = getConfig();
        if (stream != null) {
            try {
                stream.close();
            } catch (IOException e) {
                // should not happen
                throw SqlExceptionUtils.handle(e);
            }
        }
        stream = new ClickHousePipedStream(config.getMaxBufferSize(), 0, config.getSocketTimeout());

        for (int i = 0, len = values.length; i < len; i++) {
            values[i].resetToNullOrEmpty();
            flags[i] = false;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x, targetSqlType, 0);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public boolean execute() throws SQLException {
        ensureParams();

        return false; // execute(preparedQuery.apply(values));
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils
                .unsupportedError("addBatch(String) cannot be called in PreparedStatement or CallableStatement!");
    }

    @Override
    public void addBatch() throws SQLException {
        ensureOpen();

        MappedFunctions functions = ClickHouseRowBinaryProcessor.getMappedFunctions();
        for (int i = 0, len = values.length; i < len; i++) {
            if (!flags[i]) {
                throw SqlExceptionUtils.clientError(ClickHouseUtils.format("Missing value for parameter #%d", i + 1));
            }
            try {
                functions.serialize(values[i], getConfig(), columns.get(i), stream);
            } catch (IOException e) {
                // should not happen
                throw SqlExceptionUtils.handle(e);
            }
        }

        batch.add(stream.getInput());
        clearParameters();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        ensureOpen();

        int len = batch.size();
        int[] results = new int[len];
        int counter = 0;
        for (InputStream input : batch) {
            try {
                results[counter] = executeInsert(getRequest().getStatements(false).get(0), input);
            } catch (Exception e) {
                results[counter] = EXECUTE_FAILED;
                log.error("Failed to execute task %d of %d", counter + 1, len, e);
            }
            counter++;
        }

        clearBatch();

        return results;
    }

    @Override
    public void clearBatch() throws SQLException {
        ensureOpen();

        this.batch.clear();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        String s = null;
        if (reader != null) {
            try {
                s = BinaryStreamUtils.readString(reader, length);
            } catch (Throwable e) { // IOException and potentially OOM error
                throw SqlExceptionUtils.clientError(e);
            }
        }

        setString(parameterIndex, s);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        if (x != null) {
            setBinaryStream(parameterIndex, x.getBinaryStream());
        } else {
            setNull(parameterIndex, Types.BINARY);
        }
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ResultSet currentResult = getResultSet();

        return currentResult != null ? currentResult.getMetaData() : null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        if (x == null) {
            values[idx].resetToNullOrEmpty();
            flags[idx] = true;
            return;
        }

        LocalDateTime dt = null;
        if (cal != null) {
            dt = x.toLocalDateTime().atZone(jvmZoneId)
                    .withZoneSameInstant(cal.getTimeZone().toZoneId()).toLocalDateTime();
        } else {
            dt = x.toLocalDateTime();
        }

        values[idx].update(dt);
        flags[idx] = true;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].resetToNullOrEmpty();
        flags[idx] = true;
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setString(parameterIndex, String.valueOf(x));
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        ensureOpen();

        toArrayIndex(parameterIndex);

        throw SqlExceptionUtils.unsupportedError("setRowId not implemented");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        setCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        ensureOpen();

        toArrayIndex(parameterIndex);

        throw SqlExceptionUtils.unsupportedError("setNClob not implemented");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        ensureOpen();

        toArrayIndex(parameterIndex);

        throw SqlExceptionUtils.unsupportedError("setClob not implemented");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        ensureOpen();

        toArrayIndex(parameterIndex);

        throw SqlExceptionUtils.unsupportedError("setBlob not implemented");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setClob(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        ensureOpen();

        toArrayIndex(parameterIndex);

        throw SqlExceptionUtils.unsupportedError("setSQLXML not implemented");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        ensureOpen();

        toArrayIndex(parameterIndex);

        throw SqlExceptionUtils.unsupportedError("setAsciiStream not implemented");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        ensureOpen();

        toArrayIndex(parameterIndex);

        throw SqlExceptionUtils.unsupportedError("setBinaryStream not implemented");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        ensureOpen();

        toArrayIndex(parameterIndex);

        throw SqlExceptionUtils.unsupportedError("setCharacterStream not implemented");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setClob(parameterIndex, reader);
    }
}
