package com.clickhouse.jdbc;

import com.clickhouse.client.api.DataTypeUtils;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatWriter;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import com.clickhouse.jdbc.internal.ParsedPreparedStatement;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements data streaming through Client Writer API.
 * See {@link PreparedStatementImpl}
 *
 */
public class WriterStatementImpl extends PreparedStatementImpl implements PreparedStatement {

    private static final Logger LOG = LoggerFactory.getLogger(WriterStatementImpl.class);

    private static final int STREAM_BUFFER_SIZE = 8192;

    private ByteArrayOutputStream out;
    private ClickHouseBinaryFormatWriter writer;
    private final TableSchema tableSchema;

    public WriterStatementImpl(ConnectionImpl connection, String originalSql, TableSchema tableSchema,
                               ParsedPreparedStatement parsedStatement)
            throws SQLException {
        super(connection, originalSql, parsedStatement);

        if (parsedStatement.getInsertColumns() != null) {
            List<ClickHouseColumn> insertColumns = new ArrayList<>();
            for (String column : parsedStatement.getInsertColumns()) {
                insertColumns.add(tableSchema.getColumnByName(column));
            }
            this.tableSchema = new TableSchema(tableSchema.getTableName(), tableSchema.getQuery(),
                    tableSchema.getDatabaseName(), insertColumns);
        } else {
            this.tableSchema = tableSchema;
        }
        try {
            resetWriter();
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    private void resetWriter() throws IOException {
        if (out != null) {
            out.close();
        }

        out = new ByteArrayOutputStream();
        writer = new RowBinaryFormatWriter(out, tableSchema, tableSchema.hasDefaults() ?
                ClickHouseFormat.RowBinaryWithDefaults : ClickHouseFormat.RowBinary);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        ensureOpen();
        throw new UnsupportedOperationException("bug. This PreparedStatement implementation should not be used with queries");
    }

    @Override
    public int executeUpdate() throws SQLException {
        return (int) this.executeLargeUpdate();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false; // no result sets
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return null; // no result set
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        ensureOpen();

        // commit whatever changes
        try {
            writer.commitRow();
        } catch (Exception e) {
            handleSocketTimeoutException(e);
            throw new SQLException(e);
        }

        int updateCount = 0;
        InputStream in = new ByteArrayInputStream(out.toByteArray());
        InsertSettings settings = new InsertSettings();
        settings.setQueryId(setLastQueryID(settings.getQueryId()));

        try (InsertResponse response = queryTimeout == 0 ?
                connection.getClient().insert(tableSchema.getTableName(),in, writer.getFormat(), settings).get()
                : connection.getClient().insert(tableSchema.getTableName(),in, writer.getFormat(), settings).get(queryTimeout, TimeUnit.SECONDS)) {
            updateCount = Math.max(0, (int) response.getWrittenRows()); // when statement alters schema no result rows returned.
            lastQueryId = response.getQueryId();
        } catch (Exception e) {
            handleSocketTimeoutException(e);
            throw ExceptionUtils.toSqlState(e);
        } finally {
            try {
                resetWriter();
            } catch (Exception e) {
                LOG.debug("Failed to reset writer after insert", e);
            }
        }
        return updateCount;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        ensureOpen();
        writer.setValue(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        ensureOpen();
        writer.setValue(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        ensureOpen();
        writer.setByte(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        ensureOpen();
        writer.setShort(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        ensureOpen();
        writer.setInteger(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        ensureOpen();
        writer.setLong(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        ensureOpen();
        writer.setFloat(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        ensureOpen();
        writer.setDouble(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        ensureOpen();
        writer.setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        ensureOpen();
        writer.setString(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        ensureOpen();
        writer.setValue(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setDate(parameterIndex, x, defaultCalendar);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setTime(parameterIndex, x, defaultCalendar);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setTimestamp(parameterIndex, x, defaultCalendar);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setAsciiStream(parameterIndex, x, (long) length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        ensureOpen();
        writer.setValue(parameterIndex, readBytes(parameterIndex, x, null));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        ensureOpen();
        writer.setValue(parameterIndex, readBytes(parameterIndex, x, length));
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        ensureOpen();
        writer.setValue(parameterIndex, readBytes(parameterIndex, x, (long) length));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBinaryStream(parameterIndex, x, (long) length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        ensureOpen();
        writer.setValue(parameterIndex, readBytes(parameterIndex, x, null));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        ensureOpen();
        writer.setValue(parameterIndex, readBytes(parameterIndex, x, length));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x, int length) throws SQLException {
        setCharacterStream(parameterIndex, x, (long) length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        ensureOpen();
        writer.setString(parameterIndex, readChars(parameterIndex, x, length));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x) throws SQLException {
        ensureOpen();
        writer.setString(parameterIndex, readChars(parameterIndex, x, null));
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader x) throws SQLException {
        setCharacterStream(parameterIndex, x);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        setCharacterStream(parameterIndex, x, length);
    }

    /**
     * Reads the bytes of a stream parameter. The writer keeps only the value, so the stream is read when
     * the parameter is set.
     *
     * @param parameterIndex index of the parameter, for the error message
     * @param x              stream, or {@code null}
     * @param length         number of bytes to read, or {@code null} to read to the end of the stream
     * @return the bytes, or {@code null} when {@code x} is {@code null}
     * @throws SQLException when the length is negative, the stream ends before {@code length} bytes,
     *                      or the stream cannot be read
     */
    private static byte[] readBytes(int parameterIndex, InputStream x, Long length) throws SQLException {
        if (x == null) {
            return null;
        }
        checkStreamLength(parameterIndex, length);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[STREAM_BUFFER_SIZE];
        long remaining = length == null ? Long.MAX_VALUE : length;
        try {
            int read;
            while (remaining > 0 && (read = x.read(buffer, 0, (int) Math.min(buffer.length, remaining))) != -1) {
                bytes.write(buffer, 0, read);
                remaining -= read;
            }
        } catch (IOException e) {
            throw new SQLException("Failed to read the stream of parameter " + parameterIndex,
                    ExceptionUtils.SQL_STATE_SQL_ERROR, e);
        }
        checkStreamEnd(parameterIndex, length, remaining, "bytes");
        return bytes.toByteArray();
    }

    /**
     * Reads the characters of a character stream parameter. The writer keeps only the value, so the
     * stream is read when the parameter is set.
     *
     * @param parameterIndex index of the parameter, for the error message
     * @param x              reader, or {@code null}
     * @param length         number of characters to read, or {@code null} to read to the end of the stream
     * @return the characters, or {@code null} when {@code x} is {@code null}
     * @throws SQLException when the length is negative, the stream ends before {@code length} characters,
     *                      or the stream cannot be read
     */
    private static String readChars(int parameterIndex, Reader x, Long length) throws SQLException {
        if (x == null) {
            return null;
        }
        checkStreamLength(parameterIndex, length);
        StringBuilder chars = new StringBuilder();
        char[] buffer = new char[STREAM_BUFFER_SIZE];
        long remaining = length == null ? Long.MAX_VALUE : length;
        try {
            int read;
            while (remaining > 0 && (read = x.read(buffer, 0, (int) Math.min(buffer.length, remaining))) != -1) {
                chars.append(buffer, 0, read);
                remaining -= read;
            }
        } catch (IOException e) {
            throw new SQLException("Failed to read the stream of parameter " + parameterIndex,
                    ExceptionUtils.SQL_STATE_SQL_ERROR, e);
        }
        checkStreamEnd(parameterIndex, length, remaining, "characters");
        return chars.toString();
    }

    private static void checkStreamLength(int parameterIndex, Long length) throws SQLException {
        if (length != null && length < 0) {
            throw new SQLException("Stream length cannot be negative: " + length + " (parameter " + parameterIndex
                    + ")", ExceptionUtils.SQL_STATE_CLIENT_ERROR);
        }
    }

    private static void checkStreamEnd(int parameterIndex, Long length, long remaining, String unit)
            throws SQLException {
        if (length != null && remaining > 0) {
            throw new SQLException("The stream of parameter " + parameterIndex + " ended after "
                    + (length - remaining) + " of " + length + " " + unit, ExceptionUtils.SQL_STATE_SQL_ERROR);
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        ensureOpen();
        writer.clearRow();
    }

    @Override
    public boolean execute() throws SQLException {
        executeLargeUpdate();
        return false; // no result set
    }

    @Override
    public void addBatch() throws SQLException {
        ensureOpen();
        try {
            writer.commitRow();
        } catch (Exception e) {
            handleSocketTimeoutException(e);
            throw new SQLException(e);
        }
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        ensureOpen();
        setClob(parameterIndex, x.getCharacterStream());
    }

    @Override
    public void setClob(int parameterIndex, Reader x) throws SQLException {
        ensureOpen();
        setClob(parameterIndex, x, -1);
    }

    @Override
    public void setClob(int parameterIndex, Reader x, long length) throws SQLException {
        ensureOpen();
        writer.setReader(parameterIndex, x, length);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        ensureOpen();
        setBlob(parameterIndex, x.getBinaryStream(), x.length());
    }

    @Override
    public void setBlob(int parameterIndex, InputStream x, long length) throws SQLException {
        ensureOpen();
        writer.setInputStream(parameterIndex, x, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        ensureOpen();
        writer.setInputStream(parameterIndex, inputStream, -1);
    }

    @Override
    public void setNClob(int parameterIndex, Reader x, long length) throws SQLException {
        ensureOpen();
        writer.setReader(parameterIndex, x, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob x) throws SQLException {
        ensureOpen();
        setNClob(parameterIndex, x.getCharacterStream(), x.length());
    }

    @Override
    public void setNClob(int parameterIndex, Reader x) throws SQLException {
        ensureOpen();
        setNClob(parameterIndex, x, -1);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML x) throws SQLException {
        ensureOpen();
        writer.setReader(parameterIndex, x.getCharacterStream(), -1);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        ensureOpen();
        writer.setValue(parameterIndex, x.getArray());
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        ensureOpen();
        writer.setValue(parameterIndex, DataTypeUtils.toLocalDate(x, cal.getTimeZone()));
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        ensureOpen();
        writer.setValue(parameterIndex, DataTypeUtils.toLocalTime(x, cal.getTimeZone()));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        ensureOpen();
        writer.setDateTime(parameterIndex, DataTypeUtils.toLocalDateTime(x, cal.getTimeZone()));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        ensureOpen();
        writer.setValue(parameterIndex, null);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        ensureOpen();
        writer.setString(parameterIndex, x == null ? null : x.toString());
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        ensureOpen();
        throw new SQLException("ROWID is not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        ensureOpen();
        writer.setString(parameterIndex, value);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        ensureOpen();
        throw new SQLException("This form of setObject is not supported yet", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        ensureOpen();
        throw new SQLException("This form of setObject is not supported yet", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        ensureOpen();
        writer.setValue(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        ensureOpen();
        throw new SQLException("This form of setObject is not supported yet", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        ensureOpen();
        throw new SQLException("This form of setObject is not supported yet", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void close() throws SQLException {
        super.close();
        try {
            if (out != null) {
                out.close();
                out = null;
            }
            writer = null;
        } catch (Exception e) {
            LOG.debug("Failed to close writer output stream", e);
        }
    }

    @Override
    public void cancel() throws SQLException {
        try {
            resetWriter();
        } catch (Exception e ) {
            throw new SQLException(e);
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        ensureOpen();
        int batchSize = writer.getRowCount();
        long rowsInserted = executeLargeUpdate();
        int[] results = new int[batchSize];
        Arrays.fill(results, batchSize == rowsInserted? 1 : PreparedStatement.SUCCESS_NO_INFO);
        return results;
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        ensureOpen();
        int batchSize = writer.getRowCount();
        long rowsInserted = executeLargeUpdate();
        long[] results = new long[batchSize];
        Arrays.fill(results, batchSize == rowsInserted? 1 : PreparedStatement.SUCCESS_NO_INFO);
        return results;
    }
}
