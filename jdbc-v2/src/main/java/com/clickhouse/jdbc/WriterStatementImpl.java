package com.clickhouse.jdbc;

import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatWriter;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import com.clickhouse.jdbc.internal.JdbcUtils;

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
import java.util.Arrays;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

public class WriterStatementImpl extends PreparedStatementImpl implements PreparedStatement {


    private ByteArrayOutputStream out;
    private ClickHouseBinaryFormatWriter writer;
    private final TableSchema tableSchema;

    public WriterStatementImpl(ConnectionImpl connection, String originalSql, StatementType statementType)
            throws SQLException {
        super(connection, originalSql, statementType);

        String[] firstThreeTokens = originalSql.split("\\s+", 4);
        if (firstThreeTokens.length != 4) {
            throw new SQLException("Invalid or unsupported INSERT statement: " + originalSql);
        }

        String tableName = JdbcUtils.unQuoteTableName(firstThreeTokens[2]);
        this.tableSchema = connection.getClient().getTableSchema(tableName, connection.getSchema());
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
        checkClosed();
        throw new UnsupportedOperationException("bug. This PreparedStatement implementation should not be used with queries");
    }

    @Override
    public int executeUpdate() throws SQLException {
        return (int) this.executeLargeUpdate();
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        checkClosed();
        int updateCount = 0;
        InputStream in = new ByteArrayInputStream(out.toByteArray());
        InsertSettings settings = new InsertSettings();
        try (InsertResponse response = queryTimeout == 0 ?
                connection.client.insert(tableSchema.getTableName(),in, writer.getFormat(), settings).get()
                : connection.client.insert(tableSchema.getTableName(),in, writer.getFormat(), settings).get(queryTimeout, TimeUnit.SECONDS)) {
            currentResultSet = null;
            updateCount = Math.max(0, (int) response.getWrittenRows()); // when statement alters schema no result rows returned.
            metrics = response.getMetrics();
            lastQueryId = response.getQueryId();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        } finally {
            try {
                resetWriter();
            } catch (Exception e) {
                // ignore
            }
        }
        return updateCount;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkClosed();
        writer.setValue(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkClosed();
        writer.setValue(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkClosed();
        writer.setByte(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkClosed();
        writer.setShort(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkClosed();
        writer.setInteger(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkClosed();
        writer.setLong(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkClosed();
        writer.setFloat(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkClosed();
        writer.setDouble(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkClosed();
        writer.setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkClosed();
        writer.setString(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkClosed();

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
        checkClosed();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        checkClosed();

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        checkClosed();

    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkClosed();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkClosed();
    }


    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        checkClosed();

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        checkClosed();

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x, int length) throws SQLException {
        checkClosed();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        checkClosed();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x) throws SQLException {
        checkClosed();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader x) throws SQLException {
        checkClosed();

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        checkClosed();

    }

    @Override
    public void clearParameters() throws SQLException {
        checkClosed();
        writer.clearRow();
    }

    @Override
    public boolean execute() throws SQLException {
        executeLargeUpdate();
        return true;
    }

    @Override
    public void addBatch() throws SQLException {
        checkClosed();
        try {
            writer.commitRow();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        checkClosed();

    }

    @Override
    public void setClob(int parameterIndex, Reader x, long length) throws SQLException {
        checkClosed();

    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        checkClosed();

    }

    @Override
    public void setBlob(int parameterIndex, InputStream x, long length) throws SQLException {
        checkClosed();

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        checkClosed();

    }

    @Override
    public void setNClob(int parameterIndex, Reader x, long length) throws SQLException {
        checkClosed();

    }

    @Override
    public void setNClob(int parameterIndex, NClob x) throws SQLException {
        checkClosed();

    }

    @Override
    public void setClob(int parameterIndex, Reader x) throws SQLException {
        checkClosed();

    }

    @Override
    public void setNClob(int parameterIndex, Reader x) throws SQLException {
        checkClosed();

    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML x) throws SQLException {
        checkClosed();

    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        checkClosed();
        writer.setValue(parameterIndex, x.getArray());
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        checkClosed();
        writer.setValue(parameterIndex, sqlDateToInstant(x, cal));
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        checkClosed();
        writer.setValue(parameterIndex, sqlTimeToInstant(x, cal));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        checkClosed();
        writer.setDateTime(parameterIndex, sqlTimestampToZDT(x, cal));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        checkClosed();
        writer.setValue(parameterIndex, null);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        checkClosed();

    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        checkClosed();

    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        checkClosed();

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        checkClosed();
        // TODO: make proper data conversion in setObject methods
        writer.setValue(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        checkClosed();
        writer.setValue(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkClosed();
        writer.setValue(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        setObject(parameterIndex, x, targetSqlType, 0);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        checkClosed();
        writer.setValue(parameterIndex, x);
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
            // ignore
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
        checkClosed();
        int batchSize = writer.getRowCount();
        long rowsInserted = executeLargeUpdate();
        int[] results = new int[batchSize];
        Arrays.fill(results, batchSize == rowsInserted? 1 : PreparedStatement.SUCCESS_NO_INFO);
        return results;
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        checkClosed();
        int batchSize = writer.getRowCount();
        long rowsInserted = executeLargeUpdate();
        long[] results = new long[batchSize];
        Arrays.fill(results, batchSize == rowsInserted? 1 : PreparedStatement.SUCCESS_NO_INFO);
        return results;
    }
}
