package com.clickhouse.jdbc;

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.TimeZone;

import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import com.clickhouse.jdbc.internal.JdbcUtils;
import com.clickhouse.jdbc.types.Array;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultSetImpl implements ResultSet, JdbcV2Wrapper {
    private static final Logger log = LoggerFactory.getLogger(ResultSetImpl.class);
    private final ResultSetMetaData metaData;
    protected ClickHouseBinaryFormatReader reader;
    private QueryResponse response;
    private boolean closed;
    private final StatementImpl parentStatement;
    private boolean wasNull;
    private final Calendar defaultCalendar;

    public ResultSetImpl(StatementImpl parentStatement, QueryResponse response, ClickHouseBinaryFormatReader reader) {
        this.parentStatement = parentStatement;
        this.response = response;
        this.reader = reader;
        this.metaData = new com.clickhouse.jdbc.metadata.ResultSetMetaData(this);
        this.closed = false;
        this.wasNull = false;
        this.defaultCalendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed.", ExceptionUtils.SQL_STATE_CONNECTION_EXCEPTION);
        }
    }

    public TableSchema getSchema() {
        return reader.getSchema();
    }


    @Override
    public boolean next() throws SQLException {
        checkClosed();

        try {
            Map<String, Object> currentRow = reader.next();
            return currentRow != null;
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public void close() throws SQLException {
        closed = true;

        Exception e = null;
        try {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception re) {
                    log.debug("Error closing reader", re);
                    e = re;
                } finally {
                    reader = null;
                }
            }
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (Exception re) {
                    log.debug("Error closing response", re);
                    e = re;
                } finally {
                    response = null;
                }
            }
        }

        if (e != null) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnIndex)) {
                wasNull = false;
                return reader.getString(columnIndex);
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getString(%s)", parentStatement.getLastSql(), columnIndex), e);
        }
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnIndex)) {
                wasNull = false;
                return reader.getBoolean(columnIndex);
            } else {
                wasNull = true;
                return false;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getBoolean(%s)", parentStatement.getLastSql(), columnIndex), e);
        }
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnIndex)) {
                wasNull = false;
                return reader.getByte(columnIndex);
            } else {
                wasNull = true;
                return 0;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getByte(%s)", parentStatement.getLastSql(), columnIndex), e);
        }
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnIndex)) {
                wasNull = false;
                return reader.getShort(columnIndex);
            } else {
                wasNull = true;
                return 0;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getShort(%s)", parentStatement.getLastSql(), columnIndex), e);
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnIndex)) {
                wasNull = false;
                return reader.getInteger(columnIndex);
            } else {
                wasNull = true;
                return 0;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getInt(%s)", parentStatement.getLastSql(), columnIndex), e);
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnIndex)) {
                wasNull = false;
                return reader.getLong(columnIndex);
            } else {
                wasNull = true;
                return 0;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getLong(%s)", parentStatement.getLastSql(), columnIndex), e);
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnIndex)) {
                wasNull = false;
                return reader.getFloat(columnIndex);
            } else {
                wasNull = true;
                return 0;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getFloat(%s)", parentStatement.getLastSql(), columnIndex), e);
        }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnIndex)) {
                wasNull = false;
                return reader.getDouble(columnIndex);
            } else {
                wasNull = true;
                return 0;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getDouble(%s)", parentStatement.getLastSql(), columnIndex), e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnIndex)) {
                wasNull = false;
                return reader.getBigDecimal(columnIndex);
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getBigDecimal(%s)", parentStatement.getLastSql(), columnIndex), e);
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnIndex)) {
                wasNull = false;
                return reader.getByteArray(columnIndex);
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getBytes(%s)", parentStatement.getLastSql(), columnIndex), e);
        }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        checkClosed();
        try {
            //TODO: Add this to ClickHouseBinaryFormatReader
            LocalDate localDate = reader.getLocalDate(columnIndex);
            if (localDate == null) {
                wasNull = true;
                return null;
            }

            wasNull = false;
            return Date.valueOf(localDate);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getDate(%s)", parentStatement.getLastSql(), columnIndex), e);
        }
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkClosed();
        try {
            LocalDateTime localDateTime = reader.getLocalDateTime(columnIndex);
            if (localDateTime == null) {
                wasNull = true;
                return null;
            }

            wasNull = false;
            return Time.valueOf(localDateTime.toLocalTime());
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getTime(%s)", parentStatement.getLastSql(), columnIndex), e);
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkClosed();
        try {
            LocalDateTime localDateTime = reader.getLocalDateTime(columnIndex);
            if (localDateTime == null) {
                wasNull = true;
                return null;
            }

            wasNull = false;
            return Timestamp.valueOf(localDateTime);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getTimestamp(%s)", parentStatement.getLastSql(), columnIndex), e);
        }
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        checkClosed();
        //TODO: Add this to ClickHouseBinaryFormatReader
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("AsciiStream is not yet supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            return new ByteArrayInputStream(reader.getString(columnIndex).getBytes(StandardCharsets.UTF_8));
        }

        return null;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        checkClosed();
        //TODO: implement
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("BinaryStream is not yet supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnLabel)) {
                wasNull = false;
                return reader.getString(columnLabel);
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getString(%s)", parentStatement.getLastSql(), columnLabel), e);
        }
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnLabel)) {
                wasNull = false;
                return reader.getBoolean(columnLabel);
            } else {
                wasNull = true;
                return false;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getBoolean(%s)", parentStatement.getLastSql(), columnLabel), e);
        }
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnLabel)) {
                wasNull = false;
                return reader.getByte(columnLabel);
            } else {
                wasNull = true;
                return 0;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getByte(%s)", parentStatement.getLastSql(), columnLabel), e);
        }
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnLabel)) {
                wasNull = false;
                return reader.getShort(columnLabel);
            } else {
                wasNull = true;
                return 0;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getShort(%s)", parentStatement.getLastSql(), columnLabel), e);
        }
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnLabel)) {
                wasNull = false;
                return reader.getInteger(columnLabel);
            } else {
                wasNull = true;
                return 0;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getInt(%s)", parentStatement.getLastSql(), columnLabel), e);
        }
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnLabel)) {
                wasNull = false;
                return reader.getLong(columnLabel);
            } else {
                wasNull = true;
                return 0;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getLong(%s)", parentStatement.getLastSql(), columnLabel), e);
        }
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnLabel)) {
                wasNull = false;
                return reader.getFloat(columnLabel);
            } else {
                wasNull = true;
                return 0;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getFloat(%s)", parentStatement.getLastSql(), columnLabel), e);
        }
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnLabel)) {
                wasNull = false;
                return reader.getDouble(columnLabel);
            } else {
                wasNull = true;
                return 0;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getDouble(%s)", parentStatement.getLastSql(), columnLabel), e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnLabel)) {
                wasNull = false;
                return reader.getBigDecimal(columnLabel);
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getBigDecimal(%s)", parentStatement.getLastSql(), columnLabel), e);
        }
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnLabel)) {
                wasNull = false;
                return reader.getByteArray(columnLabel);
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getBytes(%s)", parentStatement.getLastSql(), columnLabel), e);
        }
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        checkClosed();
        try {
            //TODO: Add this to ClickHouseBinaryFormatReader
            LocalDate localDate = reader.getLocalDate(columnLabel);
            if (localDate == null) {
                wasNull = true;
                return null;
            }

            wasNull = false;
            return Date.valueOf(localDate);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getDate(%s)", parentStatement.getLastSql(), columnLabel), e);
        }
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        checkClosed();
        try {
            LocalDateTime localDateTime = reader.getLocalDateTime(columnLabel);
            if(localDateTime == null) {
                wasNull = true;
                return null;
            }

            wasNull = false;
            return Time.valueOf(localDateTime.toLocalTime());
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getTime(%s)", parentStatement.getLastSql(), columnLabel), e);
        }
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        checkClosed();
        try {
            LocalDateTime localDateTime = reader.getLocalDateTime(columnLabel);
            if (localDateTime == null) {
                wasNull = true;
                return null;
            }

            wasNull = false;
            return Timestamp.valueOf(localDateTime);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getTimestamp(%s)", parentStatement.getLastSql(), columnLabel), e);
        }
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        checkClosed();
        //TODO: Add this to ClickHouseBinaryFormatReader
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("AsciiStream is not yet supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        checkClosed();
        return new ByteArrayInputStream(reader.getString(columnLabel).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        checkClosed();
        //TODO: implement
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("BinaryStream is not yet supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
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
    public String getCursorName() throws SQLException {
        checkClosed();
        return "";
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return metaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getObject(getSchema().columnIndexToName(columnIndex));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(columnLabel, JdbcUtils.convertToJavaClass(getSchema().getColumnByName(columnLabel).getDataType()));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        try {
            return reader.getSchema().getColumnByName(columnLabel).getColumnIndex();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: findColumn(%s)", parentStatement.getLastSql(), columnLabel), e);
        }
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("CharacterStream is not yet supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("CharacterStream is not yet supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnIndex)) {
                wasNull = false;
                return reader.getBigDecimal(columnIndex);
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getBigDecimal(%s)", parentStatement.getLastSql(), columnIndex), e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnLabel)) {
                wasNull = false;
                return reader.getBigDecimal(columnLabel);
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getBigDecimal(%s)", parentStatement.getLastSql(), columnLabel), e);
        }
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("isBeforeFirst is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return false;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("isAfterLast is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return false;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("isFirst is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return false;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("isLast is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return false;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClosed();
    }

    @Override
    public void afterLast() throws SQLException {
        checkClosed();
    }

    @Override
    public boolean first() throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("first is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return false;
    }

    @Override
    public boolean last() throws SQLException {
        checkClosed();

        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("last is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return false;
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("getRow is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkClosed();

        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("absolute is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return false;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("relative is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("previous is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return false;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("setFetchDirection is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
    }

    @Override
    public int getType() throws SQLException {
        checkClosed();
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkClosed();
        return CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        updateNull(getSchema().columnIndexToName(columnIndex));
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        updateBoolean(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        updateByte(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        updateShort(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        updateInt(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        updateLong(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        updateFloat(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        updateDouble(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        updateBigDecimal(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        updateString(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        updateBytes(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        updateDate(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        updateTime(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        updateTimestamp(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        updateAsciiStream(getSchema().columnIndexToName(columnIndex), x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        updateBinaryStream(getSchema().columnIndexToName(columnIndex), x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        updateCharacterStream(getSchema().columnIndexToName(columnIndex), x, length);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        updateObject(getSchema().columnIndexToName(columnIndex), x, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        updateObject(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void insertRow() throws SQLException {
        checkClosed();
    }

    @Override
    public void updateRow() throws SQLException {
        checkClosed();
    }

    @Override
    public void deleteRow() throws SQLException {
        checkClosed();
    }

    @Override
    public void refreshRow() throws SQLException {
        checkClosed();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        checkClosed();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        checkClosed();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        checkClosed();
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkClosed();
        return this.parentStatement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(getSchema().columnIndexToName(columnIndex), map);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return getRef(getSchema().columnIndexToName(columnIndex));
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return getBlob(getSchema().columnIndexToName(columnIndex));
    }

    @Override
    public java.sql.Clob getClob(int columnIndex) throws SQLException {
        return getClob(getSchema().columnIndexToName(columnIndex));
    }

    @Override
    public java.sql.Array getArray(int columnIndex) throws SQLException {
        return getArray(getSchema().columnIndexToName(columnIndex));
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        return getObject(columnLabel, map.get(JdbcUtils.convertToSqlType(getSchema().getColumnByName(columnLabel).getDataType()).getName()));
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Ref is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Blob is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Clob is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public java.sql.Array getArray(String columnLabel) throws SQLException {
        checkClosed();
        try {
            ClickHouseColumn column = getSchema().getColumnByName(columnLabel);
            return new Array(reader.getList(columnLabel),
                    column.getArrayBaseColumn().getDataType().name(),
                    JdbcUtils.convertToSqlType(column.getArrayBaseColumn().getDataType()).getVendorTypeNumber());
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("SQL: [%s]; Method: getArray(%s)", parentStatement.getLastSql(), columnLabel), e);
        }
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(getSchema().columnIndexToName(columnIndex), cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        checkClosed();
        Date date = getDate(columnLabel);
        if (date == null) {
            return null;
        }
        LocalDate d = date.toLocalDate();
        Calendar c = (Calendar)( cal != null ? cal : defaultCalendar).clone();
        c.clear();
        c.set(d.getYear(), d.getMonthValue() - 1, d.getDayOfMonth(), 0, 0, 0);
        return new Date(c.getTimeInMillis());
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(getSchema().columnIndexToName(columnIndex), cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        checkClosed();
        return getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(getSchema().columnIndexToName(columnIndex), cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        checkClosed();
        return getTimestamp(columnLabel);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return getURL(getSchema().columnIndexToName(columnIndex));
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        checkClosed();
        try {
            return new URL(reader.getString(columnLabel));
        } catch (MalformedURLException e) {
            throw new SQLDataException(e.getMessage(), ExceptionUtils.SQL_STATE_DATA_EXCEPTION, e);
        }
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        updateRef(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        updateBlob(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        updateClob(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateArray(int columnIndex, java.sql.Array x) throws SQLException {
        updateArray(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateArray(String columnLabel, java.sql.Array x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return getRowId(getSchema().columnIndexToName(columnIndex));
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        updateRowId(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        updateNString(getSchema().columnIndexToName(columnIndex), nString);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        updateNClob(getSchema().columnIndexToName(columnIndex), nClob);
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return getNClob(getSchema().columnIndexToName(columnIndex));
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("NClob is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return getSQLXML(getSchema().columnIndexToName(columnIndex));
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("SQLXML is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        updateSQLXML(getSchema().columnIndexToName(columnIndex), xmlObject);
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getNString(getSchema().columnIndexToName(columnIndex));
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        checkClosed();
        return reader.getString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getNCharacterStream(getSchema().columnIndexToName(columnIndex));
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        checkClosed();
        return new CharArrayReader(reader.getString(columnLabel).toCharArray());
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        updateNCharacterStream(getSchema().columnIndexToName(columnIndex), x, length);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        updateAsciiStream(getSchema().columnIndexToName(columnIndex), x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        updateBinaryStream(getSchema().columnIndexToName(columnIndex), x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        updateCharacterStream(getSchema().columnIndexToName(columnIndex), x, length);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        updateBlob(getSchema().columnIndexToName(columnIndex), inputStream, length);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        updateClob(getSchema().columnIndexToName(columnIndex), reader, length);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        updateNClob(getSchema().columnIndexToName(columnIndex), reader, length);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        updateNCharacterStream(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        updateAsciiStream(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        updateBinaryStream(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        updateCharacterStream(getSchema().columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        updateBlob(getSchema().columnIndexToName(columnIndex), inputStream);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        updateClob(getSchema().columnIndexToName(columnIndex), reader);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        updateNClob(getSchema().columnIndexToName(columnIndex), reader);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        checkClosed();
        if (!parentStatement.connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Writes are not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return getObject(getSchema().columnIndexToName(columnIndex), type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        checkClosed();
        try {
            if (reader.hasValue(columnLabel)) {
                wasNull = false;
                if (type == null) {//As a fallback, try to get the value as is
                    return reader.readValue(columnLabel);
                }

                return (T) JdbcUtils.convert(reader.readValue(columnLabel), type);
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        updateObject(getSchema().columnIndexToName(columnIndex), x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        checkClosed();
        ResultSet.super.updateObject(columnLabel, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        updateObject(getSchema().columnIndexToName(columnIndex), x, targetSqlType);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        checkClosed();
        ResultSet.super.updateObject(columnLabel, x, targetSqlType);
    }
}
