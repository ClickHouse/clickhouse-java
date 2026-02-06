package com.clickhouse.jdbc;

import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import com.clickhouse.jdbc.internal.FeatureManager;
import com.clickhouse.jdbc.internal.JdbcUtils;
import com.clickhouse.jdbc.metadata.ResultSetMetaDataImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ResultSetImpl implements ResultSet, JdbcV2Wrapper {
    private static final Logger log = LoggerFactory.getLogger(ResultSetImpl.class);
    private ResultSetMetaDataImpl metaData;
    protected ClickHouseBinaryFormatReader reader;
    private QueryResponse response;
    private boolean closed;
    private final StatementImpl parentStatement;
    private boolean wasNull;
    private final Calendar defaultCalendar;

    private final FeatureManager  featureManager;

    public static final int AFTER_LAST = -1;
    public static final int BEFORE_FIRST = 0;
    public static final int FIRST_ROW = 1;
    private int rowPos;

    private int fetchSize;
    private int fetchDirection;
    @SuppressWarnings("unused")
    private final int maxFieldSize;
    private final int maxRows;

    private Consumer<Exception> onDataTransferException;

    public ResultSetImpl(StatementImpl parentStatement, QueryResponse response, ClickHouseBinaryFormatReader reader,
                         Consumer<Exception> onDataTransferException) throws SQLException {
        this.parentStatement = parentStatement;
        this.response = response;
        this.reader = reader;
        this.featureManager = new FeatureManager(parentStatement.getConnection().getJdbcConfig());
        TableSchema tableMetadata = reader.getSchema();

        // Result set contains columns from one database (there is a special table engine 'Merge' to do cross DB queries)
        this.metaData = new ResultSetMetaDataImpl(tableMetadata
                .getColumns(), response.getSettings().getDatabase(), "", tableMetadata.getTableName(),
                JdbcUtils.DATA_TYPE_CLASS_MAP);
        this.closed = false;
        this.wasNull = false;
        this.defaultCalendar = parentStatement.getConnection().defaultCalendar;
        this.rowPos = BEFORE_FIRST;
        this.fetchSize = parentStatement.getFetchSize();
        this.fetchDirection = parentStatement.getFetchDirection();
        this.maxFieldSize = parentStatement.getMaxFieldSize();
        this.maxRows = parentStatement.getMaxRows();
        this.onDataTransferException = onDataTransferException;
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed.", ExceptionUtils.SQL_STATE_CONNECTION_EXCEPTION);
        }
    }

    public TableSchema getSchema() {
        return reader.getSchema();
    }

    private String columnIndexToName(int index) throws SQLException {
        try {
            return getSchema().columnIndexToName(index);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("Method: columnIndexToName(%s) encountered an exception.", index), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
        }
    }


    @Override
    public boolean next() throws SQLException {
        checkClosed();

        if (rowPos == AFTER_LAST) {
            return false;
        }

        if (maxRows > 0 && rowPos == maxRows) {
            // rowPos is at current position. if we reached here it means we stepped over maxRows
            rowPos = AFTER_LAST;
            return false;
        }

        try {
            Object readerRow = reader.next();
            if (readerRow != null) {
                rowPos++;
            } else {
                rowPos = AFTER_LAST;
            }
            return readerRow != null;
        } catch (Exception e) {
            if (onDataTransferException != null) {
                onDataTransferException.accept(e);
            }
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
            parentStatement.onResultSetClosed(this);
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
        return getString(columnIndexToName(columnIndex));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return getBoolean(columnIndexToName(columnIndex));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return getByte(columnIndexToName(columnIndex));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return getShort(columnIndexToName(columnIndex));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return getInt(columnIndexToName(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getLong(columnIndexToName(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return getFloat(columnIndexToName(columnIndex));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getDouble(columnIndexToName(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(columnIndexToName(columnIndex), scale);
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
            throw ExceptionUtils.toSqlState(String.format("Method: getBytes(\"%d\") encountered an exception.", columnIndex),
                    String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
        }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(columnIndex, null);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getTime(columnIndex, null);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(columnIndex, null);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return getAsciiStream(columnIndexToName(columnIndex));
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return getUnicodeStream(columnIndexToName(columnIndex));
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return getBinaryStream(columnIndexToName(columnIndex));
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
            throw ExceptionUtils.toSqlState(String.format("Method: getString(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
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
            throw ExceptionUtils.toSqlState(String.format("Method: getBoolean(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
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
            throw ExceptionUtils.toSqlState(String.format("Method: getByte(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
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
            throw ExceptionUtils.toSqlState(String.format("Method: getShort(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
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
            throw ExceptionUtils.toSqlState(String.format("Method: getInt(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
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
            throw ExceptionUtils.toSqlState(String.format("Method: getLong(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
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
            throw ExceptionUtils.toSqlState(String.format("Method: getFloat(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
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
            throw ExceptionUtils.toSqlState(String.format("Method: getDouble(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
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
            throw ExceptionUtils.toSqlState(String.format("Method: getBigDecimal(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
        }
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(getSchema().nameToColumnIndex(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(columnLabel, null);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(columnLabel, null);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(columnLabel, null);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("getAsciiStream");

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
        featureManager.unsupportedFeatureThrow("getBinaryStream");

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
        featureManager.unsupportedFeatureThrow("getCursorName");
        return "";
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return metaData;
    }

    protected void setMetaData(ResultSetMetaDataImpl metaData) {
        this.metaData = metaData;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        try {
            return reader.getSchema().nameToColumnIndex(columnLabel);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("Method: findColumn(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
        }
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("getCharacterStream");

        return null;
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("getCharacterStream");

        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getBigDecimal(columnIndexToName(columnIndex));
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
            throw ExceptionUtils.toSqlState(String.format("Method: getBigDecimal(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
        }
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        return rowPos == BEFORE_FIRST;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        return rowPos == AFTER_LAST;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        return rowPos == FIRST_ROW;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        return (!reader.hasNext() || rowPos == maxRows) && rowPos != AFTER_LAST && rowPos != BEFORE_FIRST;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("beforeFirst");
    }

    @Override
    public void afterLast() throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("afterLast");
    }

    @Override
    public boolean first() throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("first");

        return false;
    }

    @Override
    public boolean last() throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("last");

        return false;
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        return rowPos == AFTER_LAST ? 0 : rowPos;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("absolute");

        return false;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("relative");

        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("previous");

        return false;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return fetchDirection;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        if (getType() == TYPE_FORWARD_ONLY && direction != ResultSet.FETCH_FORWARD) {
            throw new SQLException("This result set object is of FORWARD ONLY type. Only ResultSet.FETCH_FORWARD is allowed as fetchDirection.");
        }
        fetchDirection = direction;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return fetchSize;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        if (rows < 0) {
            throw new SQLException("Number of rows should be a positive integer");
        }
        fetchSize = rows;
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
        featureManager.unsupportedFeatureThrow("rowUpdated");

        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("rowInserted");

        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("rowDeleted");

        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        updateNull(columnIndexToName(columnIndex));
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        updateBoolean(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        updateByte(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        updateShort(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        updateInt(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        updateLong(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        updateFloat(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        updateDouble(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        updateBigDecimal(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        updateString(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        updateBytes(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        updateDate(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        updateTime(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        updateTimestamp(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        updateAsciiStream(columnIndexToName(columnIndex), x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        updateBinaryStream(columnIndexToName(columnIndex), x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        updateCharacterStream(columnIndexToName(columnIndex), x, length);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        updateObject(columnIndexToName(columnIndex), x, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        updateObject(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateNull");

    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateBoolean");

    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateByte");

    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateShort");

    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateInt");

    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateLong");

    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateFloat");

    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateDouble");

    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateBigDecimal");

    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateString");

    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateBytes");

    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateDate");

    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateTime");

    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateTimestamp");

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateAsciiStream");

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateBinaryStream");

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateCharacterStream");

    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateObject");

    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateObject");

    }

    @Override
    public void insertRow() throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("insertRow");
    }

    @Override
    public void updateRow() throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateRow");
    }

    @Override
    public void deleteRow() throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("deleteRow");
    }

    @Override
    public void refreshRow() throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("refreshRow");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("cancelRowUpdates");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("moveToInsertRow");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("moveToCurrentRow");
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkClosed();
        return this.parentStatement;
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return getRef(columnIndexToName(columnIndex));
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return getBlob(columnIndexToName(columnIndex));
    }

    @Override
    public java.sql.Clob getClob(int columnIndex) throws SQLException {
        return getClob(columnIndexToName(columnIndex));
    }

    @Override
    public java.sql.Array getArray(int columnIndex) throws SQLException {
        return getObject(columnIndex, java.sql.Array.class);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("getRef");

        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("getBlob");

        return null;
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("getClob");

        return null;
    }

    @Override
    public java.sql.Array getArray(String columnLabel) throws SQLException {
        return getObject(columnLabel, java.sql.Array.class);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndexToName(columnIndex), cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        checkClosed();
        try {
            LocalDate ld = reader.getLocalDate(columnLabel);
            if (ld == null) {
                wasNull = true;
                return null;
            }
            wasNull = false;

            Calendar c = (Calendar) (cal != null ? cal : defaultCalendar).clone();
            c.clear();
            c.set(ld.getYear(), ld.getMonthValue() - 1, ld.getDayOfMonth(), 0, 0, 0);
            return new Date(c.getTimeInMillis());
        } catch (Exception e) {
            ClickHouseColumn column = getSchema().getColumnByName(columnLabel);
            switch (column.getValueDataType()) {
                case Date:
                case Date32:
                case DateTime64:
                case DateTime:
                case DateTime32:
                    break;
                default:
                    throw new SQLException("Value of " + column.getValueDataType() + " type cannot be converted to Date value");
            }
            throw ExceptionUtils.toSqlState(String.format("Method: getDate(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
        }
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndexToName(columnIndex), cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        checkClosed();

        try {
            LocalDateTime ld = reader.getLocalDateTime(columnLabel);
            if (ld == null) {
                wasNull = true;
                return null;
            }
            wasNull = false;
            Calendar c = cal != null ? cal : defaultCalendar;
            long time = ld.atZone(c.getTimeZone().toZoneId()).toEpochSecond() * 1000 + TimeUnit.NANOSECONDS.toMillis(ld.getNano());
            return new Time(time);
        } catch (Exception e) {
            ClickHouseColumn column = getSchema().getColumnByName(columnLabel);
            switch (column.getValueDataType()) {
                case Time:
                case Time64:
                case DateTime64:
                case DateTime:
                case DateTime32:
                    break;
                default:
                    throw new SQLException("Value of " + column.getValueDataType() + " type cannot be converted to Time value");
            }

            throw ExceptionUtils.toSqlState(String.format("Method: getTime(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndexToName(columnIndex), cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        checkClosed();
        try {
            ZonedDateTime zdt = reader.getZonedDateTime(columnLabel);
            if (zdt == null) {
                wasNull = true;
                return null;
            }
            wasNull = false;

            Calendar c = (Calendar) (cal != null ? cal : defaultCalendar).clone();
            c.set(zdt.getYear(), zdt.getMonthValue() - 1, zdt.getDayOfMonth(), zdt.getHour(), zdt.getMinute(),
                    zdt.getSecond());
            Timestamp timestamp = new Timestamp(c.getTimeInMillis());
            timestamp.setNanos(zdt.getNano());
            return timestamp;
        } catch (Exception e) {
            ClickHouseColumn column = getSchema().getColumnByName(columnLabel);
            switch (column.getValueDataType()) {
                case DateTime64:
                case DateTime:
                case DateTime32:
                    break;
                default:
                    throw new SQLException("Value of " + column.getValueDataType() + " type cannot be converted to Timestamp value");
            }

            throw ExceptionUtils.toSqlState(String.format("Method: getTimestamp(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
        }
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return getURL(columnIndexToName(columnIndex));
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        checkClosed();
        try {
            return new URL(reader.getString(columnLabel));
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("Method: getURL(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
        }
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        updateRef(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateRef");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        updateBlob(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        updateClob(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateClob");
    }

    @Override
    public void updateArray(int columnIndex, java.sql.Array x) throws SQLException {
        updateArray(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateArray(String columnLabel, java.sql.Array x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateArray");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return getRowId(columnIndexToName(columnIndex));
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("getRowId");
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        updateRowId(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateRowId");
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
        updateNString(columnIndexToName(columnIndex), nString);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateNString");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        updateNClob(columnIndexToName(columnIndex), nClob);
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateNClob");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return getNClob(columnIndexToName(columnIndex));
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("getNClob");

        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return getSQLXML(columnIndexToName(columnIndex));
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("getSQLXML");

        return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        updateSQLXML(columnIndexToName(columnIndex), xmlObject);
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateSQLXML");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getNString(columnIndexToName(columnIndex));
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        checkClosed();
        try {
            return reader.getString(columnLabel);
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("Method: getNString(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
        }
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getNCharacterStream(columnIndexToName(columnIndex));
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        checkClosed();
        try {
            return new StringReader(reader.getString(columnLabel));
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("Method: getNCharacterStream(\"%s\") encountered an exception.", columnLabel), String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
        }
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        updateNCharacterStream(columnIndexToName(columnIndex), x, length);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        updateAsciiStream(columnIndexToName(columnIndex), x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        updateBinaryStream(columnIndexToName(columnIndex), x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        updateCharacterStream(columnIndexToName(columnIndex), x, length);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateCharacterStream");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        updateBlob(columnIndexToName(columnIndex), inputStream, length);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        updateClob(columnIndexToName(columnIndex), reader, length);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        updateNClob(columnIndexToName(columnIndex), reader, length);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateNClob");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        updateNCharacterStream(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        updateAsciiStream(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        updateBinaryStream(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        updateCharacterStream(columnIndexToName(columnIndex), x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateCharacterStream");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        updateBlob(columnIndexToName(columnIndex), inputStream);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        updateClob(columnIndexToName(columnIndex), reader);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        updateNClob(columnIndexToName(columnIndex), reader);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateNClob");
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getObject(columnIndexToName(columnIndex));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObjectImpl(columnLabel, null, Collections.emptyMap());
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndexToName(columnIndex), map);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        return getObjectImpl(columnLabel, null, map);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        checkClosed();
        return getObject(columnIndexToName(columnIndex), type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        checkClosed();
        return getObjectImpl(columnLabel, type, Collections.emptyMap());
    }

    @SuppressWarnings("unchecked")
    public <T> T getObjectImpl(String columnLabel, Class<?> type, Map<String, Class<?>> typeMap) throws SQLException {
        try {
            ClickHouseColumn column = getSchema().getColumnByName(columnLabel);
            if (column == null) {
                throw new SQLException("Column \"" + columnLabel + "\" does not exist.");
            }

            if (reader.hasValue(columnLabel)) {
                wasNull = false;

                if (type == null) {
                    switch (column.getDataType()) {
                        case Point:
                        case Ring:
                        case LineString:
                        case Polygon:
                        case MultiPolygon:
                        case MultiLineString:
                            break; // read as is
                        default:
                            if (typeMap == null || typeMap.isEmpty()) {
                                type = JdbcUtils.convertToJavaClass(column.getDataType());
                            } else {
                                type = typeMap.get(JdbcUtils.convertToSqlType(column.getDataType()).getName());
                            }
                    }
                } else {
                    ///  shortcut
                    if (type == Timestamp.class) {
                        return (T) getTimestamp(columnLabel);
                    } else if (type == Time.class) {
                        return (T) getTime(columnLabel);
                    } else if (type == Date.class) {
                        return (T) getDate(columnLabel);
                    }
                }

                if (type == null) {//As a fallback, try to get the value as is
                    return reader.readValue(columnLabel);
                }

                return (T) JdbcUtils.convert(reader.readValue(columnLabel), type, column);
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(String.format("Method: getObject(\"%s\", %s) encountered an exception.", columnLabel, type),
                    String.format("SQL: [%s]", parentStatement.getLastStatementSql()), e);
        }
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        updateObject(columnIndexToName(columnIndex), x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateObject");
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        updateObject(columnIndexToName(columnIndex), x, targetSqlType);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        checkClosed();
        featureManager.unsupportedFeatureThrow("updateObject");
    }
}
