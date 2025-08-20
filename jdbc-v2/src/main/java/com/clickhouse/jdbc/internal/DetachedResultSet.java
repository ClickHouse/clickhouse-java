package com.clickhouse.jdbc.internal;

import com.clickhouse.jdbc.JdbcV2Wrapper;
import com.clickhouse.jdbc.ResultSetImpl;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.function.Consumer;

/**
 * This class intended only for internal use because is based on internal API. Do not try using it in your application.
 * We may change it at any time because it is internal class.
 * This class should not be used in any other places than metadata resultsets.
 * This class will close parent resultset when close on this is called.
 */
public class DetachedResultSet implements ResultSet, JdbcV2Wrapper {

    private List<Map<String, Object>> records;

    private ListIterator<Map<String, Object>> iterator;

    private ResultSetMetaData metaData;

    private Map<String, Object> record;

    private boolean wasNull;

    private int row;

    private final int lastRow;

    private boolean closed;

    private Map<String, Integer> columnMap;

    private DetachedResultSet(List<Map<String, Object>> records, ResultSetMetaData metaData) throws SQLException {
        this.records = records;
        this.iterator = records.listIterator();
        this.metaData = metaData;
        this.wasNull = false;
        this.row = ResultSetImpl.BEFORE_FIRST;
        this.lastRow = records.size();
        this.closed =  false;

        this.columnMap = new HashMap<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            this.columnMap.put(metaData.getColumnName(i), i);
        }
    }

    public static DetachedResultSet createFromResultSet(ResultSet resultSet, Collection<Consumer<Map<String, Object>>> mutators) throws SQLException {
        ResultSetMetaData  metaData = resultSet.getMetaData();
        List<Map<String, Object>> records = new ArrayList<>();
        while (resultSet.next()) {
            Map<String, Object> record = new HashMap<>();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                record.put(metaData.getColumnLabel(i), resultSet.getObject(i));
            }
            for (Consumer<Map<String, Object>> mutator : mutators) {
                mutator.accept(record);
            }
            records.add(record);
        }
        return new DetachedResultSet(records, metaData);
    }

    @Override
    public boolean next() throws SQLException {

        if (iterator.hasNext()) {
            row++;
            record  = iterator.next();
            return true;
        }
        row = ResultSetImpl.AFTER_LAST;
        return false;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
        metaData = null;
        record = null;
        iterator = null;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return getString(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return getBoolean(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return getByte(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return getShort(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return getInt(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getLong(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return getFloat(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getDouble(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(metaData.getColumnLabel(columnIndex), scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return getBytes(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getTime(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return getAsciiStream(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return getUnicodeStream(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return getBinaryStream(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getObject(columnLabel,  String.class);
    }


    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getObject(columnLabel,  Boolean.class);
    }

    private Number getNumber(String columnLabel) throws SQLException {
        return (Number) getObjectImpl(columnLabel, Number.class, BigInteger.ZERO);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getNumber(columnLabel).byteValue();
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getNumber(columnLabel).shortValue();
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getNumber(columnLabel).intValue();
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getNumber(columnLabel).longValue();
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getNumber(columnLabel).floatValue();
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getNumber(columnLabel).doubleValue();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getObject(columnLabel, BigDecimal.class);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getObject(columnLabel, byte[].class);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getObject(columnLabel, Date.class);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getObject(columnLabel, Time.class);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getObject(columnLabel, Timestamp.class);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getObject(columnLabel, InputStream.class);
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getObject(columnLabel, InputStream.class);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getObject(columnLabel, InputStream.class);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public String getCursorName() throws SQLException {
        return "";
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getObject(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(columnLabel, Object.class);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        Integer index = columnMap.get(columnLabel);
        if (index == null) {
            throw new SQLException("Column not found: " + columnLabel, ExceptionUtils.SQL_STATE_CLIENT_ERROR);
        }
        return index;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return getObject(columnIndex, Reader.class);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getObject(columnLabel, Reader.class);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getBigDecimal(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getObject(columnLabel, BigDecimal.class);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return row == ResultSetImpl.BEFORE_FIRST;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return row == ResultSetImpl.AFTER_LAST;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return row == ResultSetImpl.FIRST_ROW;
    }

    @Override
    public boolean isLast() throws SQLException {
        return row == lastRow;
    }

    @Override
    public void beforeFirst() throws SQLException {
    }

    @Override
    public void afterLast() throws SQLException {
    }

    @Override
    public boolean first() throws SQLException {
        return false;
    }

    @Override
    public boolean last() throws SQLException {
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        return row;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return false;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLException("This result set object is of FORWARD ONLY type. Only ResultSet.FETCH_FORWARD is allowed as fetchDirection.");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {

    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {

    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {

    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {

    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {

    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {

    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {

    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {

    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {

    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {

    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {

    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {

    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {

    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {

    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {

    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {

    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {

    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {

    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {

    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {

    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {

    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {

    }

    @Override
    public void insertRow() throws SQLException {

    }

    @Override
    public void updateRow() throws SQLException {

    }

    @Override
    public void deleteRow() throws SQLException {

    }

    @Override
    public void refreshRow() throws SQLException {

    }

    @Override
    public void cancelRowUpdates() throws SQLException {

    }

    @Override
    public void moveToInsertRow() throws SQLException {

    }

    @Override
    public void moveToCurrentRow() throws SQLException {

    }

    @Override
    public Statement getStatement() throws SQLException {
        return null; // should return null as it is a detached result set
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(metaData.getColumnLabel(columnIndex), map);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return getRef(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return getBlob(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return getClob(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return getArray(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        if (map == null) {
            throw new SQLException("map must be not null", ExceptionUtils.SQL_STATE_CLIENT_ERROR);
        }
        int columnIndex = columnMap.getOrDefault(columnLabel, -1);
        if (columnIndex == -1) {
            throw new SQLException("column " + columnLabel + " doesn't exist", ExceptionUtils.SQL_STATE_CLIENT_ERROR);
        }

        String typeName = metaData.getColumnTypeName(columnIndex);
        return getObject(columnLabel, map.get(typeName));
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return getObject(columnLabel, Ref.class);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getObject(columnLabel, Blob.class);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getObject(columnLabel, Clob.class);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getObject(columnLabel, Array.class);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(metaData.getColumnLabel(columnIndex), cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(metaData.getColumnLabel(columnIndex), cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(metaData.getColumnLabel(columnIndex), cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return getURL(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getObject(columnLabel, URL.class);
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {

    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {

    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {

    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {

    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return getRowId(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return getObject(columnLabel, RowId.class);
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {

    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return HOLD_CURSORS_OVER_COMMIT; // this result set remains open
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {

    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return getNClob(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getObject(columnLabel, NClob.class);
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return getSQLXML(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getObject(columnLabel, SQLXML.class);
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getNString(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getObject(columnLabel, String.class);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getNCharacterStream(metaData.getColumnLabel(columnIndex));
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getObject(columnLabel, Reader.class);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return getObject(metaData.getColumnLabel(columnIndex), type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return (T) getObjectImpl(columnLabel, type, null);
    }

    private Object getObjectImpl(String columnLabel, Class<?> type, Object nullValue) throws SQLException {
        Object value = record.get(columnLabel);
        wasNull = value == null;
        if (wasNull) {
            return nullValue;
        }

        return JdbcUtils.convert(value, type);
    }
}
