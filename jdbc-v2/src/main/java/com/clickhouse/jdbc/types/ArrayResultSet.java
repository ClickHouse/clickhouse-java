package com.clickhouse.jdbc.types;

import com.clickhouse.client.api.data_formats.internal.ValueConverters;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.jdbc.internal.JdbcUtils;
import com.clickhouse.jdbc.metadata.ResultSetMetaDataImpl;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ArrayResultSet implements ResultSet {

    private final Object array;
    private final int length;
    private Integer pos;
    private boolean closed;
    private ResultSetMetaDataImpl metadata;

    private static final ClickHouseColumn INDEX_COLUMN = ClickHouseColumn.of("INDEX", ClickHouseDataType.UInt32, false, 0, 0);
    private static final String VALUE_COLUMN = "VALUE";
    private int fetchDirection = ResultSet.FETCH_FORWARD;
    private int fetchSize = 0;
    private boolean wasNull = false;
    private Map<Class<?>, Function<Object, Object>> converterMap;

    private final ClickHouseDataType componentDataType;
    private final Class<?> defaultClass;
    private final ClickHouseColumn column;

    public ArrayResultSet(Object array, ClickHouseColumn column) {
        this.array = array;
        this.length = java.lang.reflect.Array.getLength(array);
        this.pos = -1;
        this.column = column;

        List<ClickHouseColumn> nestedColumns = column.getNestedColumns();
        ClickHouseColumn valueColumn = column.getArrayNestedLevel() == 1 ? column.getArrayBaseColumn() : nestedColumns.get(0);
        this.metadata = new ResultSetMetaDataImpl(Arrays.asList(INDEX_COLUMN, valueColumn)
                , "", "", "", JdbcUtils.DATA_TYPE_CLASS_MAP);
        this.componentDataType = valueColumn.getDataType();
        this.defaultClass = JdbcUtils.DATA_TYPE_CLASS_MAP.get(componentDataType);
        if (this.length > 1) {
            ValueConverters converters = new ValueConverters();
            Class<?> itemClass = array.getClass().getComponentType();
            if (itemClass == null) {
                itemClass = java.lang.reflect.Array.get(array, 0).getClass();
            }
            converterMap = converters.getConvertersForType(itemClass);
        } else {
            // empty array - no values to convert
            converterMap = null;
        }
    }

    @Override
    public boolean next() throws SQLException {
        if (pos == length || length == 0) {
            return false;
        }
        pos++;
        return true;
    }

    private void checkColumnIndex(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > length) {
            throw new SQLException("Invalid column index: " + columnIndex);
        }
    }


    private void checkRowPosition() throws SQLException {
        if (pos < 0 || pos >= length) {
            throw new SQLException("No current row");
        }
    }

    private Object getValueAsObject(int columnIndex, Class<?> type, Object defaultValue) throws SQLException {
        checkColumnIndex(columnIndex);
        checkRowPosition();
        if (columnIndex == 1) {
            return pos;
        }

        Object value = java.lang.reflect.Array.get(array, pos);
        if (value != null && type == Array.class) {
            ClickHouseColumn nestedColumn = column.getArrayNestedLevel() == 1 ? column.getArrayBaseColumn() : column.getNestedColumns().get(0);
            return new com.clickhouse.jdbc.types.Array(nestedColumn, JdbcUtils.arrayToObjectArray(value));
        } else if (value != null && type != Object.class) {
            // if there is something to convert. type == Object.class means no conversion
            Function<Object, Object> converter = converterMap.get(type);
            if (converter != null) {
                value = converter.apply(value);
            } else {
                throw new SQLException("Value of " + value.getClass() + " cannot be converted to " + type);
            }
        }
        wasNull = value == null;
        return value == null ? defaultValue : value;
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        boolean tmp = wasNull;
        wasNull = false;
        return tmp;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkColumnIndex(columnIndex);
        return (String) getValueAsObject(columnIndex, String.class, null);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        if (columnIndex == 1) {
            throw new SQLException("INDEX column cannot be get as boolean");
        }
        return (Boolean) getValueAsObject(columnIndex, Boolean.class, false);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        if (columnIndex == 1) {
            if (pos < Byte.MAX_VALUE) {
                return (byte) getRow();
            } else {
                throw new SQLException("INDEX column value too big and cannot be get as byte");
            }
        }
        return ((Number) getValueAsObject(columnIndex, Byte.class, 0)).byteValue();
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        if (columnIndex == 1) {
            if (pos < Short.MAX_VALUE) {
                return (short) getRow();
            } else {
                throw new SQLException("INDEX column value too big and cannot be get as short");
            }
        }
        return ((Number) getValueAsObject(columnIndex, Short.class, 0)).shortValue();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        if (columnIndex == 1) {
            return getRow();
        }
        return ((Number) getValueAsObject(columnIndex, Integer.class, 0)).intValue();
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        if (columnIndex == 1) {
            return getRow();
        }
        return ((Number) getValueAsObject(columnIndex, Long.class, 0L)).longValue();
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        if (columnIndex == 1) {
            return (float) getRow();
        }
        return ((Number) getValueAsObject(columnIndex, Float.class, 0.0f)).floatValue();
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        if (columnIndex == 1) {
            return getRow();
        }
        return ((Number) getValueAsObject(columnIndex, Double.class, 0.0d)).doubleValue();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        if (columnIndex == 1) {
            return BigDecimal.valueOf(getRow());
        }
        return (BigDecimal) getValueAsObject(columnIndex, BigDecimal.class, null);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        if (columnIndex == 1) {
            throw new SQLException("INDEX column cannot be get as bytes");
        }
        return (byte[]) getValueAsObject(columnIndex, byte[].class, null);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        if (columnIndex == 1) {
            throw new SQLException("INDEX column cannot be get as date");
        }
        return (Date) getValueAsObject(columnIndex, Date.class, null);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkColumnIndex(columnIndex);
        checkRowPosition();
        if (columnIndex == 1) {
            throw new SQLException("INDEX column cannot be get as time");
        }
        return (Time) getValueAsObject(columnIndex, Time.class, null);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkColumnIndex(columnIndex);
        checkRowPosition();
        if (columnIndex == 1) {
            throw new SQLException("INDEX column cannot be get as timestamp");
        }
        return (Timestamp) getValueAsObject(columnIndex, Timestamp.class, null);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        checkColumnIndex(columnIndex);
        checkRowPosition();
        if (columnIndex == 1) {
            throw new SQLException("INDEX column cannot be get as ascii stream");
        }
        throw new SQLFeatureNotSupportedException("getAsciiStream is not implemented");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        checkColumnIndex(columnIndex);
        checkRowPosition();
        if (columnIndex == 1) {
            throw new SQLException("INDEX column cannot be get as unicode stream");
        }
        throw new SQLFeatureNotSupportedException("getUnicodeStream is not implemented");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        checkColumnIndex(columnIndex);
        checkRowPosition();
        if (columnIndex == 1) {
            throw new SQLException("INDEX column cannot be get as binary stream");
        }
        throw new SQLFeatureNotSupportedException("getBinaryStream is not implemented");
    }

    private int getColumnIndex(String columnLabel) throws SQLException {
        if (columnLabel.equalsIgnoreCase(INDEX_COLUMN.getColumnName())) {
            return 1;
        }
        if (columnLabel.equalsIgnoreCase(VALUE_COLUMN)) {
            return 2;
        }

        throw new SQLException("Unknown column label `" + columnLabel + "`");
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(getColumnIndex(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(getColumnIndex(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(getColumnIndex(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(getColumnIndex(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(getColumnIndex(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(getColumnIndex(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(getColumnIndex(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(getColumnIndex(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(getColumnIndex(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(getColumnIndex(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(getColumnIndex(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(getColumnIndex(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(getColumnIndex(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return null;
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
        return metadata;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getObject(columnIndex, defaultClass);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(getColumnIndex(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return getColumnIndex(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        if (columnIndex == 1) {
            throw new SQLException("INDEX column cannot be get as big decimal");
        }
        return (BigDecimal) getValueAsObject(columnIndex, BigDecimal.class, null);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(getColumnIndex(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return pos == -1;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return pos >= length;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return pos == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        return pos == length - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        pos = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        pos = length;
    }

    @Override
    public boolean first() throws SQLException {
        if (length > 0) {
            pos = 0;
            return true;
        }
        return false;
    }

    @Override
    public boolean last() throws SQLException {
        if (length > 0) {
            pos = length - 1;
            return true;
        }
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        if (isBeforeFirst() || isAfterLast()) {
            return 0;
        }
        return pos + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        pos = row - 1;
        return !(isAfterLast() || isBeforeFirst());
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return absolute((pos + 1) + rows);
    }

    @Override
    public boolean previous() throws SQLException {
        return absolute(pos);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (!(direction == FETCH_FORWARD || direction == FETCH_REVERSE || direction == FETCH_UNKNOWN)) {
            throw new SQLException("Invalid fetch direction: " + direction + ". Should be one of [ResultSet.FETCH_FORWARD, ResultSet.FETCH_REVERSE, ResultSet.FETCH_UNKNOWN]");
        }
        this.fetchDirection = direction;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // ignored as we fetched array already
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
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

    private void throwReadOnlyException() throws SQLException {
        throw new SQLException("ResultSet is read-only");
    }

    @Override
    public void insertRow() throws SQLException {
        throwReadOnlyException();
    }

    @Override
    public void updateRow() throws SQLException {
        throwReadOnlyException();
    }

    @Override
    public void deleteRow() throws SQLException {
        throwReadOnlyException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("refreshRow is not supported on ResultSet produced from Array object");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {

    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throwReadOnlyException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throwReadOnlyException();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return null; // null as it is produced from an Array object
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        Class<?> type = map.get(componentDataType.getName());
        if (type == null) {
            SQLType sqlType = JdbcUtils.CLICKHOUSE_TO_SQL_TYPE_MAP.get(componentDataType);
            if (sqlType != null) {
                type = map.get(sqlType.getName());
            }

            if (type == null) {
                // try to find by alias
                for (String alias : componentDataType.getAliases()) {
                    type = map.get(alias);
                    if (type != null) {
                        break;
                    }
                }
            }

            if (type == null) {
                type = defaultClass;
            }
        }

        return getObject(columnIndex, type);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(getColumnIndex(columnLabel), map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        if (columnIndex == 1) {
            throw new SQLException("INDEX column cannot be get as date");
        }
        return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(getColumnIndex(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        if (columnIndex == 1) {
            throw new SQLException("INDEX column cannot be get as time");
        }
        return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(getColumnIndex(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return (Timestamp) getValueAsObject(columnIndex, Timestamp.class, null);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(getColumnIndex(columnLabel), cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return (URL) getValueAsObject(columnIndex, URL.class, null);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(getColumnIndex(columnLabel));
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
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {

    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
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
        return null;
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return "";
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return "";
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return null;
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
        checkColumnIndex(columnIndex);
        checkRowPosition();
        if (columnIndex == 1) {
            if (Number.class.isAssignableFrom(type)) {
                return (T) pos;
            } else if (String.class.isAssignableFrom(type)) {
                return (T) String.valueOf(pos);
            } else {
                throw new SQLException("INDEX column cannot be converted to non-number value");
            }
        }

        return (T) getValueAsObject(columnIndex, type, null);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(getColumnIndex(columnLabel), type);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
