package com.clickhouse.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValue;

@Deprecated
public class ClickHouseResultSet extends AbstractResultSet {
    private ClickHouseRecord currentRow;
    private Iterator<ClickHouseRecord> rowCursor;
    private int rowNumber;
    private int lastReadColumn; // 1-based

    protected final String database;
    protected final String table;
    protected final ClickHouseStatement statement;
    protected final ClickHouseResponse response;

    protected final ClickHouseConfig config;
    protected final boolean wrapObject;
    protected final List<ClickHouseColumn> columns;
    protected final Calendar defaultCalendar;
    protected final int maxRows;
    protected final boolean nullAsDefault;
    protected final ClickHouseResultSetMetaData metaData;

    protected final JdbcTypeMapping mapper;
    protected final Map<String, Class<?>> defaultTypeMap;

    // only for testing purpose
    ClickHouseResultSet(String database, String table, ClickHouseResponse response) {
        this.database = database;
        this.table = table;
        this.statement = null;
        this.response = response;

        this.config = null;
        this.wrapObject = false;
        this.defaultCalendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

        this.mapper = JdbcTypeMapping.getDefaultMapping();
        this.defaultTypeMap = Collections.emptyMap();
        this.currentRow = null;
        try {
            this.columns = response.getColumns();
            this.metaData = new ClickHouseResultSetMetaData(new JdbcConfig(), database, table, columns, this.mapper,
                    defaultTypeMap);

            this.rowCursor = response.records().iterator();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        this.rowNumber = 0; // before the first row
        this.lastReadColumn = 0;

        this.maxRows = 0;
        this.nullAsDefault = false;
    }

    public ClickHouseResultSet(String database, String table, ClickHouseStatement statement,
            ClickHouseResponse response) throws SQLException {
        if (database == null || table == null || statement == null || response == null) {
            throw new IllegalArgumentException("Non-null database, table, statement, and response are required");
        }

        this.database = database;
        this.table = table;
        this.statement = statement;
        this.response = response;

        ClickHouseConnection conn = statement.getConnection();
        this.config = statement.getConfig();
        this.wrapObject = statement.getConnection().getJdbcConfig().useWrapperObject();
        this.defaultCalendar = conn.getDefaultCalendar();

        OutputStream output = statement.getMirroredOutput();
        if (output != null) {
            try {
                response.getInputStream().setCopyToTarget(output);
            } catch (IOException e) {
                throw SqlExceptionUtils.clientError(e);
            }
        }

        this.mapper = statement.getConnection().getJdbcTypeMapping();
        Map<String, Class<?>> typeMap = conn.getTypeMap();
        this.defaultTypeMap = typeMap != null && !typeMap.isEmpty() ? Collections.unmodifiableMap(typeMap)
                : Collections.emptyMap();
        this.currentRow = null;
        try {
            this.columns = response.getColumns();
            this.metaData = new ClickHouseResultSetMetaData(conn.getJdbcConfig(), database, table, columns, this.mapper,
                    defaultTypeMap);

            this.rowCursor = response.records().iterator();
        } catch (Exception e) {
            throw SqlExceptionUtils.handle(e);
        }

        this.rowNumber = 0; // before the first row
        this.lastReadColumn = 0;

        this.maxRows = statement.getMaxRows();
        this.nullAsDefault = statement.getNullAsDefault() > 1;
    }

    protected void ensureRead(int columnIndex) throws SQLException {
        ensureOpen();

        if (currentRow == null) {
            throw new SQLException("No data available for reading", SqlExceptionUtils.SQL_STATE_NO_DATA);
        } else if (columnIndex < 1 || columnIndex > columns.size()) {
            throw SqlExceptionUtils.clientError(ClickHouseUtils
                    .format("Column index must between 1 and %d but we got %d", columns.size() + 1, columnIndex));
        }
    }

    // this method is mocked in a test, do not make it final :-)
    protected List<ClickHouseColumn> getColumns() {
        return metaData.getColumns();
    }

    protected ClickHouseValue getValue(int columnIndex) throws SQLException {
        ensureRead(columnIndex);

        ClickHouseValue v = currentRow.getValue(columnIndex - 1);
        if (nullAsDefault && v.isNullOrEmpty()) {
            v.resetToDefault();
        }
        lastReadColumn = columnIndex;
        return v;
    }

    /**
     * Check if there is another row.
     *
     * @return {@code true} if this result set has another row after the current
     *         cursor position, {@code false} else
     * @throws SQLException if something goes wrong
     */
    protected boolean hasNext() throws SQLException {
        try {
            return (maxRows == 0 || rowNumber < maxRows) && rowCursor.hasNext();
        } catch (Exception e) {
            throw SqlExceptionUtils.handle(e);
        }
    }

    public BigInteger getBigInteger(int columnIndex) throws SQLException {
        return getValue(columnIndex).asBigInteger();
    }

    public BigInteger getBigInteger(String columnLabel) throws SQLException {
        return getValue(findColumn(columnLabel)).asBigInteger();
    }

    public String[] getColumnNames() {
        String[] columnNames = new String[columns.size()];
        int index = 0;
        for (ClickHouseColumn c : getColumns()) {
            columnNames[index++] = c.getColumnName();
        }
        return columnNames;
    }

    @Override
    public void close() throws SQLException {
        this.response.close();
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        ensureOpen();

        if (columnLabel == null || columnLabel.isEmpty()) {
            throw SqlExceptionUtils.clientError("Non-empty column label is required");
        }

        int index = 0;
        for (ClickHouseColumn c : columns) {
            index++;
            if (columnLabel.equalsIgnoreCase(c.getColumnName())) {
                return index;
            }
        }

        throw SqlExceptionUtils.clientError(
                ClickHouseUtils.format("Column [%s] does not exist in %d columns", columnLabel, columns.size()));
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return new ClickHouseArray(this, columnIndex);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return new ClickHouseArray(this, findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        ClickHouseValue v = getValue(columnIndex);
        return v.isNullOrEmpty() ? null : new ByteArrayInputStream(v.asBinary(StandardCharsets.US_ASCII));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getValue(columnIndex).asBigDecimal();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getValue(findColumn(columnLabel)).asBigDecimal();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getValue(columnIndex).asBigDecimal(scale);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getValue(findColumn(columnLabel)).asBigDecimal(scale);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        ClickHouseValue v = getValue(columnIndex);
        return v.isNullOrEmpty() ? null : new ByteArrayInputStream(v.asBinary());
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return getValue(columnIndex).asBoolean();
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getValue(findColumn(columnLabel)).asBoolean();
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return getValue(columnIndex).asByte();
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getValue(findColumn(columnLabel)).asByte();
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return getValue(columnIndex).asBinary();
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getValue(findColumn(columnLabel)).asBinary();
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        ClickHouseValue v = getValue(columnIndex);
        return v.isNullOrEmpty() ? null : new StringReader(v.asString());
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    @Override
    public String getCursorName() throws SQLException {
        ensureOpen();

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(columnIndex, null);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel), null);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        ClickHouseValue value = getValue(columnIndex);
        if (value.isNullOrEmpty()) {
            return null;
        }

        LocalDate d = value.asDate();
        Calendar c = (Calendar) (cal != null ? cal : defaultCalendar).clone();
        c.clear();
        c.set(d.getYear(), d.getMonthValue() - 1, d.getDayOfMonth(), 0, 0, 0);
        return new Date(c.getTimeInMillis());
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getValue(columnIndex).asDouble();
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getValue(findColumn(columnLabel)).asDouble();
    }

    @Override
    public int getFetchSize() throws SQLException {
        ensureOpen();

        return statement != null ? statement.getFetchSize() : 0;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return getValue(columnIndex).asFloat();
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getValue(findColumn(columnLabel)).asFloat();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return getValue(columnIndex).asInteger();
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getValue(findColumn(columnLabel)).asInteger();
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getValue(columnIndex).asLong();
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getValue(findColumn(columnLabel)).asLong();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ensureOpen();

        return metaData;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getValue(columnIndex).asString();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getValue(findColumn(columnLabel)).asString();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getObject(columnIndex, defaultTypeMap);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel), defaultTypeMap);
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        if (map == null) {
            map = defaultTypeMap;
        }

        ClickHouseValue v = getValue(columnIndex);
        ClickHouseColumn c = columns.get(columnIndex - 1);

        Class<?> javaType = null;
        if (!map.isEmpty() && (javaType = map.get(c.getOriginalTypeName())) == null) {
            javaType = map.get(c.getDataType().name());
        }

        Object value;
        if (!wrapObject) {
            value = javaType != null ? v.asObject(javaType) : v.asObject();
        } else if (c.isArray()) {
            value = new ClickHouseArray(this, columnIndex);
        } else if (c.isTuple() || c.isNested() || c.isMap()) {
            value = new ClickHouseStruct(c.getDataType().name(), v.asArray());
        } else {
            value = javaType != null ? v.asObject(javaType) : v.asObject();
        }

        return value;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return getValue(columnIndex).asObject(type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getValue(findColumn(columnLabel)).asObject(type);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getRef not implemented");
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return getRef(findColumn(columnLabel));
    }

    @Override
    public int getRow() throws SQLException {
        ensureOpen();

        return rowNumber;
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getRowId not implemented");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return getRowId(findColumn(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getSQLXML not implemented");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return getValue(columnIndex).asShort();
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getValue(findColumn(columnLabel)).asShort();
    }

    @Override
    public Statement getStatement() throws SQLException {
        ensureOpen();

        return statement;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return getValue(columnIndex).asString();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getValue(findColumn(columnLabel)).asString();
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getTime(columnIndex, null);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel), null);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        ClickHouseValue value = getValue(columnIndex);
        if (value.isNullOrEmpty()) {
            return null;
        }

        // unfortunately java.sql.Time does not support fractional seconds
        LocalTime lt = value.asTime();

        Calendar c = (Calendar) (cal != null ? cal : defaultCalendar).clone();
        c.clear();
        c.set(1970, 0, 1, lt.getHour(), lt.getMinute(), lt.getSecond());
        return new Time(c.getTimeInMillis());
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(columnIndex, null);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel), null);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        ClickHouseValue value = getValue(columnIndex);
        if (value.isNullOrEmpty()) {
            return null;
        }

        ClickHouseColumn column = columns.get(columnIndex - 1);
        TimeZone tz = column.getTimeZone();
        LocalDateTime dt = tz == null ? value.asDateTime(column.getScale())
                : value.asOffsetDateTime(column.getScale()).toLocalDateTime();

        Calendar c = (Calendar) (cal != null ? cal : defaultCalendar).clone();
        c.set(dt.getYear(), dt.getMonthValue() - 1, dt.getDayOfMonth(), dt.getHour(), dt.getMinute(),
                dt.getSecond());
        Timestamp timestamp = new Timestamp(c.getTimeInMillis());
        timestamp.setNanos(dt.getNano());

        return timestamp;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        try {
            return new URL(getString(columnIndex));
        } catch (MalformedURLException e) {
            throw SqlExceptionUtils.clientError(e);
        }
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        try {
            return new URL(getString(columnLabel));
        } catch (MalformedURLException e) {
            throw SqlExceptionUtils.clientError(e);
        }
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        ClickHouseValue v = getValue(columnIndex);
        return v.isNullOrEmpty() ? null : new ByteArrayInputStream(v.asBinary(StandardCharsets.UTF_8));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        ensureOpen();

        return currentRow == null && !hasNext();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        ensureOpen();

        return getRow() == 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return response.isClosed();
    }

    @Override
    public boolean isFirst() throws SQLException {
        ensureOpen();

        return getRow() == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        ensureOpen();

        return currentRow != null && !hasNext();
    }

    @Override
    public boolean next() throws SQLException {
        ensureOpen();

        lastReadColumn = 0;
        boolean hasNext = true;
        if (hasNext()) {
            try {
                currentRow = rowCursor.next();
            } catch (UncheckedIOException e) {
                throw SqlExceptionUtils.handle(e);
            }
            rowNumber++;
        } else {
            currentRow = null;
            hasNext = false;
        }
        return hasNext;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        ensureOpen();
    }

    @Override
    public boolean wasNull() throws SQLException {
        ensureOpen();

        try {
            return currentRow != null && lastReadColumn > 0 && getColumns().get(lastReadColumn - 1).isNullable()
                    && currentRow.getValue(lastReadColumn - 1).isNullOrEmpty();
        } catch (Exception e) {
            throw SqlExceptionUtils.handle(e);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface == ClickHouseResponse.class || iface == ClickHouseRecord.class || super.isWrapperFor(iface);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface == ClickHouseResponse.class) {
            return iface.cast(response);
        } else if (iface == ClickHouseRecord.class) {
            return iface.cast(currentRow);
        } else {
            return super.unwrap(iface);
        }
    }
}
