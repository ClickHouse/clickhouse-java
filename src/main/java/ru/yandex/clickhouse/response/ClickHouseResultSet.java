package ru.yandex.clickhouse.response;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.UUID;

import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.except.ClickHouseExceptionSpecifier;
import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import static ru.yandex.clickhouse.response.ByteFragmentUtils.parseArray;


public class ClickHouseResultSet extends AbstractResultSet {

    private final static long[] EMPTY_LONG_ARRAY = new long[0];
    private static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final String DATE_PATTERN = "yyyy-MM-dd";

    private final TimeZone dateTimeTimeZone;
    private final TimeZone dateTimeZone;
    private final SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_PATTERN);
    private final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_PATTERN);

    private final StreamSplitter bis;

    private final String db;
    private final String table;

    private List<ClickHouseColumnInfo> columns;

    private int maxRows;

    // current line
    protected ByteFragment[] values;
    // 1-based
    private int lastReadColumn;

    // next line
    protected ByteFragment nextLine;

    // total lines
    private ByteFragment totalLine;

    // row counter
    protected int rowNumber;

    // statement result set belongs to
    private final ClickHouseStatement statement;

    private final ClickHouseProperties properties;

    private boolean usesWithTotals;

    // NOTE this can't be used for `isLast` impl because
    // it does not do prefetch. It is effectively a witness
    // to the fact that rs.next() returned false.
    private boolean lastReached = false;
    
    private boolean isAfterLastReached = false;

    public ClickHouseResultSet(InputStream is, int bufferSize, String db, String table,
        boolean usesWithTotals, ClickHouseStatement statement, TimeZone timeZone,
        ClickHouseProperties properties) throws IOException
    {
        this.db = db;
        this.table = table;
        this.statement = statement;
        this.properties = properties;
        this.usesWithTotals = usesWithTotals;
        this.dateTimeTimeZone = timeZone;
        this.dateTimeZone = properties.isUseServerTimeZoneForDates()
            ? timeZone
            : TimeZone.getDefault();
        dateTimeFormat.setTimeZone(dateTimeTimeZone);
        dateFormat.setTimeZone(dateTimeZone);
        bis = new StreamSplitter(is, (byte) 0x0A, bufferSize);  ///   \n
        ByteFragment headerFragment = bis.next();
        if (headerFragment == null) {
            throw new IllegalArgumentException("ClickHouse response without column names");
        }
        String header = headerFragment.asString(true);
        if (header.startsWith("Code: ") && !header.contains("\t")) {
            is.close();
            throw new IOException("ClickHouse error: " + header);
        }
        String[] cols = toStringArray(headerFragment);
        ByteFragment typesFragment = bis.next();
        if (typesFragment == null) {
            throw new IllegalArgumentException("ClickHouse response without column types");
        }
        String[] types = toStringArray(typesFragment);
        columns = new ArrayList<ClickHouseColumnInfo>(cols.length);
        for (int i = 0; i < cols.length; i++) {
            columns.add(ClickHouseColumnInfo.parse(types[i], cols[i]));
        }
    }

    private static String[] toStringArray(ByteFragment headerFragment) {
        ByteFragment[] split = headerFragment.split((byte) 0x09);
        String[] c = new String[split.length];
        for (int i = 0; i < split.length; i++) {
            String name = split[i].asString(true);
            c[i] = name;
        }
        return c;
    }

    public boolean hasNext() throws SQLException {
        if (nextLine == null && !lastReached) {
            try {
                nextLine = bis.next();

                if (nextLine == null || (maxRows != 0 && rowNumber >= maxRows) || (usesWithTotals && nextLine.length() == 0)) {
                    if (usesWithTotals) {
                        if (onTheSeparatorRow()) {
                            totalLine = bis.next();
                            endOfStream();
                        } // otherwise do not close the stream, it is single column or invalid result set case
                    } else {
                        endOfStream();
                    }
                }
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
        return nextLine != null;
    }

    @Override 
    public boolean isBeforeFirst() throws SQLException {
        return rowNumber == 0 && hasNext();
    }

    @Override 
    public boolean isAfterLast() throws SQLException {
        return isAfterLastReached;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return rowNumber == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        return !hasNext(); 
     // && !isAfterLastReached should be probably added, 
     // but it may brake compatibility with the previous implementation
    }

    private void endOfStream() throws IOException {
        bis.close();
        lastReached = true;
        nextLine = null;
    }

    @Override
    public boolean next() throws SQLException {
        if (hasNext()) {
            values = nextLine.split((byte) 0x09);
            checkValues(columns, values, nextLine);
            nextLine = null;
            rowNumber += 1;
            return true;
        }
        isAfterLastReached = true;
        return false;
    }

    private boolean onTheSeparatorRow() throws IOException {
        // test bis vs "\n???\nEOF" pattern if not then rest to current position
        bis.mark();
        boolean onSeparatorRow = bis.next() !=null && bis.next() == null;
        bis.reset();

        return onSeparatorRow;
    }

    private void checkValues(List<ClickHouseColumnInfo> columns, ByteFragment[] values,
        ByteFragment fragment) throws SQLException
    {
        if (columns.size() != values.length) {
            throw ClickHouseExceptionSpecifier.specify(fragment.asString());
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            bis.close();
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        try {
            return bis.isClosed();
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    public void getTotals() throws SQLException {
        if (!usesWithTotals) {
            throw new IllegalStateException("Cannot get totals when totals are not being used.");
        }

        nextLine = totalLine;

        this.next();
    }

    List<ClickHouseColumnInfo> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new ClickHouseResultSetMetaData(this);
    }


    /////////////////////////////////////////////////////////


    @Override
    public boolean wasNull() throws SQLException {
        if (lastReadColumn == 0) {
            throw new IllegalStateException("You should get something before check nullability");
        }
        return getValue(lastReadColumn).isNull();
    }

    @Override
    public int getInt(String column) {
        return getInt(asColNum(column));
    }

    @Override
    public boolean getBoolean(String column) {
        return getBoolean(asColNum(column));
    }

    @Override
    public long getLong(String column) {
        return getLong(asColNum(column));
    }

    @Override
    public String getString(String column) {
        return getString(asColNum(column));
    }

    @Override
    public byte[] getBytes(String column) {
        return getBytes(asColNum(column));
    }

    public Long getTimestampAsLong(String column) {
        return getTimestampAsLong(asColNum(column));
    }

    @Override
    public Timestamp getTimestamp(String column) throws SQLException {
        Long value = getTimestampAsLong(column);
        return value == null ? null : new Timestamp(value);
    }

    @Override
    public Timestamp getTimestamp(String column, Calendar cal) throws SQLException {
        Long value = getTimestampAsLong(asColNum(column), cal.getTimeZone());
        return value == null ? null : new Timestamp(value);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        Long value = getTimestampAsLong(columnIndex, cal.getTimeZone());
        return value == null ? null : new Timestamp(value);
    }

    @Override
    public short getShort(String column) {
        return getShort(asColNum(column));
    }

    @Override
    public byte getByte(String column) {
        return getByte(asColNum(column));
    }

    @Override
    public long[] getLongArray(String column) {
        return getLongArray(asColNum(column));
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return getArray(columns.get(columnIndex - 1), getValue(columnIndex));
    }

    public Array getArray(ClickHouseColumnInfo colInfo, ByteFragment value) throws SQLException {
        if (colInfo.getClickHouseDataType() != ClickHouseDataType.Array) {
            throw new SQLException("Column not an array");
        }

        final Object array;
        switch (colInfo.getArrayBaseType()) {
            case Date :
                array = parseArray(
                    value,
                    colInfo.getArrayBaseType().getJavaClass(),
                    properties.isUseObjectsInArrays(),
                    dateFormat,
                    colInfo.getArrayLevel()
                );
                break;
            case DateTime :
                TimeZone timeZone = colInfo.getTimeZone() != null
                    ? colInfo.getTimeZone()
                    : dateTimeTimeZone;
                dateTimeFormat.setTimeZone(timeZone);
                array = parseArray(
                    value,
                    colInfo.getArrayBaseType().getJavaClass(),
                    properties.isUseObjectsInArrays(),
                    dateTimeFormat,
                    colInfo.getArrayLevel()
                );
                break;
            default :
                array = parseArray(
                    value,
                    colInfo.getArrayBaseType().getJavaClass(),
                    properties.isUseObjectsInArrays(),
                    colInfo.getArrayLevel()
                );
                break;
        }

        return new ClickHouseArray(colInfo.getArrayBaseType(), array);
    }

    @Override
    public Array getArray(String column) throws SQLException {
        return getArray(asColNum(column));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(asColNum(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(asColNum(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(asColNum(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(asColNum(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(asColNum(columnLabel));
    }

    @Override
    public String getString(int colNum) {
        return getString(getValue(colNum));
    }

    private String getString(ByteFragment value) {
        return toString(value);
    }

    @Override
    public int getInt(int colNum) {
        return getInt(getValue(colNum));
    }

    private int getInt(ByteFragment value) {
        return ByteFragmentUtils.parseInt(value);
    }

    @Override
    public boolean getBoolean(int colNum) {
        return toBoolean(getValue(colNum));
    }

    @Override
    public long getLong(int colNum) {
        return getLong(getValue(colNum));
    }

    public long getLong(ByteFragment value) {
        return ByteFragmentUtils.parseLong(value);
    }

    @Override
    public byte[] getBytes(int colNum) {
        return toBytes(getValue(colNum));
    }

    public Long getTimestampAsLong(int colNum) {
        return getTimestampAsLong(columns.get(colNum - 1), getValue(colNum));
    }

    public Long getTimestampAsLong(ClickHouseColumnInfo info, ByteFragment value) {
        TimeZone timeZone = info.getTimeZone() != null
            ? info.getTimeZone()
            : dateTimeTimeZone;
        return toTimestamp(value, timeZone);
    }

    public Long getTimestampAsLong(int colNum, TimeZone tz) {
        return toTimestamp(getValue(colNum), tz);
    }

    @Override
    public Timestamp getTimestamp(int colNum) throws SQLException {
        return getTimestamp(columns.get(colNum - 1), getValue(colNum));
    }

    public Timestamp getTimestamp(ClickHouseColumnInfo info, ByteFragment value) throws SQLException {
        Long value_ = getTimestampAsLong(info, value);
        return value_ == null ? null : new Timestamp(value_.longValue());
    }

    @Override
    public short getShort(int colNum) {
        return toShort(getValue(colNum));
    }

    @Override
    public byte getByte(int colNum) {
        return toByte(getValue(colNum));
    }

    public long[] getLongArray(int colNum) {
        return toLongArray(getValue(colNum));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return (float) getDouble(columnIndex);
    }

    private float getFloat(ByteFragment value) throws SQLException {
        return (float) getDouble(value);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getDouble(getValue(columnIndex));
    }

    private double getDouble(ByteFragment value) throws SQLException {
        String string = getString(value);
        if (string == null){
            return 0;
        } else if (string.equals("nan")) {
            return Double.NaN;
        } else if (string.equals("+inf") || string.equals("inf")) {
            return Double.POSITIVE_INFINITY;
        } else if (string.equals("-inf")) {
            return Double.NEGATIVE_INFINITY;
        } else {
            return Double.parseDouble(string);
        }
    }

    @Override
    public Statement getStatement() {
        return statement;
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(getValue(columnIndex));
    }

    private Date getDate(ByteFragment value) throws SQLException {
        // date is passed as a string from clickhouse
        if (value.isNull() || value.asString().equals("0000-00-00")) {
            return null;
        }
        try {
            return new Date(dateFormat.parse(value.asString()).getTime());
        } catch (ParseException e) {
            return null;
        }
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Timestamp ts = getTimestamp(columnIndex);
        if (ts == null) {
            return null;
        }

        return new Time(ts.getTime());
    }

    public Struct getStruct(int columnIndex) {
        //todo
        return null;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getObject(columns.get(columnIndex -1), getValue(columnIndex));
    }

    public Object getObject(ClickHouseColumnInfo chci, ByteFragment value) throws SQLException {
        try {
            if (value.isNull()) {
                return null;
            }
            ClickHouseDataType chType = chci.getClickHouseDataType();
            int type = chType.getSqlType();
            switch (type) {
                case Types.BIGINT:
                    if (!chType.isSigned()){
                        String stringVal = getString(value);
                        return new BigInteger(stringVal);
                    }
                    return getLong(value);
                case Types.INTEGER:
                    if (!chType.isSigned()){
                        return getLong(value);
                    }
                    return getInt(value);
                case Types.TINYINT:
                case Types.SMALLINT:
                    return getInt(value);
                case Types.VARCHAR:     return getString(value);
                case Types.FLOAT:       return getFloat(value);
                case Types.DOUBLE:      return getDouble(value);
                case Types.DATE:        return getDate(value);
                case Types.TIMESTAMP:   return getTimestamp(chci, value);
                case Types.BLOB:        return getString(value);
                case Types.ARRAY:       return getArray(chci, value);
                case Types.DECIMAL:     return getBigDecimal(value);
            }
            switch (chType) {
                case UUID :
                    return getObject(value, UUID.class);
                default :
                    return getString(value);
            }
        } catch (Exception e) {
            throw new RuntimeException("Parse exception: " + value.toString(), e);
        }
    }

    /////////////////////////////////////////////////////////

    private static byte toByte(ByteFragment value) {
        if (value.isNull()) {
            return 0;
        }
        return Byte.parseByte(value.asString());
    }

    private static short toShort(ByteFragment value) {
        if (value.isNull()) {
            return 0;
        }
        return Short.parseShort(value.asString());
    }

    private static boolean toBoolean(ByteFragment value) {
        if (value.isNull()) {
            return false;
        }
        return "1".equals(value.asString());    // 1 or 0 there
    }

    private static byte[] toBytes(ByteFragment value) {
        if (value.isNull()) {
            return null;
        }
        return value.unescape();
    }

    private static String toString(ByteFragment value) {
        return value.asString(true);
    }

    static long[] toLongArray(ByteFragment value) {
        if (value.isNull()) {
            return null;
        }
        if (value.charAt(0) != '[' || value.charAt(value.length()-1) != ']') {
            throw new IllegalArgumentException("not an array: "+value);
        }
        if (value.length() == 2) {
            return EMPTY_LONG_ARRAY;
        }
        ByteFragment trim = value.subseq(1, value.length() - 2);
        ByteFragment[] values = trim.split((byte) ',');
        long[] result = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = ByteFragmentUtils.parseLong(values[i]);
        }
        return result;
    }

    private Long toTimestamp(ByteFragment value, TimeZone timeZone) {
        if (value.isNull() || value.asString().equals("0000-00-00 00:00:00")) {
            return null;
        }
        try {
            dateTimeFormat.setTimeZone(timeZone);
            return dateTimeFormat.parse(value.asString()).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    //////

    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getRow() throws SQLException {
        return rowNumber;
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    /////

    // 1-based index in column list
    private int asColNum(String column) {
        for (int i = 0; i < columns.size(); i++) {
            if (column.equals(columns.get(i).getColumnName())) {
                return i+1;
            }
        }
        // TODO Java8
        throw new RuntimeException("no column " + column + " in columns list " + getColumnNames());
    }

    private ByteFragment getValue(int colNum) {
        lastReadColumn = colNum;
        return values[colNum - 1];
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return getObject(getValue(columnIndex), type);
    }

    public <T> T getObject(ByteFragment value, Class<T> type) throws SQLException {
        if(type.equals(UUID.class)) {
            return (T) UUID.fromString(getString(value));
        } else {
            throw new SQLException("Not implemented for type=" + type.toString());
        }
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(asColNum(columnLabel), type);
    }

    public ByteFragment[] getValues() {
        return values;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel)  {
        return getBigDecimal(asColNum(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex)  {
        return getBigDecimal(getValue(columnIndex));
    }

    public BigDecimal getBigDecimal(ByteFragment value)  {
        String string = getString(value);
        if (string == null) {
            return null;
        }
        return new BigDecimal(string);
    }


    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale)  {
        return getBigDecimal(asColNum(columnLabel), scale);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale)  {
        String string = getString(columnIndex);
        if (string == null) {
            return null;
        }
        BigDecimal result = new BigDecimal(string);
        return result.setScale(scale, RoundingMode.HALF_UP);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        // ignore perfomance hint
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // ignore perfomance hint
    }


    @Override
    public String toString() {
        return "ClickHouseResultSet{" +
            "sdf=" + dateTimeFormat +
            ", dateFormat=" + dateFormat +
            ", bis=" + bis +
            ", db='" + db + '\'' +
            ", table='" + table + '\'' +
            ", columns=" + getColumnNames() +
            ", maxRows=" + maxRows +
            ", values=" + Arrays.toString(values) +
            ", lastReadColumn=" + lastReadColumn +
            ", nextLine=" + nextLine +
            ", rowNumber=" + rowNumber +
            ", statement=" + statement +
            '}';
    }

    private String getColumnNames() {
        StringBuilder sb = new StringBuilder();
        for (ClickHouseColumnInfo info : columns) {
            sb.append(info.getColumnName()).append(' ');
        }
        return sb.substring(0, sb.length() - 1);
    }

}
