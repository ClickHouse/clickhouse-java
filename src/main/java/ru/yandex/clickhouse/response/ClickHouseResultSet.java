package ru.yandex.clickhouse.response;

import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.except.ClickHouseExceptionSpecifier;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.TypeUtils;
import ru.yandex.clickhouse.util.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static ru.yandex.clickhouse.response.ByteFragmentUtils.parseArray;


public class ClickHouseResultSet extends AbstractResultSet {
    private final static Pattern pattern = Pattern.compile("(select(.*?))?from\\s+(.*)");
    private final static long[] EMPTY_LONG_ARRAY = new long[]{};

    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd"); //

    private final StreamSplitter bis;

    private final String[] columnNames;
    private final String[] types;
    private Map<String, String> tableNames;      // column name -> table name
    private Map<String, Integer> columnIndexes;  // column name -> 1-based index
    private boolean usesWithTotals;

    private int maxRows;

    // current line
    private ByteFragment[] values;
    // 1-based
    private int lastReadColumn;

    // next line
    private ByteFragment nextLine;

    // total lines
    private ByteFragment totalLine;

    // row counter
    private int rowNumber;

    // statement result set belongs to
    private final ClickHouseStatement statement;
    private final ClickHouseProperties properties;


    // NOTE this can't be used for `isLast` impl because
    // it does not do prefetch. It is effectively a witness
    // to the fact that rs.next() returned false.
    private boolean lastReached = false;

    private ClickHouseResultSet(InputStream is,
                                int bufferSize,
                                ClickHouseStatement statement,
                                TimeZone timezone,
                                ClickHouseProperties properties) throws IOException {

        this.statement = statement;
        this.properties = properties;

        initTimeZone(timezone);

        this.bis = new StreamSplitter(is, (byte) 0x0A, bufferSize);  ///   \n
        ByteFragment headerFragment = bis.next();
        if (headerFragment == null) {
            throw new IllegalArgumentException("ClickHouse response without column names");
        }
        String header = headerFragment.asString(true);
        if (header.startsWith("Code: ") && !header.contains("\t")) {
            is.close();
            throw new IOException("ClickHouse error: " + header);
        }

        this.columnNames = toStringArray(headerFragment);
        this.columnIndexes = new HashMap<String, Integer>(columnNames.length);
        for (int i = 0; i < columnNames.length; i++) {
            String s = columnNames[i];
            columnIndexes.put(s, i + 1);
        }

        ByteFragment typesFragment = bis.next();
        if (typesFragment == null) {
            throw new IllegalArgumentException("ClickHouse response without column types");
        }
        this.types = toStringArray(typesFragment);
    }

    public ClickHouseResultSet(InputStream is,
                               int bufferSize,
                               String sqlQuery,
                               ClickHouseStatement statement,
                               TimeZone timezone,
                               ClickHouseProperties properties) throws IOException {

        this(is, bufferSize, statement, timezone, properties);

        String preparedSqlQuery = Utils.retainUnquoted(sqlQuery.trim(), '\'').toLowerCase();
        this.usesWithTotals = isWithTotals(preparedSqlQuery);
        this.tableNames = getTableNames(preparedSqlQuery, columnNames);
    }


    ClickHouseResultSet(InputStream is,
                        int bufferSize,
                        String database,
                        String table,
                        boolean usesWithTotals,
                        ClickHouseStatement statement,
                        TimeZone timezone, ClickHouseProperties properties) throws IOException {

        this(is, bufferSize, statement, timezone, properties);
        this.usesWithTotals = usesWithTotals;

        this.tableNames = new HashMap<String, String>(columnNames.length);
        String catalog = database + "." + table;
        for (int i = 0; i < columnNames.length; i++) {
            String s = columnNames[i];
            tableNames.put(s, catalog);
        }
    }

    private void initTimeZone(TimeZone timeZone) {
        sdf.setTimeZone(timeZone);
        if (properties.isUseServerTimeZoneForDates()) {
            dateFormat.setTimeZone(timeZone);
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

    private void endOfStream() throws IOException {
        bis.close();
        lastReached = true;
        nextLine = null;
    }

    @Override
    public boolean next() throws SQLException {
        if (hasNext()) {
            values = nextLine.split((byte) 0x09);
            checkValues(columnNames, values, nextLine);
            nextLine = null;
            rowNumber += 1;
            return true;
        } else return false;
    }

    private boolean onTheSeparatorRow() throws IOException {
        // test bis vs "\n???\nEOF" pattern if not then rest to current position
        bis.mark();
        boolean onSeparatorRow = bis.next() != null && bis.next() == null;
        bis.reset();

        return onSeparatorRow;
    }

    private void checkValues(String[] columns, ByteFragment[] values, ByteFragment fragment) throws SQLException {
        if (columns.length != values.length) {
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

    public void getTotals() throws SQLException {
        if (!usesWithTotals)
            throw new IllegalStateException("Cannot get totals when totals are not being used.");

        nextLine = totalLine;

        this.next();
    }

    /////////////////////////////////////////////////////////

    String[] getTypes() {
        return types;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new ClickHouseResultSetMetaData(this);
    }


    /////////////////////////////////////////////////////////


    @Override
    public boolean wasNull() throws SQLException {
        if (lastReadColumn == 0) throw new IllegalStateException("You should get something before check nullability");
        return getValue(lastReadColumn).isNull();
    }

    @Override
    public int getInt(String column) {
        return getInt(getColumnIndex(column));
    }

    @Override
    public boolean getBoolean(String column) {
        return getBoolean(getColumnIndex(column));
    }

    @Override
    public long getLong(String column) {
        return getLong(getColumnIndex(column));
    }

    @Override
    public String getString(String column) {
        return getString(getColumnIndex(column));
    }

    @Override
    public byte[] getBytes(String column) {
        return getBytes(getColumnIndex(column));
    }

    public Long getTimestampAsLong(String column) {
        return getTimestampAsLong(getColumnIndex(column));
    }

    @Override
    public Timestamp getTimestamp(String column) throws SQLException {
        Long value = getTimestampAsLong(column);
        return value == null ? null : new Timestamp(value);
    }

    @Override
    public short getShort(String column) {
        return getShort(getColumnIndex(column));
    }

    @Override
    public byte getByte(String column) {
        return getByte(getColumnIndex(column));
    }

    @Override
    public long[] getLongArray(String column) {
        return getLongArray(getColumnIndex(column));
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        if (TypeUtils.toSqlType(types[columnIndex - 1]) != Types.ARRAY) {
            throw new SQLException("Not an array");
        }

        String elementTypeName = TypeUtils.getArrayElementTypeName(types[columnIndex - 1]);
        int elementType = TypeUtils.toSqlType(elementTypeName);
        boolean isUnsigned = TypeUtils.isUnsigned(elementTypeName);

        final Object array;
        if (elementType == Types.DATE) {
            array = parseArray(
                    getValue(columnIndex),
                    TypeUtils.toClass(elementType, isUnsigned),
                    properties.isUseObjectsInArrays(),
                    dateFormat
            );
        } else if (elementType == Types.TIMESTAMP) {
            array = parseArray(
                    getValue(columnIndex),
                    TypeUtils.toClass(elementType, isUnsigned),
                    properties.isUseObjectsInArrays(),
                    sdf
            );
        } else {
            array = parseArray(
                    getValue(columnIndex),
                    TypeUtils.toClass(elementType, isUnsigned),
                    properties.isUseObjectsInArrays()
            );
        }

        return new ClickHouseArray(elementType, isUnsigned, array);
    }

    @Override
    public Array getArray(String column) throws SQLException {
        return getArray(getColumnIndex(column));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(getColumnIndex(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(getColumnIndex(columnLabel));
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
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(getColumnIndex(columnLabel));
    }

    /////////////////////////////////////////////////////////

    @Override
    public String getString(int colNum) {
        return toString(getValue(colNum));
    }


    @Override
    public int getInt(int colNum) {
        return ByteFragmentUtils.parseInt(getValue(colNum));
    }

    @Override
    public boolean getBoolean(int colNum) {
        return toBoolean(getValue(colNum));
    }

    @Override
    public long getLong(int colNum) {
        return ByteFragmentUtils.parseLong(getValue(colNum));
    }

    @Override
    public byte[] getBytes(int colNum) {
        return toBytes(getValue(colNum));
    }

    public Long getTimestampAsLong(int colNum) {
        return toTimestamp(getValue(colNum));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Long value = getTimestampAsLong(columnIndex);
        return value == null ? null : new Timestamp(value);
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

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        String string = getString(columnIndex);
        if (string == null) {
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
        // date is passed as a string from clickhouse
        ByteFragment value = getValue(columnIndex);
        if (value.isNull() || value.asString().equals("0000-00-00")) return null;
        try {
            return new Date(dateFormat.parse(value.asString()).getTime());
        } catch (ParseException e) {
            return null;
        }
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return new Time(getTimestamp(columnIndex).getTime());
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        try {
            if (getValue(columnIndex).isNull()) {
                return null;
            }

            String typeName = types[columnIndex - 1];
            int type = TypeUtils.toSqlType(typeName);
            switch (type) {
                case Types.BIGINT:
                    if (TypeUtils.isUnsigned(typeName)) {
                        String stringVal = getString(columnIndex);
                        return new BigInteger(stringVal);
                    }
                    return getLong(columnIndex);
                case Types.INTEGER:
                    if (TypeUtils.isUnsigned(typeName)){
                        return getLong(columnIndex);
                    }
                    return getInt(columnIndex);
                case Types.VARCHAR:     return getString(columnIndex);
                case Types.FLOAT:       return getFloat(columnIndex);
                case Types.DOUBLE:      return getDouble(columnIndex);
                case Types.DATE:        return getDate(columnIndex);
                case Types.TIMESTAMP:   return getTimestamp(columnIndex);
                case Types.BLOB:        return getString(columnIndex);
                case Types.ARRAY:       return getArray(columnIndex).getArray();
            }
            return getString(columnIndex);
        } catch (Exception e) {
            throw new RuntimeException("Parse exception: " + values[columnIndex - 1].toString(), e);
        }
    }

    /////////////////////////////////////////////////////////

    private static byte toByte(ByteFragment value) {
        return Byte.parseByte(value.asString());
    }

    private static short toShort(ByteFragment value) {
        if (value.isNull()) return 0;
        return Short.parseShort(value.asString());
    }

    private static boolean toBoolean(ByteFragment value) {
        if (value.isNull()) return false;
        return "1".equals(value.asString());    // 1 or 0 there
    }

    private static byte[] toBytes(ByteFragment value) {
        if (value.isNull()) return null;
        return value.unescape();
    }

    private static String toString(ByteFragment value) {
        return value.asString(true);
    }

    static long[] toLongArray(ByteFragment value) {
        if (value.isNull()) return null;
        if (value.charAt(0) != '[' || value.charAt(value.length() - 1) != ']') {
            throw new IllegalArgumentException("not an array: " + value);
        }
        if (value.length() == 2) return EMPTY_LONG_ARRAY;
        ByteFragment trim = value.subseq(1, value.length() - 2);
        ByteFragment[] values = trim.split((byte) ',');
        long[] result = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = ByteFragmentUtils.parseLong(values[i]);
        }
        return result;
    }

    private Long toTimestamp(ByteFragment value) {
        String str = value.isNull() ? null : value.asString();
        if (value.isNull() || "0000-00-00 00:00:00".equals(str)) return null;
        try {
            return sdf.parse(value.asString()).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getRow() throws SQLException {
        return rowNumber + 1;
    }

    public String getDatabaseName(int columnIndex) {
        if (columnNames.length <= columnIndex - 1)
            return "";

        String catalog = getCatalogByIndex(columnIndex);
        int index = catalog.indexOf('.');
        if (index == -1)
            return properties.getDatabase();
        else
            return catalog.substring(0, index).trim();
    }

    public String getTableName(int columnIndex) {
        if (columnNames.length <= columnIndex - 1)
            return "";

        String table = getCatalogByIndex(columnIndex);
        int index = table.indexOf('.');
        if (index == -1)
            return table;
        else
            return table.substring(index + 1).trim();
    }

    private String getCatalogByIndex(int columnIndex) {
        String columnName = columnNames[columnIndex - 1].toLowerCase();
        return tableNames.get(columnName);
    }

    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }


    private int getColumnIndex(String column) {
        Integer index = columnIndexes.get(column);
        if (index == null) {
            throw new RuntimeException("no column " + column + " in columnNames list " + Arrays.toString(getColumnNames()));
        } else {
            return index;
        }
    }

    private ByteFragment getValue(int colNum) {
        lastReadColumn = colNum;
        return values[colNum - 1];
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLException("Not implemented");
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(getColumnIndex(columnLabel), type);
    }

    public ByteFragment[] getValues() {
        return values;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) {
        return getBigDecimal(getColumnIndex(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) {
        String string = getString(columnIndex);
        if (string == null) {
            return null;
        }
        return new BigDecimal(string);
    }


    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) {
        return getBigDecimal(getColumnIndex(columnLabel), scale);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) {
        String string = getString(columnIndex);
        if (string == null) {
            return null;
        }
        BigDecimal result = new BigDecimal(string);
        return result.setScale(scale, RoundingMode.HALF_UP);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        // ignore performance hint
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // ignore performance hint
    }

    static Map<String, String> getTableNames(String query, String[] columnNames) {
        Map<String, String> tableNames = new HashMap<String, String>(columnNames.length);
        Matcher matcher = pattern.matcher(query);
        if (matcher.find()) {
            String params = matcher.group(2);
            if (params != null) {
                String[] param = params.split(",");
                if (param.length == 1 && "*".equals(param[0].trim())) {
                    String tableName = getTableName(query);
                    for (String column : columnNames)
                        tableNames.put(column.toLowerCase(), tableName);

                } else
                    return getTableNames(query, tableNames);
            }
        }
        return tableNames;
    }

    private static String getTableName(String query) {
        Matcher matcher = pattern.matcher(query);
        if (matcher.find()) {
            String anotherPart = matcher.group(3);
            if (anotherPart != null) {
                String candidate = extractCatalog(anotherPart);
                if (candidate == null)
                    return getTableName(anotherPart);
                else
                    return candidate;
            }
        }
        return null;
    }

    private static Map<String, String> getTableNames(String query, Map<String, String> tableNames) {
        Matcher matcher = pattern.matcher(query);
        while (matcher.find()) {

            String tableName = null;
            String anotherPart = matcher.group(3);
            if (anotherPart != null) {
                tableName = extractCatalog(anotherPart);
                getTableNames(query.substring(matcher.start(3) + 4), tableNames);
            }

            String params = matcher.group(2);
            if (params != null) {
                for (String param : params.split(",")) {
                    String paramName = getParamName(param);
                    if (!tableNames.containsKey(paramName))
                        tableNames.put(paramName, tableName);
                }
            }
        }
        return tableNames;
    }

    private static String getParamName(String str) {
        int index = str.indexOf("as");
        if (index == -1)
            return str.trim();
        else
            return str.substring(index + 2).trim();
    }

    private static String extractCatalog(String query) {
        int index = 0;
        int start = 0;
        for (int i = 0; i < query.length(); i++) {
            char symbol = query.charAt(i);
            if (Character.isLetterOrDigit(symbol) || symbol == '.' || symbol == '_')
                index++;
            else
                break;
        }
        if (index == start)
            return null;
        else
            return query.substring(start, index);
    }


    private static boolean isWithTotals(String sql) {
        return sql.startsWith("select") && sql.contains(" with totals ");
    }

    @Override
    public String toString() {
       return new StringBuilder("ClickHouseResultSet{")
        .append("dateFormat=").append(dateFormat)
        .append(", columnNames=").append(Arrays.toString(columnNames))
        .append(", types=").append(Arrays.toString(types))
        .append(", maxRows=").append(maxRows)
        .append(", values=").append(Arrays.toString(values))
        .append(", lastReadColumn=").append(lastReadColumn)
        .append(", totalLine=").append(totalLine)
        .append(", rowNumber=").append(rowNumber)
        .append(", lastReached=").append(lastReached)
        .append(", statement=").append(statement)
        .append('}').toString();
    }
}
