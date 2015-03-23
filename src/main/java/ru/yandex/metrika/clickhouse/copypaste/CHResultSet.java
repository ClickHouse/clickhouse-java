package ru.yandex.metrika.clickhouse.copypaste;

import ru.yandex.metrika.clickhouse.util.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * формат полей.
 * 0000-00-00 00:00:00 - timestamp
 *
 * @author orantius
 * @version $Id$
 * @since 7/12/12
 */
public class CHResultSet extends AbstractResultSet {

    private static final Logger log = Logger.of(CHResultSet.class);

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd"); //

    private final StreamSplitter bis;

    private final String db;
    private final String table;

    private final Map<String, Integer> col = new HashMap<String, Integer>(); // column name -> 1-based index
    private final String[] columns;
    private final String[] types;

    private int maxRows;

    // current line
    private ByteFragment[] values;
    // 1-based
    private int lastReadColumn;

    // next line
    private ByteFragment nextLine;

    // row counter
    private int rowNumber;

    public CHResultSet(InputStream is, int bufferSize, String db, String table) throws IOException {
        this.db = db;
        this.table = table;
        bis = new StreamSplitter(is, (byte) 0x0A, bufferSize);  ///   \n
        ByteFragment headerFragment = bis.next();
        if (headerFragment == null) {
            throw new IllegalArgumentException("clickhouse response without column names");
        }
        String header = headerFragment.asString(true);
        if (header.startsWith("Code: ") && !header.contains("\t")) {
            is.close();
            throw new IOException("Clickhouse error: " + header);
        }
        columns = toStringArray(headerFragment);
        ByteFragment typesFragment = bis.next();
        if (typesFragment == null) {
            throw new IllegalArgumentException("clickhouse response without column types");
        }
        types = toStringArray(typesFragment);

        for (int i = 0; i < columns.length; i++) {
            String s = columns[i];
            col.put(s, i + 1);
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
        if (nextLine == null) {
            try {
                nextLine = bis.next();
                if (nextLine == null || nextLine.length() == 0 || (maxRows != 0 && rowNumber >= maxRows)) {
                    bis.close();
                }
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
        return nextLine != null && nextLine.length() > 0;
    }
    @Override
    public boolean next() throws SQLException {
        if (hasNext()) {
            values = nextLine.split((byte) 0x09);
            nextLine = null;
            rowNumber += 1;
            return true;
        } else return false;
    }

    @Override
    public void close() throws SQLException {
        try {
            bis.close();
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    /////////////////////////////////////////////////////////

    String[] getTypes() {
        return types;
    }

    public String[] getColumnNames() {
        return columns;
    }

    Map<String, Integer> getCol() {
        return col;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new CHResultSetMetaData(this);
    }


    /////////////////////////////////////////////////////////


    @Override
    public boolean wasNull() throws SQLException {
        if (lastReadColumn == 0) throw new IllegalStateException("You should get something before check nullability");
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

    public long getTimestampAsLong(String column) {
        return getTimestampAsLong(asColNum(column));
    }

    @Override
    public Timestamp getTimestamp(String column) throws SQLException {
        return new Timestamp(getTimestampAsLong(column));
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

    public long getTimestampAsLong(int colNum) {
        return toTimestamp(getValue(colNum));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        if (getValue(columnIndex).isNull()) return null;
        return new Timestamp(getTimestampAsLong(columnIndex));
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
        if (string == null) return 0;
        return Double.parseDouble(string);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        // дата из кликхауса приходит в виде строки
        ByteFragment value = getValue(columnIndex);
        if (value.isNull()) return null;
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
            int type = toSqlType(types[columnIndex - 1]);
            switch (type) {
                case Types.BIGINT:      return getLong(columnIndex);
                case Types.INTEGER:     return getInt(columnIndex);
                case Types.VARCHAR:     return getString(columnIndex);
                case Types.FLOAT:       return getFloat(columnIndex);
                case Types.DATE:        return getDate(columnIndex);
                case Types.TIMESTAMP:   return getTime(columnIndex);
                case Types.BLOB:        return getString(columnIndex);
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
        return "1".equals(value.asString());    //вроде бы там   1/0
    }

    private static byte[] toBytes(ByteFragment value) {
        if (value.isNull()) return null;
        return value.unescape();
    }

    private static String toString(ByteFragment value) {
        return value.asString(true);
    }

    private static long[] toLongArray(ByteFragment value) {
        if (value.isNull()) return null;
        if (value.charAt(0) != '[' || value.charAt(value.length()-1) != ']') {
            throw new IllegalArgumentException("not an array: "+value);
        }
        ByteFragment trim = value.subseq(1, value.length() - 2);
        ByteFragment[] values = trim.split((byte) ',');
        long[] result = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = ByteFragmentUtils.parseLong(values[i]);
        }
        return result;
    }

    private static long toTimestamp(ByteFragment value) {
        if (value.isNull()) return 0;
        try {
            return sdf.parse(value.asString()).getTime();
        } catch (ParseException e) {
            return 0;
        }
    }

    //////

    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getRow() throws SQLException {
        return rowNumber + 1;
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

    // 1-based insex in column list
    private int asColNum(String column) {
        if (col.containsKey(column)) {
            return col.get(column);
        } else {
            throw new RuntimeException("no column " + column + " in columns list " + Arrays.toString(getColumnNames()));
        }
    }

    private ByteFragment getValue(int colNum) {
        lastReadColumn = colNum;
        return values[colNum - 1];
    }

    public static int toSqlType(String type) {

        if (type.startsWith("Int") || type.startsWith("UInt")) {
            if (type.endsWith("64")) return Types.BIGINT;
            else return Types.INTEGER;
        }
        if ("String".equals(type)) return Types.VARCHAR;
        if (type.startsWith("Float")) return Types.FLOAT;
        if ("Date".equals(type)) return Types.DATE;
        if ("DateTime".equals(type)) return Types.TIMESTAMP;
        if ("FixedString".equals(type)) return Types.BLOB;

        // don't know what to return actually
        return Types.VARCHAR;

    }

    public static int[] supportedTypes() {
        return new int[] {
                Types.BIGINT, Types.INTEGER, Types.VARCHAR, Types.FLOAT,
                Types.DATE, Types.TIMESTAMP, Types.BLOB
        };
    }

}
