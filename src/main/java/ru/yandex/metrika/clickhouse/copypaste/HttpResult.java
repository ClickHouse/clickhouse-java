package ru.yandex.metrika.clickhouse.copypaste;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * формат полей.
 * 0000-00-00 00:00:00 - timestamp
 * \0 - null?
 * <p/>
 * запросы, которые работают
 * select * from WatchLog_Chunk_2012071003020404100 limit 10
 * show tables
 *
 * @author orantius
 * @version $Id$
 * @since 7/12/12
 */
public class HttpResult extends AbstractResultSet {

    private ByteFragment nextLine;
    private final StreamSplitter bis;

    private final Map<String, Integer> col = new HashMap<String, Integer>(); // column name -> 1-based index
    private final String[] columns;
    private final String[] types;
    // number of characters read from is stream
    private long bytes = 0;
    private final CountingInputStream cis;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //

    private ByteFragment[] values;

    public HttpResult(InputStream is, CountingInputStream cis, int bufferSize) throws IOException {
        this.cis = cis;
        bis = new StreamSplitter(is, (byte) 0x0A, bufferSize);  ///   \n
        ByteFragment headerFragment = bis.next();
        if (headerFragment == null) {
            throw new IllegalArgumentException("ru.yandex.metrika.clickhouse response without column names");
        }
        String header = headerFragment.asString();
        if (header.startsWith("Code: ") && !header.contains("\t")) {
            is.close();
            throw new IOException("Clickhouse error: " + header);
        }
        columns = Patterns.TAB.split(header);
        ByteFragment typesFragment = bis.next();
        if (typesFragment == null) {
            throw new IllegalArgumentException("ru.yandex.metrika.clickhouse response without column types");
        }
        types = Patterns.TAB.split(typesFragment.asString());

        for (int i = 0; i < columns.length; i++) {
            String s = columns[i];
            col.put(s, i + 1);
        }
    }

    public boolean hasNext() throws SQLException {
        if (nextLine == null) {
            try {
                nextLine = bis.next();
                if (nextLine == null || nextLine.length() == 0) {
                    bis.close();
                } else {
                    bytes += nextLine.getLen();
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
        return new ResultSetMetaData() {
            @Override
            public int getColumnCount() throws SQLException {
                return columns.length;
            }

            @Override
            public boolean isAutoIncrement(int column) throws SQLException {
                return false;
            }

            @Override
            public boolean isCaseSensitive(int column) throws SQLException {
                return true;
            }

            @Override
            public boolean isSearchable(int column) throws SQLException {
                return true;
            }

            @Override
            public boolean isCurrency(int column) throws SQLException {
                return false;
            }

            @Override
            public int isNullable(int column) throws SQLException {
                return columnNoNulls;
            }

            @Override
            public boolean isSigned(int column) throws SQLException {
                return !types[column - 1].startsWith("U");
            }

            @Override
            public int getColumnDisplaySize(int column) throws SQLException {
                return 80;
            }

            @Override
            public String getColumnLabel(int column) throws SQLException {
                return columns[column - 1];
            }

            @Override
            public String getColumnName(int column) throws SQLException {
                return columns[column - 1];
            }

            @Override
            public String getSchemaName(int column) throws SQLException {
                return "";
            }

            @Override
            public int getPrecision(int column) throws SQLException {
                return 0;
            }

            @Override
            public int getScale(int column) throws SQLException {
                return 0;
            }

            @Override
            public String getTableName(int column) throws SQLException {
                throw new UnsupportedOperationException("table name unknown at this stage");
            }

            @Override
            public String getCatalogName(int column) throws SQLException {
                throw new UnsupportedOperationException("catalog name unknown at this stage");
            }

            @Override
            public int getColumnType(int column) throws SQLException {
                return toSqlType(getColumnTypeName(column));
            }

            @Override
            public String getColumnTypeName(int column) throws SQLException {
                if (types.length < column) {
                    throw new ArrayIndexOutOfBoundsException("Array length: " + types.length + " requested: " + (column - 1));
                }
                return types[column - 1];
            }

            @Override
            public boolean isReadOnly(int column) throws SQLException {
                return true;
            }

            @Override
            public boolean isWritable(int column) throws SQLException {
                return false;
            }

            @Override
            public boolean isDefinitelyWritable(int column) throws SQLException {
                return false;
            }

            @Override
            public String getColumnClassName(int column) throws SQLException {
                throw new UnsupportedOperationException("no classes for now");
            }

            @Override
            public <T> T unwrap(Class<T> iface) throws SQLException {
                return null;
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) throws SQLException {
                return false;
            }
        };
    }


    /////////////////////////////////////////////////////////


    @Override
    public boolean wasNull() throws SQLException {
        return super.wasNull();
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


    /////////////////////////////////////////////////////////

    @Override
    public String getString(int colNum) {
        return toString(values[colNum - 1]);
    }

    @Override
    public int getInt(int colNum) {
        return ByteFragmentUtils.parseInt(values[colNum - 1]);
    }

    @Override
    public boolean getBoolean(int colNum) {
        return toBoolean(values[colNum - 1]);
    }

    @Override
    public long getLong(int colNum) {
        return ByteFragmentUtils.parseLong(values[colNum - 1]);
    }

    @Override
    public byte[] getBytes(int colNum) {
        return toBytes(values[colNum - 1]);
    }

    public long getTimestampAsLong(int colNum) {
        return toTimestamp(values[colNum - 1]);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return new Timestamp(getTimestampAsLong(columnIndex));
    }

    @Override
    public short getShort(int colNum) {
        return toShort(values[colNum - 1]);
    }

    @Override
    public byte getByte(int colNum) {
        return toByte(values[colNum - 1]);
    }

    public long[] getLongArray(int colNum) {
        return toLongArray(values[colNum - 1]);
    }


    /////////////////////////////////////////////////////////

    private static byte toByte(ByteFragment value) {
        return Byte.parseByte(value.asString());
    }

    private static short toShort(ByteFragment value) {
        return Short.parseShort(value.asString());
    }

    private static boolean toBoolean(ByteFragment value) {
        return "1".equals(value.asString());    //вроде бы там   1/0
    }

    private static byte[] toBytes(ByteFragment value) {
        return value.unescape();
    }

    private static String toString(ByteFragment value) {
        return value.asString(true);
    }

    private static long[] toLongArray(ByteFragment value) {
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
        try {
            return sdf.parse(value.asString()).getTime();
        } catch (ParseException e) {
            return 0;
        }
    }

    // 1-based insex in column list
    private int asColNum(String column) {
        if (col.containsKey(column)) {
            return col.get(column);
        } else {
            throw new RuntimeException("no column " + column + " in columns list " + Arrays.toString(getColumnNames()));
        }
    }

    private int toSqlType(String type) {

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
}
