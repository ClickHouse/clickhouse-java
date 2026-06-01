package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.internal.NumberConverter;
import com.clickhouse.client.api.internal.SchemaUtils;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.value.ClickHouseBitmap;
import com.clickhouse.data.value.ClickHouseGeoMultiPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoPointValue;
import com.clickhouse.data.value.ClickHouseGeoPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoRingValue;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class JSONEachRowFormatReader implements ClickHouseTextFormatReader {
    private final JsonParser parser;
    private TableSchema schema;
    private Map<String, Object> currentRow;
    private Map<String, Object> nextRow;
    private boolean hasNext;

    public JSONEachRowFormatReader(JsonParser parser) {
        this.parser = parser;
        try {
            this.nextRow = parser.nextRow();
            this.hasNext = this.nextRow != null;
            if (nextRow != null) {
                List<ClickHouseColumn> columns = new ArrayList<>();
                for (String key : nextRow.keySet()) {
                    // For JSONEachRow we don't know the exact ClickHouse type, so we use a reasonable default.
                    // We can try to guess based on the value type in the first row.
                    columns.add(ClickHouseColumn.of(key, SchemaUtils.inferDataType(nextRow.get(key)), false));
                }
                this.schema = new TableSchema(columns);
            } else {
                this.schema = new TableSchema(new ArrayList<>());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize JSON reader", e);
        }
    }

    @Override
    public <T> T readValue(int colIndex) {
        return (T) currentRow.get(schema.columnIndexToName(colIndex));
    }

    @Override
    public <T> T readValue(String colName) {
        return (T) currentRow.get(colName);
    }

    @Override
    public boolean hasValue(String colName) {
        return currentRow.containsKey(colName) && currentRow.get(colName) != null;
    }

    @Override
    public boolean hasValue(int colIndex) {
        return hasValue(schema.columnIndexToName(colIndex));
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public Map<String, Object> next() {
        if (!hasNext) {
            currentRow = null;
            return null;
        }

        currentRow = nextRow;
        readNextRow();
        return currentRow;
    }

    private void readNextRow() {
        try {
            nextRow = parser.nextRow();
            hasNext = nextRow != null;
        } catch (Exception e) {
            hasNext = false;
            nextRow = null;
            throw new RuntimeException("Failed to read next JSON row", e);
        }
    }

    @Override
    public String getString(String colName) {
        Object val = currentRow.get(colName);
        return val == null ? null : val.toString();
    }

    @Override
    public byte getByte(String colName) {
        return ((Number) currentRow.get(colName)).byteValue();
    }

    @Override
    public short getShort(String colName) {
        return ((Number) currentRow.get(colName)).shortValue();
    }

    @Override
    public int getInteger(String colName) {
        return ((Number) currentRow.get(colName)).intValue();
    }

    @Override
    public long getLong(String colName) {
        return ((Number) currentRow.get(colName)).longValue();
    }

    @Override
    public float getFloat(String colName) {
        return ((Number) currentRow.get(colName)).floatValue();
    }

    @Override
    public double getDouble(String colName) {
        return ((Number) currentRow.get(colName)).doubleValue();
    }

    @Override
    public boolean getBoolean(String colName) {
        Object val = currentRow.get(colName);
        if (val instanceof Boolean) {
            return (Boolean) val;
        }
        if (val instanceof Number) {
            // Match AbstractBinaryFormatReader (SerializerUtils.convertToBoolean):
            // any non-zero integral value is true, zero is false. Fractional
            // values keep the same behavior because they are truncated by
            // longValue() before the zero check.
            return ((Number) val).longValue() != 0;
        }
        if (val == null) {
            throw new ClientException("Column '" + colName + "' has null value and cannot be converted to boolean");
        }
        throw new ClientException("Cannot convert value of type " + val.getClass().getName()
                + " in column '" + colName + "' to boolean");
    }

    @Override
    public BigInteger getBigInteger(String colName) {
        Object val = currentRow.get(colName);
        if (val == null) return null;
        if (val instanceof BigInteger) return (BigInteger) val;
        return new BigDecimal(val.toString()).toBigInteger();
    }

    @Override
    public BigDecimal getBigDecimal(String colName) {
        Object val = currentRow.get(colName);
        if (val == null) return null;
        if (val instanceof BigDecimal) return (BigDecimal) val;
        return new BigDecimal(val.toString());
    }

    @Override
    public Instant getInstant(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ZonedDateTime getZonedDateTime(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Duration getDuration(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Inet4Address getInet4Address(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Inet6Address getInet6Address(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UUID getUUID(String colName) {
        Object val = currentRow.get(colName);
        return val == null ? null : UUID.fromString(val.toString());
    }

    @Override
    public ClickHouseGeoPointValue getGeoPoint(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClickHouseGeoRingValue getGeoRing(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClickHouseGeoPolygonValue getGeoPolygon(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClickHouseGeoMultiPolygonValue getGeoMultiPolygon(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<T> getList(String colName) {
        Object val = currentRow.get(colName);
        if (val == null) {
            return null;
        }
        if (!(val instanceof List<?>)) {
            throw new ClientException("Column '" + colName + "' is not of array type (actual: "
                    + val.getClass().getName() + ")");
        }
        return (List<T>) val;
    }

    @Override
    public byte[] getByteArray(String colName) {
        return getPrimitiveArray(colName, byte.class);
    }

    @Override
    public int[] getIntArray(String colName) {
        return getPrimitiveArray(colName, int.class);
    }

    @Override
    public long[] getLongArray(String colName) {
        return getPrimitiveArray(colName, long.class);
    }

    @Override
    public float[] getFloatArray(String colName) {
        return getPrimitiveArray(colName, float.class);
    }

    @Override
    public double[] getDoubleArray(String colName) {
        return getPrimitiveArray(colName, double.class);
    }

    @Override
    public boolean[] getBooleanArray(String colName) {
        return getPrimitiveArray(colName, boolean.class);
    }

    @Override
    public short[] getShortArray(String colName) {
        return getPrimitiveArray(colName, short.class);
    }

    @Override
    public String[] getStringArray(String colName) {
        List<?> list = asArrayList(colName);
        if (list == null) {
            return null;
        }
        String[] out = new String[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Object el = list.get(i);
            out[i] = el == null ? null : el.toString();
        }
        return out;
    }

    @Override
    public Object[] getObjectArray(String colName) {
        List<?> list = asArrayList(colName);
        return list == null ? null : list.toArray(new Object[0]);
    }

    /**
     * Returns the value of the given column as a {@code List}, or {@code null}
     * if the value is missing. Throws {@link ClientException} when the column
     * exists but is not an array.
     */
    private List<?> asArrayList(String colName) {
        Object val = currentRow.get(colName);
        if (val == null) {
            return null;
        }
        if (!(val instanceof List<?>)) {
            throw new ClientException("Column '" + colName + "' is not of array type (actual: "
                    + val.getClass().getName() + ")");
        }
        return (List<?>) val;
    }

    @SuppressWarnings("unchecked")
    private <T> T getPrimitiveArray(String colName, Class<?> componentType) {
        List<?> list = asArrayList(colName);
        if (list == null) {
            return null;
        }
        try {
            Object array = Array.newInstance(componentType, list.size());
            for (int i = 0; i < list.size(); i++) {
                Object el = list.get(i);
                if (el == null) {
                    throw new ClientException("Column '" + colName
                            + "' contains a null element which cannot fit into an array of primitive "
                            + componentType.getName());
                }
                Array.set(array, i, coerceToComponent(el, componentType));
            }
            return (T) array;
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ClientException("Value of column '" + colName
                    + "' cannot be converted to an array of " + componentType.getName(), e);
        }
    }

    /**
     * Coerces a parsed JSON element to a boxed primitive type. JSON parsers
     * may materialize numeric array elements as different boxed types
     * (e.g. {@code Integer}, {@code Long}, {@code Double}, {@code BigDecimal}),
     * so element-level conversion is necessary before populating a typed
     * primitive array. The {@code componentType} is always one of the eight
     * Java primitives passed by {@link #getPrimitiveArray}; unsupported
     * component types are rejected explicitly to keep the helper total.
     */
    private static Object coerceToComponent(Object value, Class<?> componentType) {
        if (componentType == byte.class) {
            return NumberConverter.toByte(value);
        }
        if (componentType == short.class) {
            return NumberConverter.toShort(value);
        }
        if (componentType == int.class) {
            return NumberConverter.toInt(value);
        }
        if (componentType == long.class) {
            return NumberConverter.toLong(value);
        }
        if (componentType == float.class) {
            return NumberConverter.toFloat(value);
        }
        if (componentType == double.class) {
            return NumberConverter.toDouble(value);
        }
        if (componentType == boolean.class) {
            if (value instanceof Boolean) {
                return value;
            }
            if (value instanceof Number) {
                return ((Number) value).longValue() != 0;
            }
            throw new IllegalArgumentException(
                    "Cannot convert " + value.getClass().getName() + " to boolean array element");
        }
        throw new IllegalArgumentException("Unsupported component type: " + componentType.getName());
    }

    @Override
    public String getString(int index) {
        return getString(schema.columnIndexToName(index));
    }

    @Override
    public byte getByte(int index) {
        return getByte(schema.columnIndexToName(index));
    }

    @Override
    public short getShort(int index) {
        return getShort(schema.columnIndexToName(index));
    }

    @Override
    public int getInteger(int index) {
        return getInteger(schema.columnIndexToName(index));
    }

    @Override
    public long getLong(int index) {
        return getLong(schema.columnIndexToName(index));
    }

    @Override
    public float getFloat(int index) {
        return getFloat(schema.columnIndexToName(index));
    }

    @Override
    public double getDouble(int index) {
        return getDouble(schema.columnIndexToName(index));
    }

    @Override
    public boolean getBoolean(int index) {
        return getBoolean(schema.columnIndexToName(index));
    }

    @Override
    public BigInteger getBigInteger(int index) {
        return getBigInteger(schema.columnIndexToName(index));
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        return getBigDecimal(schema.columnIndexToName(index));
    }

    @Override
    public Instant getInstant(int index) {
        return getInstant(schema.columnIndexToName(index));
    }

    @Override
    public ZonedDateTime getZonedDateTime(int index) {
        return getZonedDateTime(schema.columnIndexToName(index));
    }

    @Override
    public Duration getDuration(int index) {
        return getDuration(schema.columnIndexToName(index));
    }

    @Override
    public Inet4Address getInet4Address(int index) {
        return getInet4Address(schema.columnIndexToName(index));
    }

    @Override
    public Inet6Address getInet6Address(int index) {
        return getInet6Address(schema.columnIndexToName(index));
    }

    @Override
    public UUID getUUID(int index) {
        return getUUID(schema.columnIndexToName(index));
    }

    @Override
    public ClickHouseGeoPointValue getGeoPoint(int index) {
        return getGeoPoint(schema.columnIndexToName(index));
    }

    @Override
    public ClickHouseGeoRingValue getGeoRing(int index) {
        return getGeoRing(schema.columnIndexToName(index));
    }

    @Override
    public ClickHouseGeoPolygonValue getGeoPolygon(int index) {
        return getGeoPolygon(schema.columnIndexToName(index));
    }

    @Override
    public ClickHouseGeoMultiPolygonValue getGeoMultiPolygon(int index) {
        return getGeoMultiPolygon(schema.columnIndexToName(index));
    }

    @Override
    public <T> List<T> getList(int index) {
        return getList(schema.columnIndexToName(index));
    }

    @Override
    public byte[] getByteArray(int index) {
        return getByteArray(schema.columnIndexToName(index));
    }

    @Override
    public int[] getIntArray(int index) {
        return getIntArray(schema.columnIndexToName(index));
    }

    @Override
    public long[] getLongArray(int index) {
        return getLongArray(schema.columnIndexToName(index));
    }

    @Override
    public float[] getFloatArray(int index) {
        return getFloatArray(schema.columnIndexToName(index));
    }

    @Override
    public double[] getDoubleArray(int index) {
        return getDoubleArray(schema.columnIndexToName(index));
    }

    @Override
    public boolean[] getBooleanArray(int index) {
        return getBooleanArray(schema.columnIndexToName(index));
    }

    @Override
    public short[] getShortArray(int index) {
        return getShortArray(schema.columnIndexToName(index));
    }

    @Override
    public String[] getStringArray(int index) {
        return getStringArray(schema.columnIndexToName(index));
    }

    @Override
    public Object[] getObjectArray(int index) {
        return getObjectArray(schema.columnIndexToName(index));
    }

    @Override
    public Object[] getTuple(int index) {
        return getTuple(schema.columnIndexToName(index));
    }

    @Override
    public Object[] getTuple(String colName) {
        Object value = currentRow.get(colName);
        if (value == null) {
            return null;
        }
        if (value instanceof List<?>) {
            return ((List<?>) value).toArray(new Object[0]);
        }
        return (Object[]) value;
    }

    @Override
    public byte getEnum8(String colName) {
        return getByte(colName);
    }

    @Override
    public byte getEnum8(int index) {
        return getByte(index);
    }

    @Override
    public short getEnum16(String colName) {
        return getShort(colName);
    }

    @Override
    public short getEnum16(int index) {
        return getShort(index);
    }

    @Override
    public LocalDate getLocalDate(String colName) {
        Object val = currentRow.get(colName);
        return val == null ? null : LocalDate.parse(val.toString());
    }

    @Override
    public LocalDate getLocalDate(int index) {
        return getLocalDate(schema.columnIndexToName(index));
    }

    @Override
    public LocalTime getLocalTime(String colName) {
        Object val = currentRow.get(colName);
        return val == null ? null : LocalTime.parse(val.toString());
    }

    @Override
    public LocalTime getLocalTime(int index) {
        return getLocalTime(schema.columnIndexToName(index));
    }

    @Override
    public LocalDateTime getLocalDateTime(String colName) {
        Object val = currentRow.get(colName);
        return val == null ? null : LocalDateTime.parse(val.toString());
    }

    @Override
    public LocalDateTime getLocalDateTime(int index) {
        return getLocalDateTime(schema.columnIndexToName(index));
    }

    @Override
    public OffsetDateTime getOffsetDateTime(String colName) {
        Object val = currentRow.get(colName);
        return val == null ? null : OffsetDateTime.parse(val.toString());
    }

    @Override
    public OffsetDateTime getOffsetDateTime(int index) {
        return getOffsetDateTime(schema.columnIndexToName(index));
    }

    @Override
    public TableSchema getSchema() {
        return schema;
    }

    @Override
    public ClickHouseBitmap getClickHouseBitmap(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClickHouseBitmap getClickHouseBitmap(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TemporalAmount getTemporalAmount(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TemporalAmount getTemporalAmount(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {
        parser.close();
    }
}
