package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.JsonParser;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.value.ClickHouseBitmap;
import com.clickhouse.data.value.ClickHouseGeoMultiPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoPointValue;
import com.clickhouse.data.value.ClickHouseGeoPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoRingValue;

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
    private Map<String, Object> firstRow;
    private boolean firstRowRead = false;

    public JSONEachRowFormatReader(JsonParser parser) {
        this.parser = parser;
        try {
            this.firstRow = parser.nextRow();
            if (firstRow != null) {
                List<ClickHouseColumn> columns = new ArrayList<>();
                for (String key : firstRow.keySet()) {
                    // For JSONEachRow we don't know the exact ClickHouse type, so we use a reasonable default.
                    // We can try to guess based on the value type in the first row.
                    columns.add(ClickHouseColumn.of(key, guessDataType(firstRow.get(key)), false));
                }
                this.schema = new TableSchema(columns);
            } else {
                this.schema = new TableSchema(new ArrayList<>());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize JSON reader", e);
        }
    }

    private ClickHouseDataType guessDataType(Object value) {
        if (value instanceof Number) {
            if (value instanceof Integer || value instanceof Long || value instanceof BigInteger) {
                return ClickHouseDataType.Int64;
            } else if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
                double d = ((Number) value).doubleValue();
                if (d == Math.floor(d) && !Double.isInfinite(d) && d <= Long.MAX_VALUE && d >= Long.MIN_VALUE) {
                    return ClickHouseDataType.Int64;
                }
                return ClickHouseDataType.Float64;
            } else {
                return ClickHouseDataType.Float64;
            }
        } else if (value instanceof Boolean) {
            return ClickHouseDataType.Bool;
        } else {
            return ClickHouseDataType.String;
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
        if (!firstRowRead) {
            return firstRow != null;
        }
        return true; // We'll find out in next()
    }

    @Override
    public Map<String, Object> next() {
        if (!firstRowRead) {
            firstRowRead = true;
            currentRow = firstRow;
            return currentRow;
        }
        try {
            currentRow = parser.nextRow();
            return currentRow;
        } catch (Exception e) {
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
        if (val instanceof Boolean) return (Boolean) val;
        if (val instanceof Number) return ((Number) val).intValue() != 0;
        return Boolean.parseBoolean(val.toString());
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
        return UUID.fromString(currentRow.get(colName).toString());
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
        return (List<T>) currentRow.get(colName);
    }

    @Override
    public byte[] getByteArray(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] getIntArray(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long[] getLongArray(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float[] getFloatArray(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double[] getDoubleArray(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean[] getBooleanArray(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short[] getShortArray(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] getStringArray(String colName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] getObjectArray(String colName) {
        throw new UnsupportedOperationException();
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
        return (Object[]) currentRow.get(colName);
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
        return LocalDate.parse(currentRow.get(colName).toString());
    }

    @Override
    public LocalDate getLocalDate(int index) {
        return getLocalDate(schema.columnIndexToName(index));
    }

    @Override
    public LocalTime getLocalTime(String colName) {
        return LocalTime.parse(currentRow.get(colName).toString());
    }

    @Override
    public LocalTime getLocalTime(int index) {
        return getLocalTime(schema.columnIndexToName(index));
    }

    @Override
    public LocalDateTime getLocalDateTime(String colName) {
        return LocalDateTime.parse(currentRow.get(colName).toString());
    }

    @Override
    public LocalDateTime getLocalDateTime(int index) {
        return getLocalDateTime(schema.columnIndexToName(index));
    }

    @Override
    public OffsetDateTime getOffsetDateTime(String colName) {
        return OffsetDateTime.parse(currentRow.get(colName).toString());
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
