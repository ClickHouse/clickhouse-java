package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.NullValueException;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.value.ClickHouseArrayValue;
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
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MapBackedRecord implements GenericRecord {

    private final Map<String, Object> record;

    private TableSchema schema;

    public MapBackedRecord(Map<String, Object> record, TableSchema schema) {
        this.record = record;
        this.schema = schema;
    }

    public <T> T readValue(int colIndex) {
        if (colIndex < 1 || colIndex > schema.getColumns().size()) {
            throw new ClientException("Column index out of bounds: " + colIndex);
        }
        colIndex = colIndex - 1;
        return (T) record.get(schema.indexToName(colIndex));
    }

    public <T> T readValue(String colName) {
        return (T) record.get(colName);
    }

    @Override
    public String getString(String colName) {
        Object value = readValue(colName);
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            return (String) value;
        }
        return value.toString();
    }

    @Override
    public String getString(int index) {
        // TODO: it may be incorrect to call .toString() on some objects
        Object value = readValue(index);
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            return (String) value;
        }
        return value.toString();
    }

    private <T> T readPrimitiveValue(String colName, String typeName) {
        Object value = readValue(colName);
        if (value == null) {
            throw new NullValueException("Column '" + colName + "' has null value and it cannot be cast to " + typeName);
        }
        return (T) value;
    }

    private <T> T readPrimitiveValue(int colIndex, String typeName) {
        Object value = readValue(colIndex);
        if (value == null) {
            throw new NullValueException("Column at index = " + colIndex + " has null value and it cannot be cast to " + typeName);
        }
        return (T) value;
    }

    @Override
    public byte getByte(String colName) {
        return readPrimitiveValue(colName, "byte");
    }

    @Override
    public short getShort(String colName) {
        return readPrimitiveValue(colName, "short");
    }

    @Override
    public int getInteger(String colName) {
        return readPrimitiveValue(colName, "int");
    }

    @Override
    public long getLong(String colName) {
        return readPrimitiveValue(colName, "long");
    }

    @Override
    public float getFloat(String colName) {
        return readPrimitiveValue(colName, "float");
    }

    @Override
    public double getDouble(String colName) {
        return readPrimitiveValue(colName, "double");
    }

    @Override
    public boolean getBoolean(String colName) {
        return readPrimitiveValue(colName, "boolean");
    }

    @Override
    public BigInteger getBigInteger(String colName) {
        return readValue(colName);
    }

    @Override
    public BigDecimal getBigDecimal(String colName) {
        return readValue(colName);
    }

    @Override
    public Instant getInstant(String colName) {
        int colIndex = schema.nameToIndex(colName);
        ClickHouseColumn column = schema.getColumns().get(colIndex);
        switch (column.getDataType()) {
            case Date:
            case Date32:
                LocalDate data = readValue(colName);
                return data.atStartOfDay().toInstant(ZoneOffset.UTC);
            case DateTime:
            case DateTime64:
                LocalDateTime dateTime = readValue(colName);
                return dateTime.toInstant(column.getTimeZone().toZoneId().getRules().getOffset(dateTime));

        }
        throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to Instant");
    }

    @Override
    public ZonedDateTime getZonedDateTime(String colName) {
        int colIndex = schema.nameToIndex(colName);
        ClickHouseColumn column = schema.getColumns().get(colIndex);
        switch (column.getDataType()) {
            case DateTime:
            case DateTime64:
                LocalDateTime dateTime = readValue(colName);
                return dateTime.atZone(column.getTimeZone().toZoneId());
            case Date:
            case Date32:
                LocalDate data = readValue(colName);
                return data.atStartOfDay(column.getTimeZone().toZoneId());
        }

        throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to Instant");
    }

    @Override
    public Duration getDuration(String colName) {
        int colIndex = schema.nameToIndex(colName);
        ClickHouseColumn column = schema.getColumns().get(colIndex);
        BigInteger value = readValue(colName);
        try {
            switch (column.getDataType()) {
                case IntervalYear:
                    return Duration.of(value.longValue(), java.time.temporal.ChronoUnit.YEARS);
                case IntervalQuarter:
                    return Duration.of(value.longValue() * 3, java.time.temporal.ChronoUnit.MONTHS);
                case IntervalMonth:
                    return Duration.of(value.longValue(), java.time.temporal.ChronoUnit.MONTHS);
                case IntervalWeek:
                    return Duration.of(value.longValue(), ChronoUnit.WEEKS);
                case IntervalDay:
                    return Duration.of(value.longValue(), java.time.temporal.ChronoUnit.DAYS);
                case IntervalHour:
                    return Duration.of(value.longValue(), java.time.temporal.ChronoUnit.HOURS);
                case IntervalMinute:
                    return Duration.of(value.longValue(), java.time.temporal.ChronoUnit.MINUTES);
                case IntervalSecond:
                    return Duration.of(value.longValue(), java.time.temporal.ChronoUnit.SECONDS);
                case IntervalMicrosecond:
                    return Duration.of(value.longValue(), java.time.temporal.ChronoUnit.MICROS);
                case IntervalMillisecond:
                    return Duration.of(value.longValue(), java.time.temporal.ChronoUnit.MILLIS);
                case IntervalNanosecond:
                    return Duration.of(value.longValue(), java.time.temporal.ChronoUnit.NANOS);
            }
        } catch (ArithmeticException e) {
            throw new ClientException("Stored value is bigger then Long.MAX_VALUE and it cannot be converted to Duration without information loss", e);
        }
        throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to Duration");
    }

    @Override
    public Inet4Address getInet4Address(String colName) {
        return readValue(colName);
    }

    @Override
    public Inet6Address getInet6Address(String colName) {
        return readValue(colName);
    }

    @Override
    public UUID getUUID(String colName) {
        return readValue(colName);
    }

    @Override
    public ClickHouseGeoPointValue getGeoPoint(String colName) {
        return ClickHouseGeoPointValue.of(readValue(colName));
    }

    @Override
    public ClickHouseGeoRingValue getGeoRing(String colName) {
        return ClickHouseGeoRingValue.of(readValue(colName));
    }

    @Override
    public ClickHouseGeoPolygonValue getGeoPolygon(String colName) {
        return ClickHouseGeoPolygonValue.of(readValue(colName));
    }

    @Override
    public ClickHouseGeoMultiPolygonValue getGeoMultiPolygon(String colName) {
        return ClickHouseGeoMultiPolygonValue.of(readValue(colName));
    }


    @Override
    public <T> List<T> getList(String colName) {
        ClickHouseArrayValue<?> array = readValue(colName);
        return null;
    }


    private <T> T getPrimitiveArray(String colName) {
        BinaryStreamReader.ArrayValue array = readValue(colName);
        if (array.itemType.isPrimitive()) {
            return (T) array.array;
        } else {
            throw new ClientException("Array is not of primitive type");
        }
    }

    @Override
    public byte[] getByteArray(String colName) {
        return getPrimitiveArray(colName);
    }

    @Override
    public int[] getIntArray(String colName) {
        return getPrimitiveArray(colName);
    }

    @Override
    public long[] getLongArray(String colName) {
        return getPrimitiveArray(colName);
    }

    @Override
    public float[] getFloatArray(String colName) {
        return getPrimitiveArray(colName);
    }

    @Override
    public double[] getDoubleArray(String colName) {
        return getPrimitiveArray(colName);
    }

    @Override
    public boolean hasValue(int colIndex) {
        return record.containsKey(schema.indexToName(colIndex));
    }

    @Override
    public boolean hasValue(String colName) {
        return record.containsKey(colName);
    }

    @Override
    public byte getByte(int index) {
        return readPrimitiveValue(index, "byte");
    }

    @Override
    public short getShort(int index) {
        return readPrimitiveValue(index, "short");
    }

    @Override
    public int getInteger(int index) {
        return readPrimitiveValue(index, "int");
    }

    @Override
    public long getLong(int index) {
        return readPrimitiveValue(index, "long");
    }

    @Override
    public float getFloat(int index) {
        return readPrimitiveValue(index, "float");
    }

    @Override
    public double getDouble(int index) {
        return readPrimitiveValue(index, "double");
    }

    @Override
    public boolean getBoolean(int index) {
        return readPrimitiveValue(index, "boolean");
    }

    @Override
    public BigInteger getBigInteger(int index) {
        return readValue(index);
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        return readValue(index);
    }

    @Override
    public Instant getInstant(int index) {
        return readValue(index);
    }

    @Override
    public ZonedDateTime getZonedDateTime(int index) {
        return readValue(index);
    }

    @Override
    public Duration getDuration(int index) {
        return readValue(index);
    }

    @Override
    public Inet4Address getInet4Address(int index) {
        return readValue(index);
    }

    @Override
    public Inet6Address getInet6Address(int index) {
        return readValue(index);
    }

    @Override
    public UUID getUUID(int index) {
        return readValue(index);
    }

    @Override
    public ClickHouseGeoPointValue getGeoPoint(int index) {
        return readValue(index);
    }

    @Override
    public ClickHouseGeoRingValue getGeoRing(int index) {
        return readValue(index);
    }

    @Override
    public ClickHouseGeoPolygonValue getGeoPolygon(int index) {
        return readValue(index);
    }

    @Override
    public ClickHouseGeoMultiPolygonValue getGeoMultiPolygon(int index) {
        return readValue(index);
    }

    @Override
    public <T> List<T> getList(int index) {
        return readValue(index);
    }

    @Override
    public byte[] getByteArray(int index) {
        return getPrimitiveArray(schema.indexToName(index));
    }

    @Override
    public int[] getIntArray(int index) {
        return getPrimitiveArray(schema.indexToName(index));
    }

    @Override
    public long[] getLongArray(int index) {
        return getPrimitiveArray(schema.indexToName(index));
    }

    @Override
    public float[] getFloatArray(int index) {
        return getPrimitiveArray(schema.indexToName(index));
    }

    @Override
    public double[] getDoubleArray(int index) {
        return getPrimitiveArray(schema.indexToName(index));
    }

    @Override
    public Object[] getTuple(int index) {
        return readValue(index);
    }

    @Override
    public Object[] getTuple(String colName) {
        return readValue(colName);
    }

    @Override
    public byte getEnum8(String colName) {
        return readValue(colName);
    }

    @Override
    public byte getEnum8(int index) {
        return readValue(index);
    }

    @Override
    public short getEnum16(String colName) {
        return readValue(colName);
    }

    @Override
    public short getEnum16(int index) {
        return readValue(index);
    }

    @Override
    public LocalDate getLocalDate(String colName) {
        Object value = readValue(colName);
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalDate();
        }
        return (LocalDate) value;

    }

    @Override
    public LocalDate getLocalDate(int index) {
        Object value = readValue(index);
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalDate();
        }
        return (LocalDate) value;
    }

    @Override
    public LocalDateTime getLocalDateTime(String colName) {
        Object value = readValue(colName);
        if (value instanceof LocalDate) {
            return ((LocalDate) value).atStartOfDay();
        }
        return (LocalDateTime) value;
    }

    @Override
    public LocalDateTime getLocalDateTime(int index) {
        return readValue(index);
    }
}
