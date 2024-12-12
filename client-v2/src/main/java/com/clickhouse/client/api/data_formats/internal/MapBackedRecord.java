package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.NullValueException;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.value.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class MapBackedRecord implements GenericRecord {

    private final Map<String, Object> record;

    private final TableSchema schema;

    private Map[] columnConverters;

    public MapBackedRecord(Map<String, Object> record, Map[] columnConverters, TableSchema schema) {
        this.record = new HashMap<>(record);
        this.schema = schema;
        this.columnConverters = columnConverters;
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

    private <T> T readNumberValue(String colName, NumberConverter.NumberType targetType) {
        int colIndex = schema.nameToIndex(colName);

        Function<Object, Object> converter = (Function<Object, Object>) columnConverters[colIndex].get(targetType);
        if (converter != null) {
            Object value = readValue(colName);
            if (value == null) {
                throw new NullValueException("Column " + colName + " has null value and it cannot be cast to " +
                        targetType.getTypeName());
            }
            return (T) converter.apply(value);
        } else {
            String columnTypeName = schema.getColumnByName(colName).getDataType().name();
            throw new ClientException("Column " + colName + " " + columnTypeName +
                    " cannot be converted to " + targetType.getTypeName());
        }
    }

    @Override
    public byte getByte(String colName) {
        return readNumberValue(colName, NumberConverter.NumberType.Byte);
    }

    @Override
    public short getShort(String colName) {
        return readNumberValue(colName, NumberConverter.NumberType.Short);
    }

    @Override
    public int getInteger(String colName) {
        return readNumberValue(colName, NumberConverter.NumberType.Int);
    }

    @Override
    public long getLong(String colName) {
        return readNumberValue(colName, NumberConverter.NumberType.Long);
    }

    @Override
    public float getFloat(String colName) {
        return readNumberValue(colName, NumberConverter.NumberType.Float);
    }

    @Override
    public double getDouble(String colName) {
        return readNumberValue(colName, NumberConverter.NumberType.Double);
    }

    @Override
    public boolean getBoolean(String colName) {
        return readNumberValue(colName, NumberConverter.NumberType.Boolean);
    }

    @Override
    public BigInteger getBigInteger(String colName) {
        return readNumberValue(colName, NumberConverter.NumberType.BigInteger);
    }

    @Override
    public BigDecimal getBigDecimal(String colName) {
        return readNumberValue(colName, NumberConverter.NumberType.BigDecimal);
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
            case Date:
            case Date32:
                return readValue(colName);
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
        return getList(schema.nameToIndex(colName));
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
    public boolean[] getBooleanArray(String colName) {
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
        return getByte(schema.indexToName(index));
    }

    @Override
    public short getShort(int index) {
        return getShort(schema.indexToName(index));
    }

    @Override
    public int getInteger(int index) {
        return getInteger(schema.indexToName(index));
    }

    @Override
    public long getLong(int index) {
        return getLong(schema.indexToName(index));
    }

    @Override
    public float getFloat(int index) {
        return getFloat(schema.indexToName(index));
    }

    @Override
    public double getDouble(int index) {
        return getDouble(schema.indexToName(index));
    }

    @Override
    public boolean getBoolean(int index) {
        return getBoolean(schema.indexToName(index));
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
        Object value = readValue(index);
        if (value instanceof BinaryStreamReader.ArrayValue) {
            return ((BinaryStreamReader.ArrayValue) value).asList();
        } else {
            throw new ClientException("Column is not of array type");
        }
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
    public boolean[] getBooleanArray(int index) {
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
    public LocalDate getLocalDate(int index) {
        Object value = readValue(index);
        if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).toLocalDate();
        }
        return (LocalDate) value;
    }

    @Override
    public LocalDate getLocalDate(String colName) {
        Object value = readValue(colName);
        if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).toLocalDate();
        }
        return (LocalDate) value;

    }

    @Override
    public LocalDateTime getLocalDateTime(String colName) {
        Object value = readValue(colName);
        if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).toLocalDateTime();
        }
        return (LocalDateTime) value;
    }

    @Override
    public LocalDateTime getLocalDateTime(int index) {
        Object value = readValue(index);
        if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).toLocalDateTime();
        }
        return (LocalDateTime) value;
    }

    @Override
    public ClickHouseBitmap getClickHouseBitmap(String colName) {
        return readValue(colName);
    }

    @Override
    public ClickHouseBitmap getClickHouseBitmap(int index) {
        return readValue(index);
    }

    @Override
    public TableSchema getSchema() {
        return this.schema;
    }

    @Override
    public Object getObject(String colName) {
        return readValue(colName);
    }

    @Override
    public Object getObject(int index) {
        return readValue(index);
    }

    @Override
    public Map<String, Object> getValues() {
        return this.record;
    }
}
