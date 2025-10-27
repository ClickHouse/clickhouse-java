package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.DataTypeUtils;
import com.clickhouse.client.api.internal.DataTypeConverter;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.NullValueException;
import com.clickhouse.data.ClickHouseColumn;
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
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class MapBackedRecord implements GenericRecord {

    private final Map<String, Object> record;

    private final TableSchema schema;

    private Map[] columnConverters;

    private DataTypeConverter dataTypeConverter;

    public MapBackedRecord(Map<String, Object> record, Map[] columnConverters, TableSchema schema) {
        this.record = new HashMap<>(record);
        this.schema = schema;
        this.columnConverters = columnConverters;
        this.dataTypeConverter = DataTypeConverter.INSTANCE;
    }

    public <T> T readValue(int colIndex) {
        if (colIndex < 1 || colIndex > schema.getColumns().size()) {
            throw new ClientException("Column index out of bounds: " + colIndex);
        }

        return (T) record.get(schema.columnIndexToName(colIndex));
    }

    public <T> T readValue(String colName) {
        return (T) record.get(colName);
    }

    @Override
    public String getString(String colName) {
        return dataTypeConverter.convertToString(readValue(colName), schema.getColumnByName(colName));
    }

    @Override
    public String getString(int index) {
        return getString(schema.columnIndexToName(index));
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
            throw new ClientException("Column '" + colName + "' of type " + columnTypeName +
                    " cannot be converted to '" + targetType.getTypeName() + "' value");
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
        ClickHouseColumn column =  schema.getColumnByName(colName);
        switch (column.getDataType()) {
            case Date:
            case Date32:
                LocalDate data = readValue(colName);
                return data.atStartOfDay().toInstant(ZoneOffset.UTC);
            case DateTime:
            case DateTime64:
                LocalDateTime dateTime = readValue(colName);
                return dateTime.toInstant(column.getTimeZone().toZoneId().getRules().getOffset(dateTime));
            case Time:
                return Instant.ofEpochSecond(getLong(colName));
            case Time64:
                return DataTypeUtils.instantFromTime64Integer(column.getScale(), getLong(colName));

        }
        throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to Instant");
    }

    @Override
    public ZonedDateTime getZonedDateTime(String colName) {
        ClickHouseColumn column = schema.getColumnByName(colName);
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
        return readValue(colName);
    }

    @Override
    public TemporalAmount getTemporalAmount(String colName) {
        return readValue(colName);
    }

    @Override
    public Inet4Address getInet4Address(String colName) {
        return InetAddressConverter.convertToIpv4(readValue(colName));
    }

    @Override
    public Inet6Address getInet6Address(String colName) {
        return InetAddressConverter.convertToIpv6(readValue(colName));
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
        Object value = readValue(colName);
        if (value instanceof BinaryStreamReader.ArrayValue) {
            return ((BinaryStreamReader.ArrayValue) value).asList();
        } else if (value instanceof List<?>) {
            return (List<T>) value;
        } else {
            throw new ClientException("Column is not of array type");
        }
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
    public short[] getShortArray(String colName) {
        return getPrimitiveArray(colName);
    }

    @Override
    public String[] getStringArray(String colName) {
        return getPrimitiveArray(colName);
    }

    @Override
    public boolean hasValue(int colIndex) {
        return hasValue(schema.columnIndexToName(colIndex));
    }

    @Override
    public boolean hasValue(String colName) {
        return record.containsKey(colName);
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
    public TemporalAmount getTemporalAmount(int index) {
        return readValue(index);
    }

    @Override
    public Inet4Address getInet4Address(int index) {
        return InetAddressConverter.convertToIpv4(readValue(index));
    }

    @Override
    public Inet6Address getInet6Address(int index) {
        return InetAddressConverter.convertToIpv6(readValue(index));
    }

    @Override
    public UUID getUUID(int index) {
        return readValue(index);
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
        return getPrimitiveArray(schema.columnIndexToName(index));
    }

    @Override
    public int[] getIntArray(int index) {
        return getPrimitiveArray(schema.columnIndexToName(index));
    }

    @Override
    public long[] getLongArray(int index) {
        return getPrimitiveArray(schema.columnIndexToName(index));
    }

    @Override
    public float[] getFloatArray(int index) {
        return getPrimitiveArray(schema.columnIndexToName(index));
    }

    @Override
    public double[] getDoubleArray(int index) {
        return getPrimitiveArray(schema.columnIndexToName(index));
    }

    @Override
    public boolean[] getBooleanArray(int index) {
        return getPrimitiveArray(schema.columnIndexToName(index));
    }

    @Override
    public short[] getShortArray(int index) {
        return getPrimitiveArray(schema.columnIndexToName(index));
    }

    @Override
    public String[] getStringArray(int index) {
        return getPrimitiveArray(schema.columnIndexToName(index));
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
        return getLocalDate(schema.columnIndexToName(index));
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
       return getLocalDateTime(schema.columnIndexToName(index));
    }

    @Override
    public OffsetDateTime getOffsetDateTime(String colName) {
        Object value = readValue(colName);
        if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).toOffsetDateTime();
        }
        return (OffsetDateTime) value;
    }

    @Override
    public OffsetDateTime getOffsetDateTime(int index) {
        return getOffsetDateTime(schema.columnIndexToName(index));
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
