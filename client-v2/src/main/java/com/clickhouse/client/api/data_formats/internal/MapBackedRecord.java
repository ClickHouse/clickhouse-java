package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.ClientException;
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
import java.time.LocalTime;
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
                if (targetType == NumberConverter.NumberType.BigInteger || targetType == NumberConverter.NumberType.BigDecimal) {
                    return null;
                }
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
        int colIndex = column.getColumnIndex();
        switch (column.getValueDataType()) {
            case Date:
            case Date32:
                LocalDate date = getLocalDate(colIndex);
                return date == null ? null : Instant.from(date);
            case Time:
            case Time64:
                LocalDateTime time = getLocalDateTime(colName);
                return time == null ? null : time.toInstant(ZoneOffset.UTC);
            case DateTime:
            case DateTime64:
            case DateTime32:
                ZonedDateTime zdt = getZonedDateTime(colName);
                return zdt == null ? null : zdt.toInstant();
            case Dynamic:
            case Variant:
                Object value = readValue(colName);
                Instant instant = AbstractBinaryFormatReader.objectToInstant(value);
                if (value == null || instant != null) {
                    return instant;
                }
                break;
        }
        throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to Instant");
    }

    @Override
    public ZonedDateTime getZonedDateTime(String colName) {
        return getZonedDateTime(schema.nameToColumnIndex(colName));
    }

    @Override
    public Duration getDuration(String colName) {
        TemporalAmount temporalAmount = readValue(colName);
        return temporalAmount == null ? null : Duration.from(temporalAmount);
    }

    @Override
    public TemporalAmount getTemporalAmount(String colName) {
        return readValue(colName);
    }

    @Override
    public Inet4Address getInet4Address(String colName) {
        Object val = readValue(colName);
        return val == null ? null : InetAddressConverter.convertToIpv4((java.net.InetAddress) val);
    }

    @Override
    public Inet6Address getInet6Address(String colName) {
        Object val = readValue(colName);
        return val == null ? null : InetAddressConverter.convertToIpv6((java.net.InetAddress) val);
    }

    @Override
    public UUID getUUID(String colName) {
        return readValue(colName);
    }

    @Override
    public ClickHouseGeoPointValue getGeoPoint(String colName) {
        Object val = readValue(colName);
        return val == null ? null : ClickHouseGeoPointValue.of((double[]) val);
    }

    @Override
    public ClickHouseGeoRingValue getGeoRing(String colName) {
        Object val = readValue(colName);
        return val == null ? null : ClickHouseGeoRingValue.of((double[][]) val);
    }

    @Override
    public ClickHouseGeoPolygonValue getGeoPolygon(String colName) {
        Object val = readValue(colName);
        return val == null ? null : ClickHouseGeoPolygonValue.of((double[][][]) val);
    }

    @Override
    public ClickHouseGeoMultiPolygonValue getGeoMultiPolygon(String colName) {
        Object val = readValue(colName);
        return val == null ? null : ClickHouseGeoMultiPolygonValue.of((double[][][][]) val);
    }


    @Override
    public <T> List<T> getList(String colName) {
        Object value = readValue(colName);
        if (value == null) {
            return null;
        }
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
        if (array == null) {
            return null;
        }
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
        Object value = readValue(colName);
        if (value == null) {
            return null;
        }
        if (value instanceof BinaryStreamReader.ArrayValue) {
            BinaryStreamReader.ArrayValue array = (BinaryStreamReader.ArrayValue) value;
            int length = array.length;
            if (!array.itemType.equals(String.class))
                throw new ClientException("Not A String type.");
            String [] values = new String[length];
            for (int i = 0; i < length; i++) {
                values[i] = (String)((BinaryStreamReader.ArrayValue) value).get(i);
            }
            return values;
        }
        throw new ClientException("Not ArrayValue type.");
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
        ClickHouseColumn column = schema.getColumnByIndex(index);
        switch (column.getValueDataType()) {
            case DateTime:
            case DateTime64:
            case DateTime32:
                return readValue(index);
            case Dynamic:
            case Variant:
                Object value = readValue(index);
                if (value == null) {
                    return null;
                } else if (value instanceof ZonedDateTime) {
                    return (ZonedDateTime) value;
                }
                break;
        }
        throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to ZonedDateTime");
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
        Object val = readValue(index);
        return val == null ? null : InetAddressConverter.convertToIpv4((java.net.InetAddress) val);
    }

    @Override
    public Inet6Address getInet6Address(int index) {
        Object val = readValue(index);
        return val == null ? null : InetAddressConverter.convertToIpv6((java.net.InetAddress) val);
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
        Object val = readValue(colName);
        if (val == null) {
            throw new NullValueException("Column " + colName + " has null value and it cannot be cast to byte");
        }
        if (val instanceof BinaryStreamReader.EnumValue) {
            return ((BinaryStreamReader.EnumValue) val).byteValue();
        }
        return (byte) val;
    }

    @Override
    public byte getEnum8(int index) {
        return getEnum8(schema.columnIndexToName(index));
    }

    @Override
    public short getEnum16(String colName) {
        Object val = readValue(colName);
        if (val == null) {
            throw new NullValueException("Column " + colName + " has null value and it cannot be cast to short");
        }
        if (val instanceof BinaryStreamReader.EnumValue) {
            return ((BinaryStreamReader.EnumValue) val).shortValue();
        }
        return (short) val;
    }

    @Override
    public short getEnum16(int index) {
        return getEnum16(schema.columnIndexToName(index));
    }

    @Override
    public LocalDate getLocalDate(int index) {
        return getLocalDate(schema.columnIndexToName(index));
    }

    @Override
    public LocalDate getLocalDate(String colName) {
        ClickHouseColumn column = schema.getColumnByName(colName);
        switch(column.getValueDataType()) {
            case Date:
            case Date32:
                return (LocalDate) getObject(colName);
            case DateTime:
            case DateTime32:
            case DateTime64:
                LocalDateTime dt = getLocalDateTime(colName);
                return dt == null ? null : dt.toLocalDate();
            case Dynamic:
            case Variant:
                Object value = getObject(colName);
                LocalDate localDate = AbstractBinaryFormatReader.objectToLocalDate(value);
                if (value == null || localDate != null) {
                    return localDate;
                }
                break;
        }
        throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to LocalDate");
    }

    @Override
    public LocalTime getLocalTime(String colName) {
        ClickHouseColumn column = schema.getColumnByName(colName);
        switch(column.getValueDataType()) {
            case Time:
            case Time64:
                LocalDateTime val = (LocalDateTime) getObject(colName);
                return val == null ? null : val.toLocalTime();
            case DateTime:
            case DateTime32:
            case DateTime64:
                LocalDateTime dt = getLocalDateTime(colName);
                return dt == null ? null : dt.toLocalTime();
            case Dynamic:
            case Variant:
                Object value = getObject(colName);
                LocalTime localTime = AbstractBinaryFormatReader.objectToLocalTime(value);
                if (value == null || localTime != null) {
                    return localTime;
                }
                break;
        }
        throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to LocalTime");
    }

    @Override
    public LocalTime getLocalTime(int index) {
        return getLocalTime(schema.columnIndexToName(index));
    }

    @Override
    public LocalDateTime getLocalDateTime(String colName) {
        ClickHouseColumn column = schema.getColumnByName(colName);
        switch(column.getValueDataType()) {
            case Time:
            case Time64:
                // Types present wide range of value so LocalDateTime let to access to actual value
                return (LocalDateTime) getObject(colName);
            case DateTime:
            case DateTime32:
            case DateTime64:
                ZonedDateTime val = (ZonedDateTime) readValue(colName);
                return val == null ? null : val.toLocalDateTime();
            case Dynamic:
            case Variant:
                Object value = getObject(colName);
                LocalDateTime localDateTime = AbstractBinaryFormatReader.objectToLocalDateTime(value);
                if (value == null || localDateTime != null) {
                    return localDateTime;
                }
                break;
        }
        throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to LocalDateTime");
    }

    @Override
    public LocalDateTime getLocalDateTime(int index) {
       return getLocalDateTime(schema.columnIndexToName(index));
    }

    @Override
    public OffsetDateTime getOffsetDateTime(String colName) {
        ClickHouseColumn column = schema.getColumnByName(colName);
        switch(column.getValueDataType()) {
            case DateTime:
            case DateTime32:
            case DateTime64:
            case Dynamic:
            case Variant:
                ZonedDateTime val = getZonedDateTime(colName);
                return val == null ? null : val.toOffsetDateTime();
            default:
                throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to OffsetDataTime");
        }
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
