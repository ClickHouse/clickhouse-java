package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.internal.DataTypeConverter;
import com.clickhouse.client.api.internal.MapUtils;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metadata.NoSuchColumnException;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.NullValueException;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.serde.POJOFieldDeserializer;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.value.ClickHouseBitmap;
import com.clickhouse.data.value.ClickHouseGeoMultiPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoPointValue;
import com.clickhouse.data.value.ClickHouseGeoPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoRingValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractBinaryFormatReader implements ClickHouseBinaryFormatReader {

    public static final Map<ClickHouseDataType, Class<?>> NO_TYPE_HINT_MAPPING = Collections.emptyMap();

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBinaryFormatReader.class);

    protected InputStream input;

    protected BinaryStreamReader binaryStreamReader;

    protected DataTypeConverter dataTypeConverter;

    private TableSchema schema;
    private ClickHouseColumn[] columns;
    private Map[] convertions;
    private boolean hasNext = true;
    private boolean initialState = true; // reader is in initial state, no records have been read yet

    protected AbstractBinaryFormatReader(InputStream inputStream, QuerySettings querySettings, TableSchema schema,BinaryStreamReader.ByteBufferAllocator byteBufferAllocator, Map<ClickHouseDataType, Class<?>> defaultTypeHintMap) {
        this.input = inputStream;
        Map<String, Object> settings = querySettings == null ? Collections.emptyMap() : querySettings.getAllSettings();
        Boolean useServerTimeZone = (Boolean) settings.get(ClientConfigProperties.USE_SERVER_TIMEZONE.getKey());
        TimeZone timeZone = (useServerTimeZone == Boolean.TRUE && querySettings != null) ?
                querySettings.getServerTimeZone() :
                (TimeZone) settings.get(ClientConfigProperties.USE_TIMEZONE.getKey());
        if (timeZone == null) {
            throw new ClientException("Time zone is not set. (useServerTimezone:" + useServerTimeZone + ")");
        }
        boolean jsonAsString = MapUtils.getFlag(settings,
                ClientConfigProperties.serverSetting(ServerSettings.OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING), false);
        this.binaryStreamReader = new BinaryStreamReader(inputStream, timeZone, LOG, byteBufferAllocator, jsonAsString,
                defaultTypeHintMap);
        if (schema != null) {
            setSchema(schema);
        }
        this.dataTypeConverter = DataTypeConverter.INSTANCE; // singleton while no need to customize conversion
    }

    protected Object[] currentRecord;
    protected Object[] nextRecord;

    protected boolean nextRecordEmpty = true;

    /**
     * Reads next record into POJO object using set of serializers.
     * There should be a serializer for each column in the record, otherwise it will silently skip a field
     * It is done in such a way because it is not the reader concern. Calling code should validate this.
     *
     * Note: internal API
     * @param deserializers
     * @param obj
     * @return
     * @throws IOException
     */
    public boolean readToPOJO(Map<String, POJOFieldDeserializer> deserializers, Object obj ) throws IOException {
        if (columns == null || columns.length == 0) {
            return false;
        }

        boolean firstColumn = true;

        for (ClickHouseColumn column : columns) {
            try {
                POJOFieldDeserializer deserializer = deserializers.get(column.getColumnName());
                if (deserializer != null) {
                    deserializer.setValue(obj, binaryStreamReader, column);
                } else {
                    binaryStreamReader.skipValue(column);
                }
                firstColumn = false;
            } catch (EOFException e) {
                if (firstColumn) {
                    endReached();
                    return false;
                }
                throw e;
            } catch (Exception e) {
                throw new ClientException("Failed to set value of '" + column.getColumnName(), e);
            }
        }
        return true;
    }

    /**
     * It is still internal method and should be used with care.
     * Usually this method is called to read next record into internal object and affects hasNext() method.
     * So after calling this one:
     * - hasNext(), next() and get methods cannot be called
     * - stream should be read with readRecord() method fully
     *
     * @param record
     * @return
     * @throws IOException
     */
    public boolean readRecord(Map<String, Object> record) throws IOException {
        if (columns == null || columns.length == 0) {
            return false;
        }

        boolean firstColumn = true;
        for (ClickHouseColumn column : columns) {
            try {
                Object val = binaryStreamReader.readValue(column);
                if (val != null) {
                    record.put(column.getColumnName(), val);
                } else {
                    record.remove(column.getColumnName());
                }
                firstColumn = false;
            } catch (EOFException e) {
                if (firstColumn) {
                    endReached();
                    return false;
                }
                throw e;
            }
        }
        return true;
    }

    protected boolean readRecord(Object[] record) throws IOException {
        if (columns == null || columns.length == 0) {
            return false;
        }

        boolean firstColumn = true;
        for (int i = 0; i < columns.length; i++) {
            try {
                Object val = binaryStreamReader.readValue(columns[i]);
                if (val != null) {
                    record[i] = val;
                } else {
                    record[i] = null;
                }
                firstColumn = false;
            } catch (EOFException e) {
                if (firstColumn) {
                    endReached();
                    return false;
                }
                throw e;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T readValue(int colIndex) {
        if (colIndex < 1 || colIndex > getSchema().getColumns().size()) {
            throw new ClientException("Column index out of bounds: " + colIndex);
        }
        return (T) currentRecord[colIndex - 1];
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T readValue(String colName) {
        return (T) currentRecord[getSchema().nameToIndex(colName)];
    }

    @Override
    public boolean hasNext() {
        if (initialState) {
            readNextRecord();
        }

        return hasNext;
    }


    protected void readNextRecord() {
        initialState = false;
        try {
            nextRecordEmpty = true;
            if (!readRecord(nextRecord)) {
                endReached();
            } else {
                nextRecordEmpty = false;
            }
        } catch (IOException e) {
            endReached();
            throw new ClientException("Failed to read next row", e);
        }
    }

    @Override
    public Map<String, Object> next() {
        if (!hasNext) {
            return null;
        }

        if (!nextRecordEmpty) {
            Object[] tmp = currentRecord;
            currentRecord = nextRecord;
            nextRecord = tmp;
            readNextRecord();
            return new RecordWrapper(currentRecord, schema);
        } else {
            try {
                if (readRecord(currentRecord)) {
                    readNextRecord();
                    return new RecordWrapper(currentRecord, schema);
                } else {
                    currentRecord = null;
                    return null;
                }
            } catch (IOException e) {
                endReached();
                throw new ClientException("Failed to read row", e);
            }
        }
    }

    protected void endReached() {
        initialState = false;
        hasNext = false;
    }

    protected void setSchema(TableSchema schema) {
        this.schema = schema;
        this.columns = schema.getColumns().toArray(ClickHouseColumn.EMPTY_ARRAY);
        this.convertions = new Map[columns.length];

        this.currentRecord = new Object[columns.length];
        this.nextRecord = new Object[columns.length];

        for (int i = 0; i < columns.length; i++) {
            ClickHouseColumn column = columns[i];
            ClickHouseDataType columnDataType = column.getDataType();
            if (columnDataType.equals(ClickHouseDataType.SimpleAggregateFunction)){
                columnDataType = column.getNestedColumns().get(0).getDataType();
            }
            switch (columnDataType) {
                case Int8:
                case Int16:
                case UInt8:
                case Int32:
                case UInt16:
                case Int64:
                case UInt32:
                case Int128:
                case UInt64:
                case Int256:
                case UInt128:
                case UInt256:
                case Float32:
                case Float64:
                case Decimal:
                case Decimal32:
                case Decimal64:
                case Decimal128:
                case Decimal256:
                case Bool:
                case String:
                case Enum8:
                case Enum16:
                case Variant:
                case Dynamic:
                    this.convertions[i] = NumberConverter.NUMBER_CONVERTERS;
                    break;
                default:
                    this.convertions[i] = Collections.emptyMap();
            }
        }
    }

    public Map[] getConvertions() {
        return convertions;
    }

    @Override
    public TableSchema getSchema() {
        return schema;
    }

    @Override
    public String getString(String colName) {
        return getString(schema.nameToColumnIndex(colName));
    }

    @Override
    public String getString(int index) {
        ClickHouseColumn column = schema.getColumnByIndex(index);
        Object value;
        switch (column.getValueDataType()) {
            case Date:
            case Date32:
                value = getLocalDate(index);
                break;
            case Time:
            case Time64:
                value = getLocalTime(index);
                break;
            case DateTime:
            case DateTime32:
            case DateTime64:
                value = getLocalDateTime(index);
                break;
            default:
                value = readValue(index);
        }

        return dataTypeConverter.convertToString(value, column);
    }

    @SuppressWarnings("unchecked")
    private <T> T readNumberValue(int index, NumberConverter.NumberType targetType) {
        int colIndex = index - 1;
        Function<Object, Object> converter = (Function<Object, Object>) convertions[colIndex].get(targetType);
        if (converter != null) {
            Object value = readValue(index);
            if (value == null) {
                if (targetType == NumberConverter.NumberType.BigInteger || targetType == NumberConverter.NumberType.BigDecimal) {
                    return null;
                }
                throw new NullValueException("Column at index " + index + " has null value and it cannot be cast to " +
                        targetType.getTypeName());
            }
            return (T) converter.apply(value);
        } else {
            throw new ClientException("Column at index " + index + " " + columns[colIndex].getDataType().name() +
                    " cannot be converted to " + targetType.getTypeName());
        }
    }

    @Override
    public byte getByte(String colName) {
        return getByte(schema.nameToColumnIndex(colName));
    }

    @Override
    public short getShort(String colName) {
        return getShort(schema.nameToColumnIndex(colName));
    }

    @Override
    public int getInteger(String colName) {
        return getInteger(schema.nameToColumnIndex(colName));
    }

    @Override
    public long getLong(String colName) {
        return getLong(schema.nameToColumnIndex(colName));
    }

    @Override
    public float getFloat(String colName) {
        return getFloat(schema.nameToColumnIndex(colName));
    }

    @Override
    public double getDouble(String colName) {
        return getDouble(schema.nameToColumnIndex(colName));
    }

    @Override
    public boolean getBoolean(String colName) {
        return getBoolean(schema.nameToColumnIndex(colName));
    }

    @Override
    public BigInteger getBigInteger(String colName) {
        return getBigInteger(schema.nameToColumnIndex(colName));
    }

    @Override
    public BigDecimal getBigDecimal(String colName) {
        return getBigDecimal(schema.nameToColumnIndex(colName));
    }

    @Override
    public Instant getInstant(String colName) {
        return getInstant(getSchema().nameToColumnIndex(colName));
    }

    @Override
    public ZonedDateTime getZonedDateTime(String colName) {
        return getZonedDateTime(schema.nameToColumnIndex(colName));
    }

    @Override
    public Duration getDuration(String colName) {
        return getDuration(schema.nameToColumnIndex(colName));
    }

    @Override
    public TemporalAmount getTemporalAmount(String colName) {
        return getTemporalAmount(schema.nameToColumnIndex(colName));
    }

    @Override
    public Inet4Address getInet4Address(String colName) {
        return getInet4Address(schema.nameToColumnIndex(colName));
    }

    @Override
    public Inet6Address getInet6Address(String colName) {
        return getInet6Address(schema.nameToColumnIndex(colName));
    }

    @Override
    public UUID getUUID(String colName) {
        return getUUID(schema.nameToColumnIndex(colName));
    }

    @Override
    public ClickHouseGeoPointValue getGeoPoint(String colName) {
        return getGeoPoint(schema.nameToColumnIndex(colName));
    }

    @Override
    public ClickHouseGeoRingValue getGeoRing(String colName) {
        return getGeoRing(schema.nameToColumnIndex(colName));
    }

    @Override
    public ClickHouseGeoPolygonValue getGeoPolygon(String colName) {
        return getGeoPolygon(schema.nameToColumnIndex(colName));
    }

    @Override
    public ClickHouseGeoMultiPolygonValue getGeoMultiPolygon(String colName) {
        return getGeoMultiPolygon(schema.nameToColumnIndex(colName));
    }


    @Override
    public <T> List<T> getList(String colName) {
        return getList(schema.nameToColumnIndex(colName));
    }


    @SuppressWarnings("unchecked")
    private <T> T getPrimitiveArray(int index, Class<?> componentType) {
        try {
            Object value = readValue(index);
            if (value == null) {
                return null;
            }
            if (value instanceof BinaryStreamReader.ArrayValue) {
                BinaryStreamReader.ArrayValue array = (BinaryStreamReader.ArrayValue) value;
                if (array.itemType.isPrimitive()) {
                    return (T) array.array;
                } else {
                    throw new ClientException("Array is not of primitive type");
                }
            } else if (value instanceof List<?>) {
                List<?> list = (List<?>) value;
                Object array = Array.newInstance(componentType, list.size());
                for (int i = 0; i < list.size(); i++) {
                    Array.set(array, i, list.get(i));
                }
                return (T)array;
            } else if (componentType == byte.class) {
                if (value instanceof String) {
                    return (T) ((String) value).getBytes(StandardCharsets.UTF_8);
                } else if (value instanceof InetAddress) {
                    return (T) ((InetAddress) value).getAddress();
                }
            }
            throw new ClientException("Column is not of array type");
        } catch (ClassCastException e) {
            throw new ClientException("Column is not of array type", e);
        }
    }

    @Override
    public byte[] getByteArray(String colName) {
        return getByteArray(schema.nameToColumnIndex(colName));
    }

    @Override
    public int[] getIntArray(String colName) {
        return getIntArray(schema.nameToColumnIndex(colName));
    }

    @Override
    public long[] getLongArray(String colName) {
        return getLongArray(schema.nameToColumnIndex(colName));
    }

    @Override
    public float[] getFloatArray(String colName) {
        return getFloatArray(schema.nameToColumnIndex(colName));
    }

    @Override
    public double[] getDoubleArray(String colName) {
        return getDoubleArray(schema.nameToColumnIndex(colName));
    }

    @Override
    public boolean[] getBooleanArray(String colName) {
        return getBooleanArray(schema.nameToColumnIndex(colName));
    }

    @Override
    public short[] getShortArray(String colName) {
        return getShortArray(schema.nameToColumnIndex(colName));
    }

    @Override
    public String[] getStringArray(String colName) {
        return getStringArray(schema.nameToColumnIndex(colName));
    }

    @Override
    public Object[] getObjectArray(String colName) {
        return getObjectArray(schema.nameToColumnIndex(colName));
    }

    @Override
    public boolean hasValue(int colIndex) {
        if (colIndex < 1 || colIndex > currentRecord.length) {
            return false;
        }
        return currentRecord[colIndex - 1] != null;
    }

    @Override
    public boolean hasValue(String colName) {
        try {
            return hasValue(schema.nameToColumnIndex(colName));
        } catch (NoSuchColumnException e) {
            return false;
        }
    }

    @Override
    public byte getByte(int index) {
        return readNumberValue(index, NumberConverter.NumberType.Byte);
    }

    @Override
    public short getShort(int index) {
        return readNumberValue(index, NumberConverter.NumberType.Short);
    }

    @Override
    public int getInteger(int index) {
        return readNumberValue(index, NumberConverter.NumberType.Int);
    }

    @Override
    public long getLong(int index) {
        return readNumberValue(index, NumberConverter.NumberType.Long);
    }

    @Override
    public float getFloat(int index) {
        return readNumberValue(index, NumberConverter.NumberType.Float);
    }

    @Override
    public double getDouble(int index) {
        return readNumberValue(index, NumberConverter.NumberType.Double);
    }

    @Override
    public boolean getBoolean(int index) {
        return readNumberValue(index, NumberConverter.NumberType.Boolean);
    }

    @Override
    public BigInteger getBigInteger(int index) {
        return readNumberValue(index, NumberConverter.NumberType.BigInteger);
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        return readNumberValue(index, NumberConverter.NumberType.BigDecimal);
    }

    @Override
    public Instant getInstant(int index) {
        ClickHouseColumn column = schema.getColumnByIndex(index);
        switch (column.getValueDataType()) {
            case Date:
            case Date32:
                LocalDate date = getLocalDate(index);
                return date == null ? null : date.atStartOfDay(ZoneId.of("UTC")).toInstant();
            case Time:
            case Time64:
                LocalDateTime dt = getLocalDateTime(index);
                return dt == null ? null : dt.toInstant(ZoneOffset.UTC);
            case DateTime:
            case DateTime64:
            case DateTime32:
                ZonedDateTime zdt = readValue(index);
                return zdt.toInstant();
            case Dynamic:
            case Variant:
                Object value = readValue(index);
                Instant instant = objectToInstant(value);
                if (value == null || instant != null) {
                    return instant;
                }
                break;
        }
        throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to Instant");
    }

    static Instant objectToInstant(Object value) {
        if (value instanceof LocalDateTime) {
            LocalDateTime dateTime = (LocalDateTime) value;
            return Instant.from(dateTime.atZone(ZoneId.of("UTC")));
        } else if (value instanceof ZonedDateTime) {
            ZonedDateTime dateTime = (ZonedDateTime) value;
            return dateTime.toInstant();
        }
        return null;
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
        TemporalAmount temporalAmount = getTemporalAmount(index);
        return temporalAmount == null ? null : Duration.from(temporalAmount);
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
        Object val = readValue(index);
        return val == null ? null : ClickHouseGeoPointValue.of((double[]) val);
    }

    @Override
    public ClickHouseGeoRingValue getGeoRing(int index) {
        Object val = readValue(index);
        return val == null ? null : ClickHouseGeoRingValue.of((double[][]) val);
    }

    @Override
    public ClickHouseGeoPolygonValue getGeoPolygon(int index) {
        Object val = readValue(index);
        return val == null ? null : ClickHouseGeoPolygonValue.of((double[][][]) val);
    }

    @Override
    public ClickHouseGeoMultiPolygonValue getGeoMultiPolygon(int index) {
        Object val = readValue(index);
        return val == null ? null : ClickHouseGeoMultiPolygonValue.of((double[][][][]) val);
    }

    @Override
    public <T> List<T> getList(int index) {
        Object value = readValue(index);
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

    @Override
    public byte[] getByteArray(int index) {
        try {
            return getPrimitiveArray(index, byte.class);
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ClientException("Value cannot be converted to an array of primitives", e);
        }
    }

    @Override
    public int[] getIntArray(int index) {
        try {
            return getPrimitiveArray(index, int.class);
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ClientException("Value cannot be converted to an array of primitives", e);
        }
    }

    @Override
    public long[] getLongArray(int index) {
        try {
            return getPrimitiveArray(index, long.class);
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ClientException("Value cannot be converted to an array of primitives", e);
        }
    }

    @Override
    public float[] getFloatArray(int index) {
        try {
            return getPrimitiveArray(index, float.class);
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ClientException("Value cannot be converted to an array of primitives", e);
        }
    }

    @Override
    public double[] getDoubleArray(int index) {
        try {
            return getPrimitiveArray(index, double.class);
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ClientException("Value cannot be converted to an array of primitives", e);
        }
    }

    @Override
    public boolean[] getBooleanArray(int index) {
        try {
            return getPrimitiveArray(index, boolean.class);
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ClientException("Value cannot be converted to an array of primitives", e);
        }
    }

    @Override
    public short[] getShortArray(int index) {
        try {
            return getPrimitiveArray(index, short.class);
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ClientException("Value cannot be converted to an array of primitives", e);
        }
    }

    @Override
    public String[] getStringArray(int index) {
        Object value = readValue(index);
        if (value == null) {
            return null;
        }
        if (value instanceof BinaryStreamReader.ArrayValue) {
            BinaryStreamReader.ArrayValue array = (BinaryStreamReader.ArrayValue) value;
            if (array.itemType == String.class) {
                return (String[]) array.getArray();
            } else if (array.itemType == BinaryStreamReader.EnumValue.class) {
                BinaryStreamReader.EnumValue[] enumValues = (BinaryStreamReader.EnumValue[]) array.getArray();
                return Arrays.stream(enumValues).map(BinaryStreamReader.EnumValue::getName).toArray(String[]::new);
            } else {
                throw new ClientException("Not an array of strings");
            }
        }
        throw new ClientException("Column is not of array type");
    }

    @Override
    public Object[] getObjectArray(int index) {
        Object value = readValue(index);
        if (value == null) {
            return null;
        }
        if (value instanceof BinaryStreamReader.ArrayValue) {
            return ((BinaryStreamReader.ArrayValue) value).toObjectArray();
        } else if (value instanceof List<?>) {
            return ((List<?>) value).toArray(new Object[0]);
        }
        throw new ClientException("Column is not of array type");
    }

    @Override
    public Object[] getTuple(int index) {
        return readValue(index);
    }

    @Override
    public Object[] getTuple(String colName) {
        return getTuple(schema.nameToColumnIndex(colName));
    }

    @Override
    public byte getEnum8(String colName) {
        return getEnum8(schema.nameToColumnIndex(colName));
    }

    @Override
    public byte getEnum8(int index) {
        BinaryStreamReader.EnumValue enumValue = readValue(index);
        if (enumValue == null) {
            throw new NullValueException("Column at index " + index + " has null value and it cannot be converted to enum8 numeric value");
        }
        return enumValue.byteValue();
    }

    @Override
    public short getEnum16(String colName) {
        return getEnum16(schema.nameToColumnIndex(colName));
    }

    @Override
    public short getEnum16(int index) {
        BinaryStreamReader.EnumValue enumValue = readValue(index);
        if (enumValue == null) {
            throw new NullValueException("Column at index " + index + " has null value and it cannot be converted to enum16 numeric value");
        }
        return enumValue.shortValue();
    }

    @Override
    public LocalDate getLocalDate(String colName) {
        return getLocalDate(schema.nameToColumnIndex(colName));
    }

    @Override
    public LocalDate getLocalDate(int index) {
        ClickHouseColumn column = schema.getColumnByIndex(index);
        switch(column.getValueDataType()) {
            case Date:
            case Date32:
                return readValue(index);
            case DateTime:
            case DateTime32:
            case DateTime64:
                ZonedDateTime zdt = readValue(index);
                return zdt == null ? null : zdt.toLocalDate();
            case Dynamic:
            case Variant:
                Object value = readValue(index);
                LocalDate localDate = objectToLocalDate(value);
                if (value == null || localDate != null) {
                    return localDate;
                }
                break;
        }
        throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to LocalDate");
    }

    static LocalDate objectToLocalDate(Object value) {
        if (value instanceof LocalDate) {
            return (LocalDate) value;
        } else if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime)value).toLocalDate();
        } else if (value instanceof LocalDateTime) {
            return ((LocalDateTime)value).toLocalDate();
        }
        return null;
    }

    @Override
    public LocalTime getLocalTime(String colName) {
        return getLocalTime(schema.nameToColumnIndex(colName));
    }

    @Override
    public LocalTime getLocalTime(int index) {
        ClickHouseColumn column = schema.getColumnByIndex(index);
        switch(column.getValueDataType()) {
            case Time:
            case Time64:
                LocalDateTime dt = readValue(index);
                return dt == null ? null : dt.toLocalTime();
            case DateTime:
            case DateTime32:
            case DateTime64:
                ZonedDateTime zdt = readValue(index);
                return zdt == null ? null : zdt.toLocalTime();
            case Dynamic:
            case Variant:
                Object value = readValue(index);
                LocalTime localTime = objectToLocalTime(value);
                if (value == null || localTime != null) {
                    return localTime;
                }
                break;
        }
        throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to LocalTime");
    }

    static LocalTime objectToLocalTime(Object value) {
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime)value).toLocalTime();
        } else if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime)value).toLocalTime();
        }
        return null;
    }

    @Override
    public LocalDateTime getLocalDateTime(String colName) {
        return getLocalDateTime(schema.nameToColumnIndex(colName));
    }

    @Override
    public LocalDateTime getLocalDateTime(int index) {
        ClickHouseColumn column = schema.getColumnByIndex(index);
        switch(column.getValueDataType()) {
            case Time:
            case Time64:
                return readValue(index);
            case DateTime:
            case DateTime32:
            case DateTime64:
                ZonedDateTime zdt = readValue(index);
                return zdt == null ? null : zdt.toLocalDateTime();
            case Dynamic:
            case Variant:
                Object value = readValue(index);
                LocalDateTime ldt = objectToLocalDateTime(value);
                if (value == null || ldt != null) {
                    return ldt;
                }
                break;

        }
        throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to LocalDateTime");
    }

    static LocalDateTime objectToLocalDateTime(Object value) {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        } else if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime)value).toLocalDateTime();
        }

        return null;
    }

    @Override
    public OffsetDateTime getOffsetDateTime(String colName) {
       return getOffsetDateTime(schema.nameToColumnIndex(colName));
    }

    @Override
    public OffsetDateTime getOffsetDateTime(int index) {
        ClickHouseColumn column = schema.getColumnByIndex(index);
        switch(column.getValueDataType()) {
            case DateTime:
            case DateTime32:
            case DateTime64:
                ZonedDateTime zdt = readValue(index);
                return zdt == null ? null : zdt.toOffsetDateTime();
            case Dynamic:
            case Variant:
                Object value = readValue(index);
                if (value == null) {
                    return null;
                } else if (value instanceof ZonedDateTime) {
                    return ((ZonedDateTime) value).toOffsetDateTime();
                }

        }
        throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to OffsetDateTime");
    }

    @Override
    public ClickHouseBitmap getClickHouseBitmap(String colName) {
        return getClickHouseBitmap(schema.nameToColumnIndex(colName));
    }

    @Override
    public ClickHouseBitmap getClickHouseBitmap(int index) {
        return readValue(index);
    }

    @Override
    public void close() throws Exception {
        input.close();
    }

    private static class RecordWrapper implements Map<String, Object> {

        private final WeakReference<Object[]> recordRef;

        private final WeakReference<TableSchema> schemaRef;

        int size;
        public RecordWrapper(Object[] record, TableSchema schema) {
            this.recordRef = new WeakReference<>(record);
            this.schemaRef = new WeakReference<>(schema);
            this.size = record.length;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean isEmpty() {
            return size == 0;
        }

        @Override
        @SuppressWarnings("ConstantConditions")
        public boolean containsKey(Object key) {
            if (key instanceof String) {
                return recordRef.get()[schemaRef.get().nameToIndex((String)key)] != null;
            }
            return false;
        }

        @Override
        public boolean containsValue(Object value) {
            for (Object obj : recordRef.get()) {
                if (obj == value) {
                    return true;
                }
            }
            return false;
        }

        @Override
        @SuppressWarnings("ConstantConditions")
        public Object get(Object key) {
            if (key instanceof String) {
                 try {
                     int index = schemaRef.get().nameToIndex((String) key);
                     if (index < size) {
                         return recordRef.get()[index];
                     }
                 } catch (NoSuchColumnException e) {
                     return null;
                 }
            }

            return null;
        }

        @Override
        public Object put(String key, Object value) {
            throw new UnsupportedOperationException("Record is read-only");
        }

        @Override
        public Object remove(Object key) {
            throw new UnsupportedOperationException("Record is read-only");
        }

        @Override
        public void putAll(Map<? extends String, ?> m) {
            throw new UnsupportedOperationException("Record is read-only");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("Record is read-only");
        }

        @Override
        @SuppressWarnings("ConstantConditions")
        public Set<String> keySet() {
            // TODO: create a view in Schema
            return schemaRef.get().getColumns().stream().map(ClickHouseColumn::getColumnName).collect(Collectors.toSet());
        }

        @Override
        @SuppressWarnings("ConstantConditions")
        public Collection<Object> values() {
            return Arrays.asList(recordRef.get());
        }

        @Override
        @SuppressWarnings("ConstantConditions")
        public Set<Entry<String, Object>> entrySet() {
            int i = 0;
            Set<Entry<String, Object>> entrySet = new HashSet<>();
            for (ClickHouseColumn column : schemaRef.get().getColumns()) {
                entrySet.add( new AbstractMap.SimpleImmutableEntry(column.getColumnName(), recordRef.get()[i++]));
            }
            return entrySet;
        }
    }
}
