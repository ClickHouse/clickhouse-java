package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.DataTypeUtils;
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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
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

    @Override
    public <T> T readValue(int colIndex) {
        if (colIndex < 1 || colIndex > getSchema().getColumns().size()) {
            throw new ClientException("Column index out of bounds: " + colIndex);
        }
        return (T) currentRecord[colIndex - 1];
    }

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
                case Time:
                case Time64:
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
        return dataTypeConverter.convertToString(readValue(colName), schema.getColumnByName(colName));
    }

    @Override
    public String getString(int index) {
        return getString(schema.columnIndexToName(index));
    }

    private <T> T readNumberValue(String colName, NumberConverter.NumberType targetType) {
        int colIndex = schema.nameToIndex(colName);
        Function<Object, Object> converter = (Function<Object, Object>) convertions[colIndex].get(targetType);
        if (converter != null) {
            Object value = readValue(colName);
            if (value == null) {
                throw new NullValueException("Column " + colName + " has null value and it cannot be cast to " +
                        targetType.getTypeName());
            }
            return (T) converter.apply(value);
        } else {
            throw new ClientException("Column " + colName + " " + columns[colIndex].getDataType().name() +
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
        ClickHouseDataType columnDataType = column.getDataType();
        if (columnDataType.equals(ClickHouseDataType.SimpleAggregateFunction)){
            columnDataType = column.getNestedColumns().get(0).getDataType();
        }
        switch (columnDataType) {
            case Date:
            case Date32:
                LocalDate data = readValue(colName);
                return data.atStartOfDay().toInstant(ZoneOffset.UTC);
            case DateTime:
            case DateTime64:
                Object colValue = readValue(colName);
                if (colValue instanceof LocalDateTime) {
                    LocalDateTime dateTime = (LocalDateTime) colValue;
                    return dateTime.toInstant(column.getTimeZone().toZoneId().getRules().getOffset(dateTime));
                } else {
                    ZonedDateTime dateTime = (ZonedDateTime) colValue;
                    return dateTime.toInstant();
                }
            case Time:
                return Instant.ofEpochSecond(getLong(colName));
            case Time64:
                return DataTypeUtils.instantFromTime64Integer(column.getScale(), getLong(colName));
            default:
                throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to Instant");
        }
    }

    @Override
    public ZonedDateTime getZonedDateTime(String colName) {
        int colIndex = schema.nameToIndex(colName);
        ClickHouseColumn column = schema.getColumns().get(colIndex);
        ClickHouseDataType columnDataType = column.getDataType();
        if (columnDataType.equals(ClickHouseDataType.SimpleAggregateFunction)){
            columnDataType = column.getNestedColumns().get(0).getDataType();
        }
        switch (columnDataType) {
            case DateTime:
            case DateTime64:
            case Date:
            case Date32:
                return readValue(colName);
            default:
                throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to ZonedDateTime");
        }
    }

    @Override
    public Duration getDuration(String colName) {
        TemporalAmount temporalAmount = getTemporalAmount(colName);
        return Duration.from(temporalAmount);
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


    private <T> T getPrimitiveArray(String colName, Class<?> componentType) {
        try {
            Object value = readValue(colName);
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
            }
            throw new ClientException("Column is not of array type");
        } catch (ClassCastException e) {
            throw new ClientException("Column is not of array type", e);
        }
    }

    @Override
    public byte[] getByteArray(String colName) {
        try {
            return getPrimitiveArray(colName, byte.class);
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ClientException("Value cannot be converted to an array of primitives", e);
        }
    }

    @Override
    public int[] getIntArray(String colName) {
        try {
            return getPrimitiveArray(colName, int.class);
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ClientException("Value cannot be converted to an array of primitives", e);
        }
    }

    @Override
    public long[] getLongArray(String colName) {
        try {
            return getPrimitiveArray(colName,  long.class);
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ClientException("Value cannot be converted to an array of primitives", e);
        }
    }

    @Override
    public float[] getFloatArray(String colName) {
        try {
            return getPrimitiveArray(colName,  float.class);
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ClientException("Value cannot be converted to an array of primitives", e);
        }
    }

    @Override
    public double[] getDoubleArray(String colName) {
        try {
            return getPrimitiveArray(colName,  double.class);
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ClientException("Value cannot be converted to an array of primitives", e);
        }
    }

    @Override
    public boolean[] getBooleanArray(String colName) {
        try {
            return getPrimitiveArray(colName,  boolean.class);
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ClientException("Value cannot be converted to an array of primitives", e);
        }
    }

    @Override
    public short[] getShortArray(String colName) {
        try {
            return getPrimitiveArray(colName,  short.class);
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ClientException("Value cannot be converted to an array of primitives", e);
        }
    }

    @Override
    public String[] getStringArray(String colName) {
        Object value = readValue(colName);
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
        return currentRecord[colIndex - 1] != null;
    }

    @Override
    public boolean hasValue(String colName) {
        return currentRecord[getSchema().nameToIndex(colName)] != null;
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
        return readValue(index);
    }

    @Override
    public Duration getDuration(int index) {
        return getDuration(schema.columnIndexToName(index));
    }

    @Override
    public TemporalAmount getTemporalAmount(int index) {
        return getTemporalAmount(schema.columnIndexToName(index));
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
       return getByteArray(schema.columnIndexToName(index));
    }

    @Override
    public int[] getIntArray(int index) {
        return getIntArray(schema.columnIndexToName(index));
    }

    @Override
    public long[] getLongArray(int index) {
        return  getLongArray(schema.columnIndexToName(index));
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
    public Object[] getTuple(int index) {
        return readValue(index);
    }

    @Override
    public Object[] getTuple(String colName) {
        return readValue(colName);
    }

    @Override
    public byte getEnum8(String colName) {
        BinaryStreamReader.EnumValue enumValue = readValue(colName);
        return enumValue.byteValue();
    }

    @Override
    public byte getEnum8(int index) {
        return getEnum8(schema.columnIndexToName(index));
    }

    @Override
    public short getEnum16(String colName) {
        BinaryStreamReader.EnumValue enumValue = readValue(colName);
        return enumValue.shortValue();
    }

    @Override
    public short getEnum16(int index) {
        return getEnum16(schema.columnIndexToName(index));
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
    public LocalDate getLocalDate(int index) {
       return getLocalDate(schema.columnIndexToName(index));
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
