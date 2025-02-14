package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.internal.MapUtils;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.NullValueException;
import com.clickhouse.client.api.query.POJOSetter;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseEnum;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public abstract class AbstractBinaryFormatReader implements ClickHouseBinaryFormatReader {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBinaryFormatReader.class);

    protected InputStream input;

    protected Map<String, Object> settings;

    protected BinaryStreamReader binaryStreamReader;

    private TableSchema schema;
    private ClickHouseColumn[] columns;
    private Map[] convertions;
    private volatile boolean hasNext = true;
    private volatile boolean initialState = true; // reader is in initial state, no records have been read yet

    protected AbstractBinaryFormatReader(InputStream inputStream, QuerySettings querySettings, TableSchema schema,
                                         BinaryStreamReader.ByteBufferAllocator byteBufferAllocator) {
        this.input = inputStream;
        this.settings = querySettings == null ? Collections.emptyMap() : new HashMap<>(querySettings.getAllSettings());
        Boolean useServerTimeZone = (Boolean) this.settings.get(ClientConfigProperties.USE_SERVER_TIMEZONE.getKey());
        TimeZone timeZone = useServerTimeZone == Boolean.TRUE && querySettings != null ? querySettings.getServerTimeZone() :
                (TimeZone) this.settings.get(ClientConfigProperties.USE_TIMEZONE.getKey());
        if (timeZone == null) {
            throw new ClientException("Time zone is not set. (useServerTimezone:" + useServerTimeZone + ")");
        }
        boolean jsonAsString = MapUtils.getFlag(this.settings,
                ClientConfigProperties.serverSetting(ServerSettings.OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING), false);
        this.binaryStreamReader = new BinaryStreamReader(inputStream, timeZone, LOG, byteBufferAllocator, jsonAsString);
        if (schema != null) {
            setSchema(schema);
        }
    }

    protected Map<String, Object> currentRecord = new ConcurrentHashMap<>();
    protected Map<String, Object> nextRecord = new ConcurrentHashMap<>();

    protected AtomicBoolean nextRecordEmpty = new AtomicBoolean(true);

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
    public boolean readToPOJO(Map<String, POJOSetter> deserializers, Object obj ) throws IOException {
        boolean firstColumn = true;

        for (ClickHouseColumn column : columns) {
            try {
                POJOSetter deserializer = deserializers.get(column.getColumnName());
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
     * - hasNext(), next() should not be called
     * - stream should be read with readRecord() method fully
     *
     * @param record
     * @return
     * @throws IOException
     */
    public boolean readRecord(Map<String, Object> record) throws IOException {
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

    @Override
    public <T> T readValue(int colIndex) {
        if (colIndex < 1 || colIndex > getSchema().getColumns().size()) {
            throw new ClientException("Column index out of bounds: " + colIndex);
        }
        return (T) currentRecord.get(getSchema().columnIndexToName(colIndex));
    }

    @Override
    public <T> T readValue(String colName) {
        return (T) currentRecord.get(colName);
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
            nextRecordEmpty.set(true);
            if (!readRecord(nextRecord)) {
                endReached();
            } else {
                nextRecordEmpty.compareAndSet(true, false);
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

        if (!nextRecordEmpty.get()) {
            Map<String, Object> tmp = currentRecord;
            currentRecord = nextRecord;
            nextRecord = tmp;
            readNextRecord();
            return currentRecord;
        } else {
            try {
                if (readRecord(currentRecord)) {
                    readNextRecord();
                    return currentRecord;
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

        for (int i = 0; i < columns.length; i++) {
            ClickHouseColumn column = columns[i];

            switch (column.getDataType()) {
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
        return readAsString(readValue(colName), schema.getColumnByName(colName));
    }

    /**
     * Converts value in to a string representation. Does some formatting for selected data types
     * @return string representation of a value for specified column
     */
    public static String readAsString(Object value, ClickHouseColumn column) {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            return (String) value;
        } else if (value instanceof ZonedDateTime) {
            ClickHouseDataType dataType = column.getDataType();
            ZonedDateTime zdt = (ZonedDateTime) value;
            if (dataType == ClickHouseDataType.Date) {
                return zdt.format(com.clickhouse.client.api.DataTypeUtils.DATE_FORMATTER);
            }
            return value.toString();
        } else if (value instanceof BinaryStreamReader.EnumValue) {
            return ((BinaryStreamReader.EnumValue)value).name;
        } else if (value instanceof Number ) {
            ClickHouseDataType dataType = column.getDataType();
            int num = ((Number) value).intValue();
            if (column.getDataType() == ClickHouseDataType.Variant) {
                for (ClickHouseColumn c : column.getNestedColumns()) {
                    // TODO: will work only if single enum listed as variant
                    if (c.getDataType() == ClickHouseDataType.Enum8 || c.getDataType() == ClickHouseDataType.Enum16) {
                        return c.getEnumConstants().name(num);
                    }
                }
            } else if (dataType == ClickHouseDataType.Enum8 || dataType == ClickHouseDataType.Enum16) {
                return column.getEnumConstants().name(num);
            }
        } else if (value instanceof BinaryStreamReader.ArrayValue) {
            return ((BinaryStreamReader.ArrayValue)value).asList().toString();
        }
        return value.toString();
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
        switch (column.getDataType()) {
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
            default:
                throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to Instant");
        }
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
            default:
                throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to Instant");
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
        try {
            BinaryStreamReader.ArrayValue array = readValue(colName);
            return array.asList();
        } catch (ClassCastException e) {
            throw new ClientException("Column is not of array type", e);
        }
    }


    private <T> T getPrimitiveArray(String colName) {
        try {
            BinaryStreamReader.ArrayValue array = readValue(colName);
            if (array.itemType.isPrimitive()) {
                return (T) array.array;
            } else {
                throw new ClientException("Array is not of primitive type");
            }
        } catch (ClassCastException e) {
            throw new ClientException("Column is not of array type", e);
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
        return currentRecord.containsKey(getSchema().columnIndexToName(colIndex));
    }

    @Override
    public boolean hasValue(String colName) {
        getSchema().getColumnByName(colName);
        return currentRecord.containsKey(colName);
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
        return readValue(index);
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
        Object value = readValue(index);
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
    public OffsetDateTime getOffsetDateTime(String colName) {
        Object value = readValue(colName);
        if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).toOffsetDateTime();
        }
        return (OffsetDateTime) value;
    }

    @Override
    public OffsetDateTime getOffsetDateTime(int index) {
        Object value = readValue(index);
        if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).toOffsetDateTime();
        }
        return (OffsetDateTime) value;
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
}
