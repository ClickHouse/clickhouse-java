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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
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
                throw new ClientException("Failed to put value of '" + column.getColumnName() + "' into POJO", e);
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
        colIndex = colIndex - 1;
        return (T) currentRecord.get(getSchema().indexToName(colIndex));
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
        this.columns = schema.getColumns().toArray(new ClickHouseColumn[0]);
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
        return currentRecord.containsKey(getSchema().indexToName(colIndex - 1));
    }

    @Override
    public boolean hasValue(String colName) {
        getSchema().getColumnByName(colName);
        return currentRecord.containsKey(colName);
    }

    @Override
    public byte getByte(int index) {
        return getByte(schema.indexToName(index - 1 ));
    }

    @Override
    public short getShort(int index) {
        return getShort(schema.indexToName(index - 1));
    }

    @Override
    public int getInteger(int index) {
        return getInteger(schema.indexToName(index - 1));
    }

    @Override
    public long getLong(int index) {
        return getLong(schema.indexToName(index - 1));
    }

    @Override
    public float getFloat(int index) {
        return getFloat(schema.indexToName(index - 1));
    }

    @Override
    public double getDouble(int index) {
        return getDouble(schema.indexToName(index - 1));
    }

    @Override
    public boolean getBoolean(int index) {
        return getBoolean(schema.indexToName(index - 1));
    }

    @Override
    public BigInteger getBigInteger(int index) {
        return getBigInteger(schema.indexToName(index - 1));
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        return getBigDecimal(schema.indexToName(index - 1));
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
        return getList(schema.indexToName(index));
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
