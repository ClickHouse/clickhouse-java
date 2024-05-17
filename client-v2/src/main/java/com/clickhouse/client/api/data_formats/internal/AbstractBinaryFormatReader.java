package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseArraySequence;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.value.ClickHouseArrayValue;
import com.clickhouse.data.value.ClickHouseGeoMultiPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoPointValue;
import com.clickhouse.data.value.ClickHouseGeoPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoRingValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractBinaryFormatReader implements ClickHouseBinaryFormatReader {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBinaryFormatReader.class);

    protected InputStream inputStream;

    protected ClickHouseInputStream chInputStream;

    protected Map<String, Object> settings;

    protected BinaryStreamReader binaryStreamReader;

    private List<Object> record;

    private TableSchema schema;

    protected volatile boolean hasNext = true;

    protected AbstractBinaryFormatReader(InputStream inputStream, QuerySettings querySettings, TableSchema schema) {
        this.inputStream = inputStream;
        this.chInputStream = inputStream instanceof ClickHouseInputStream ?
                (ClickHouseInputStream) inputStream : ClickHouseInputStream.of(inputStream);
        this.settings = new HashMap<>(querySettings.getAllSettings());
        this.binaryStreamReader = new BinaryStreamReader(chInputStream, LOG);
        setSchema(schema);
    }

    protected Map<String, Object> currentRecord = new ConcurrentHashMap<>();

    protected abstract void readRecord(Map<String, Object> record) throws IOException;

    @Override
    public void copyRecord(Map<String, Object> destination) throws IOException {
        destination.putAll(currentRecord);
    }

    @Override
    public <T> T readValue(int colIndex) throws IOException {
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
        if (hasNext) {
            try {
                return chInputStream.available() > 0;
            } catch (IOException e) {
                hasNext = false;
                LOG.error("Failed to check if there is more data available", e);
                return false;
            }
        }
        return false;
    }

    protected void setSchema(TableSchema schema) {
        this.schema = schema;
    }

    public TableSchema getSchema() {
        return schema;
    }

    @Override
    public String getString(String colName) {
        return readValue(colName);
    }

    @Override
    public Byte getByte(String colName) {
        return readValue(colName);
    }

    @Override
    public Short getShort(String colName) {
        return readValue(colName);
    }

    @Override
    public Integer getInteger(String colName) {
        return readValue(colName);
    }

    @Override
    public Long getLong(String colName) {
        return readValue(colName);
    }

    @Override
    public Float getFloat(String colName) {
        return readValue(colName);
    }

    @Override
    public Double getDouble(String colName) {
        return readValue(colName);
    }

    @Override
    public Boolean getBoolean(String colName) {
        return readValue(colName);
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
    public ClickHouseGeoPolygonValue asGeoPolygon(String colName) {
        return ClickHouseGeoPolygonValue.of(readValue(colName));
    }

    @Override
    public ClickHouseGeoMultiPolygonValue asGeoMultiPolygon(String colName) {
        return ClickHouseGeoMultiPolygonValue.of(readValue(colName));
    }


    @Override
    public <T> List<T> getList(String colName) {
        ClickHouseArrayValue<?> array = readValue(colName);
        return null;
    }

    @Override
    public <T> List<List<T>> getTwoDimensionalList(String colName) {
        return null;
    }

    @Override
    public <T> List<List<List<T>>> getThreeDimensionalList(String colName) {
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
}
