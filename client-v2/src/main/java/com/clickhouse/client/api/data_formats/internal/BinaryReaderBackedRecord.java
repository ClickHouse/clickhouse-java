package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.value.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.*;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class BinaryReaderBackedRecord implements GenericRecord {

    private final ClickHouseBinaryFormatReader reader;

    public BinaryReaderBackedRecord(ClickHouseBinaryFormatReader reader) {
        this.reader = reader;
    }

    @Override
    public String getString(String colName) {
        return reader.getString(colName);
    }

    @Override
    public byte getByte(String colName) {
        return reader.getByte(colName);
    }

    @Override
    public short getShort(String colName) {
        return reader.getShort(colName);
    }

    @Override
    public int getInteger(String colName) {
        return reader.getInteger(colName);
    }

    @Override
    public long getLong(String colName) {
        return reader.getLong(colName);
    }

    @Override
    public float getFloat(String colName) {
        return reader.getFloat(colName);
    }

    @Override
    public double getDouble(String colName) {
        return reader.getDouble(colName);
    }

    @Override
    public boolean getBoolean(String colName) {
        return reader.getBoolean(colName);
    }

    @Override
    public BigInteger getBigInteger(String colName) {
        return reader.getBigInteger(colName);
    }

    @Override
    public BigDecimal getBigDecimal(String colName) {
        return reader.getBigDecimal(colName);
    }

    @Override
    public Instant getInstant(String colName) {
        return reader.getInstant(colName);
    }

    @Override
    public ZonedDateTime getZonedDateTime(String colName) {
        return reader.getZonedDateTime(colName);
    }

    @Override
    public Duration getDuration(String colName) {
        return reader.getDuration(colName);
    }

    @Override
    public TemporalAmount getTemporalAmount(String colName) {
        return reader.getTemporalAmount(colName);
    }

    @Override
    public Inet4Address getInet4Address(String colName) {
        return reader.getInet4Address(colName);
    }

    @Override
    public Inet6Address getInet6Address(String colName) {
        return reader.getInet6Address(colName);
    }

    @Override
    public UUID getUUID(String colName) {
        return reader.getUUID(colName);
    }

    @Override
    public ClickHouseGeoPointValue getGeoPoint(String colName) {
        return reader.getGeoPoint(colName);
    }

    @Override
    public ClickHouseGeoRingValue getGeoRing(String colName) {
        return reader.getGeoRing(colName);
    }

    @Override
    public ClickHouseGeoPolygonValue getGeoPolygon(String colName) {
        return reader.getGeoPolygon(colName);
    }

    @Override
    public ClickHouseGeoMultiPolygonValue getGeoMultiPolygon(String colName) {
        return reader.getGeoMultiPolygon(colName);
    }

    @Override
    public <T> List<T> getList(String colName) {
        return reader.getList(colName);
    }

    @Override
    public byte[] getByteArray(String colName) {
        return reader.getByteArray(colName);
    }

    @Override
    public int[] getIntArray(String colName) {
        return reader.getIntArray(colName);
    }

    @Override
    public long[] getLongArray(String colName) {
        return reader.getLongArray(colName);
    }

    @Override
    public float[] getFloatArray(String colName) {
        return reader.getFloatArray(colName);
    }

    @Override
    public double[] getDoubleArray(String colName) {
        return reader.getDoubleArray(colName);
    }

    @Override
    public boolean[] getBooleanArray(String colName) {
        return reader.getBooleanArray(colName);
    }

    @Override
    public String getString(int index) {
        return reader.getString(index);
    }

    @Override
    public boolean hasValue(int colIndex) {
        return reader.hasValue(colIndex);
    }

    @Override
    public boolean hasValue(String colName) {
        return reader.hasValue(colName);
    }

    @Override
    public byte getByte(int index) {
        return reader.getByte(index);
    }

    @Override
    public short getShort(int index) {
        return reader.getShort(index);
    }

    @Override
    public int getInteger(int index) {
        return reader.getInteger(index);
    }

    @Override
    public long getLong(int index) {
        return reader.getLong(index);
    }

    @Override
    public float getFloat(int index) {
        return reader.getFloat(index);
    }

    @Override
    public double getDouble(int index) {
        return reader.getDouble(index);
    }

    @Override
    public boolean getBoolean(int index) {
        return reader.getBoolean(index);
    }

    @Override
    public BigInteger getBigInteger(int index) {
        return reader.getBigInteger(index);
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        return reader.getBigDecimal(index);
    }

    @Override
    public Instant getInstant(int index) {
        return reader.getInstant(index);
    }

    @Override
    public ZonedDateTime getZonedDateTime(int index) {
        return reader.getZonedDateTime(index);
    }

    @Override
    public Duration getDuration(int index) {
        return reader.getDuration(index);
    }

    @Override
    public TemporalAmount getTemporalAmount(int index) {
        return reader.getTemporalAmount(index);
    }

    @Override
    public Inet4Address getInet4Address(int index) {
        return reader.getInet4Address(index);
    }

    @Override
    public Inet6Address getInet6Address(int index) {
        return reader.getInet6Address(index);
    }

    @Override
    public UUID getUUID(int index) {
        return reader.getUUID(index);
    }

    @Override
    public ClickHouseGeoPointValue getGeoPoint(int index) {
        return reader.getGeoPoint(index);
    }

    @Override
    public ClickHouseGeoRingValue getGeoRing(int index) {
        return reader.getGeoRing(index);
    }

    @Override
    public ClickHouseGeoPolygonValue getGeoPolygon(int index) {
        return reader.getGeoPolygon(index);
    }

    @Override
    public ClickHouseGeoMultiPolygonValue getGeoMultiPolygon(int index) {
        return reader.getGeoMultiPolygon(index);
    }

    @Override
    public <T> List<T> getList(int index) {
        return reader.getList(index);
    }

    @Override
    public byte[] getByteArray(int index) {
        return reader.getByteArray(index);
    }

    @Override
    public int[] getIntArray(int index) {
        return reader.getIntArray(index);
    }

    @Override
    public long[] getLongArray(int index) {
        return reader.getLongArray(index);
    }

    @Override
    public float[] getFloatArray(int index) {
        return reader.getFloatArray(index);
    }

    @Override
    public double[] getDoubleArray(int index) {
        return reader.getDoubleArray(index);
    }

    @Override
    public boolean[] getBooleanArray(int index) {
        return reader.getBooleanArray(index);
    }

    @Override
    public Object[] getTuple(int index) {
        return reader.getTuple(index);
    }

    @Override
    public Object[] getTuple(String colName) {
        return reader.getTuple(colName);
    }

    @Override
    public byte getEnum8(String colName) {
        return reader.getEnum8(colName);
    }

    @Override
    public byte getEnum8(int index) {
        return reader.getEnum8(index);
    }

    @Override
    public short getEnum16(String colName) {
        return reader.getEnum16(colName);
    }

    @Override
    public short getEnum16(int index) {
        return reader.getEnum16(index);
    }

    @Override
    public LocalDate getLocalDate(String colName) {
        return reader.getLocalDate(colName);
    }

    @Override
    public LocalDate getLocalDate(int index) {
        return reader.getLocalDate(index);
    }

    @Override
    public LocalDateTime getLocalDateTime(String colName) {
        return reader.getLocalDateTime(colName);
    }

    @Override
    public LocalDateTime getLocalDateTime(int index) {
        return reader.getLocalDateTime(index);
    }

    @Override
    public OffsetDateTime getOffsetDateTime(String colName) {
        return reader.getOffsetDateTime(colName);
    }

    @Override
    public OffsetDateTime getOffsetDateTime(int index) {
        return reader.getOffsetDateTime(index);
    }

    @Override
    public Object getObject(String colName) {
        return reader.readValue(colName);
    }

    @Override
    public Object getObject(int index) {
        return reader.readValue(index);
    }

    @Override
    public ClickHouseBitmap getClickHouseBitmap(String colName) {
        return reader.readValue(colName);
    }

    @Override
    public ClickHouseBitmap getClickHouseBitmap(int index) {
        return reader.readValue(index);
    }

    @Override
    public TableSchema getSchema() {
        return reader.getSchema();
    }

    @Override
    public Map<String, Object> getValues() {
        return this.getSchema().getColumns().stream().collect(Collectors.toMap(
                ClickHouseColumn::getColumnName,
                column -> this.getObject(column.getColumnName())));
    }
}
