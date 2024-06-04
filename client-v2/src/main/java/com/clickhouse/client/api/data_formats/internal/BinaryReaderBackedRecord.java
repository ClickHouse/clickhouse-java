package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.GenericRecord;
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
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

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
        return 0;
    }

    @Override
    public short getShort(String colName) {
        return 0;
    }

    @Override
    public int getInteger(String colName) {
        return 0;
    }

    @Override
    public long getLong(String colName) {
        return 0;
    }

    @Override
    public float getFloat(String colName) {
        return 0;
    }

    @Override
    public double getDouble(String colName) {
        return 0;
    }

    @Override
    public boolean getBoolean(String colName) {
        return false;
    }

    @Override
    public BigInteger getBigInteger(String colName) {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String colName) {
        return null;
    }

    @Override
    public Instant getInstant(String colName) {
        return null;
    }

    @Override
    public ZonedDateTime getZonedDateTime(String colName) {
        return null;
    }

    @Override
    public Duration getDuration(String colName) {
        return null;
    }

    @Override
    public Inet4Address getInet4Address(String colName) {
        return null;
    }

    @Override
    public Inet6Address getInet6Address(String colName) {
        return null;
    }

    @Override
    public UUID getUUID(String colName) {
        return null;
    }

    @Override
    public ClickHouseGeoPointValue getGeoPoint(String colName) {
        return null;
    }

    @Override
    public ClickHouseGeoRingValue getGeoRing(String colName) {
        return null;
    }

    @Override
    public ClickHouseGeoPolygonValue asGeoPolygon(String colName) {
        return null;
    }

    @Override
    public ClickHouseGeoMultiPolygonValue asGeoMultiPolygon(String colName) {
        return null;
    }

    @Override
    public <T> List<T> getList(String colName) {
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

    @Override
    public byte[] getByteArray(String colName) {
        return new byte[0];
    }

    @Override
    public int[] getIntArray(String colName) {
        return new int[0];
    }

    @Override
    public long[] getLongArray(String colName) {
        return new long[0];
    }

    @Override
    public float[] getFloatArray(String colName) {
        return new float[0];
    }

    @Override
    public double[] getDoubleArray(String colName) {
        return new double[0];
    }

    @Override
    public String getString(int index) {
        return null;
    }

    @Override
    public byte getByte(int index) {
        return 0;
    }

    @Override
    public short getShort(int index) {
        return 0;
    }

    @Override
    public int getInteger(int index) {
        return 0;
    }

    @Override
    public long getLong(int index) {
        return 0;
    }

    @Override
    public float getFloat(int index) {
        return 0;
    }

    @Override
    public double getDouble(int index) {
        return 0;
    }

    @Override
    public boolean getBoolean(int index) {
        return false;
    }

    @Override
    public BigInteger getBigInteger(int index) {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        return null;
    }

    @Override
    public Instant getInstant(int index) {
        return null;
    }

    @Override
    public ZonedDateTime getZonedDateTime(int index) {
        return null;
    }

    @Override
    public Duration getDuration(int index) {
        return null;
    }

    @Override
    public Inet4Address getInet4Address(int index) {
        return null;
    }

    @Override
    public Inet6Address getInet6Address(int index) {
        return null;
    }

    @Override
    public UUID getUUID(int index) {
        return null;
    }

    @Override
    public ClickHouseGeoPointValue getGeoPoint(int index) {
        return null;
    }

    @Override
    public ClickHouseGeoRingValue getGeoRing(int index) {
        return null;
    }

    @Override
    public ClickHouseGeoPolygonValue asGeoPolygon(int index) {
        return null;
    }

    @Override
    public ClickHouseGeoMultiPolygonValue asGeoMultiPolygon(int index) {
        return null;
    }

    @Override
    public <T> List<T> getList(int index) {
        return null;
    }

    @Override
    public byte[] getByteArray(int index) {
        return new byte[0];
    }

    @Override
    public int[] getIntArray(int index) {
        return new int[0];
    }

    @Override
    public long[] getLongArray(int index) {
        return new long[0];
    }

    @Override
    public float[] getFloatArray(int index) {
        return new float[0];
    }

    @Override
    public double[] getDoubleArray(int index) {
        return new double[0];
    }

    @Override
    public Object[] getTuple(int index) {
        return new Object[0];
    }

    @Override
    public Object[] getTuple(String colName) {
        return new Object[0];
    }

    @Override
    public byte getEnum8(String colName) {
        return 0;
    }

    @Override
    public byte getEnum8(int index) {
        return 0;
    }

    @Override
    public short getEnum16(String colName) {
        return 0;
    }

    @Override
    public short getEnum16(int index) {
        return 0;
    }

    @Override
    public LocalDate getLocalDate(String colName) {
        return null;
    }

    @Override
    public LocalDate getLocalDate(int index) {
        return null;
    }

    @Override
    public LocalDateTime getLocalDateTime(String colName) {
        return null;
    }

    @Override
    public LocalDateTime getLocalDateTime(int index) {
        return null;
    }
}
