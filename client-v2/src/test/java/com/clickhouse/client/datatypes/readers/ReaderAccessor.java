package com.clickhouse.client.datatypes.readers;

import com.clickhouse.client.api.data_formats.ClickHouseFormatReader;
import com.clickhouse.client.api.query.GenericRecord;

import java.util.function.BiFunction;

/**
 * Typed accessors of {@link ClickHouseFormatReader} and {@link GenericRecord}. Each accessor is called both by
 * column index and by name.
 */
enum ReaderAccessor {
    STRING("getString", false, ClickHouseFormatReader::getString, ClickHouseFormatReader::getString,
            GenericRecord::getString, GenericRecord::getString),
    BYTE("getByte", true, ClickHouseFormatReader::getByte, ClickHouseFormatReader::getByte,
            GenericRecord::getByte, GenericRecord::getByte),
    SHORT("getShort", true, ClickHouseFormatReader::getShort, ClickHouseFormatReader::getShort,
            GenericRecord::getShort, GenericRecord::getShort),
    INTEGER("getInteger", true, ClickHouseFormatReader::getInteger, ClickHouseFormatReader::getInteger,
            GenericRecord::getInteger, GenericRecord::getInteger),
    LONG("getLong", true, ClickHouseFormatReader::getLong, ClickHouseFormatReader::getLong,
            GenericRecord::getLong, GenericRecord::getLong),
    FLOAT("getFloat", true, ClickHouseFormatReader::getFloat, ClickHouseFormatReader::getFloat,
            GenericRecord::getFloat, GenericRecord::getFloat),
    DOUBLE("getDouble", true, ClickHouseFormatReader::getDouble, ClickHouseFormatReader::getDouble,
            GenericRecord::getDouble, GenericRecord::getDouble),
    BOOLEAN("getBoolean", true, ClickHouseFormatReader::getBoolean, ClickHouseFormatReader::getBoolean,
            GenericRecord::getBoolean, GenericRecord::getBoolean),
    BIG_INTEGER("getBigInteger", false, ClickHouseFormatReader::getBigInteger, ClickHouseFormatReader::getBigInteger,
            GenericRecord::getBigInteger, GenericRecord::getBigInteger),
    BIG_DECIMAL("getBigDecimal", false, ClickHouseFormatReader::getBigDecimal, ClickHouseFormatReader::getBigDecimal,
            GenericRecord::getBigDecimal, GenericRecord::getBigDecimal),
    INSTANT("getInstant", false, ClickHouseFormatReader::getInstant, ClickHouseFormatReader::getInstant,
            GenericRecord::getInstant, GenericRecord::getInstant),
    ZONED_DATE_TIME("getZonedDateTime", false, ClickHouseFormatReader::getZonedDateTime,
            ClickHouseFormatReader::getZonedDateTime,
            GenericRecord::getZonedDateTime, GenericRecord::getZonedDateTime),
    OFFSET_DATE_TIME("getOffsetDateTime", false, ClickHouseFormatReader::getOffsetDateTime,
            ClickHouseFormatReader::getOffsetDateTime,
            GenericRecord::getOffsetDateTime, GenericRecord::getOffsetDateTime),
    LOCAL_DATE("getLocalDate", false, ClickHouseFormatReader::getLocalDate, ClickHouseFormatReader::getLocalDate,
            GenericRecord::getLocalDate, GenericRecord::getLocalDate),
    LOCAL_TIME("getLocalTime", false, ClickHouseFormatReader::getLocalTime, ClickHouseFormatReader::getLocalTime,
            GenericRecord::getLocalTime, GenericRecord::getLocalTime),
    LOCAL_DATE_TIME("getLocalDateTime", false, ClickHouseFormatReader::getLocalDateTime,
            ClickHouseFormatReader::getLocalDateTime,
            GenericRecord::getLocalDateTime, GenericRecord::getLocalDateTime),
    DURATION("getDuration", false, ClickHouseFormatReader::getDuration, ClickHouseFormatReader::getDuration,
            GenericRecord::getDuration, GenericRecord::getDuration),
    TEMPORAL_AMOUNT("getTemporalAmount", false, ClickHouseFormatReader::getTemporalAmount,
            ClickHouseFormatReader::getTemporalAmount,
            GenericRecord::getTemporalAmount, GenericRecord::getTemporalAmount),
    INET4_ADDRESS("getInet4Address", false, ClickHouseFormatReader::getInet4Address,
            ClickHouseFormatReader::getInet4Address,
            GenericRecord::getInet4Address, GenericRecord::getInet4Address),
    INET6_ADDRESS("getInet6Address", false, ClickHouseFormatReader::getInet6Address,
            ClickHouseFormatReader::getInet6Address,
            GenericRecord::getInet6Address, GenericRecord::getInet6Address),
    UUID("getUUID", false, ClickHouseFormatReader::getUUID, ClickHouseFormatReader::getUUID,
            GenericRecord::getUUID, GenericRecord::getUUID),
    GEO_POINT("getGeoPoint", false, ClickHouseFormatReader::getGeoPoint, ClickHouseFormatReader::getGeoPoint,
            GenericRecord::getGeoPoint, GenericRecord::getGeoPoint),
    GEO_RING("getGeoRing", false, ClickHouseFormatReader::getGeoRing, ClickHouseFormatReader::getGeoRing,
            GenericRecord::getGeoRing, GenericRecord::getGeoRing),
    GEO_POLYGON("getGeoPolygon", false, ClickHouseFormatReader::getGeoPolygon, ClickHouseFormatReader::getGeoPolygon,
            GenericRecord::getGeoPolygon, GenericRecord::getGeoPolygon),
    GEO_MULTI_POLYGON("getGeoMultiPolygon", false, ClickHouseFormatReader::getGeoMultiPolygon,
            ClickHouseFormatReader::getGeoMultiPolygon,
            GenericRecord::getGeoMultiPolygon, GenericRecord::getGeoMultiPolygon),
    LIST("getList", false, ClickHouseFormatReader::getList, ClickHouseFormatReader::getList,
            GenericRecord::getList, GenericRecord::getList),
    BYTE_ARRAY("getByteArray", false, ClickHouseFormatReader::getByteArray, ClickHouseFormatReader::getByteArray,
            GenericRecord::getByteArray, GenericRecord::getByteArray),
    SHORT_ARRAY("getShortArray", false, ClickHouseFormatReader::getShortArray, ClickHouseFormatReader::getShortArray,
            GenericRecord::getShortArray, GenericRecord::getShortArray),
    INT_ARRAY("getIntArray", false, ClickHouseFormatReader::getIntArray, ClickHouseFormatReader::getIntArray,
            GenericRecord::getIntArray, GenericRecord::getIntArray),
    LONG_ARRAY("getLongArray", false, ClickHouseFormatReader::getLongArray, ClickHouseFormatReader::getLongArray,
            GenericRecord::getLongArray, GenericRecord::getLongArray),
    FLOAT_ARRAY("getFloatArray", false, ClickHouseFormatReader::getFloatArray, ClickHouseFormatReader::getFloatArray,
            GenericRecord::getFloatArray, GenericRecord::getFloatArray),
    DOUBLE_ARRAY("getDoubleArray", false, ClickHouseFormatReader::getDoubleArray,
            ClickHouseFormatReader::getDoubleArray,
            GenericRecord::getDoubleArray, GenericRecord::getDoubleArray),
    BOOLEAN_ARRAY("getBooleanArray", false, ClickHouseFormatReader::getBooleanArray,
            ClickHouseFormatReader::getBooleanArray,
            GenericRecord::getBooleanArray, GenericRecord::getBooleanArray),
    STRING_ARRAY("getStringArray", false, ClickHouseFormatReader::getStringArray,
            ClickHouseFormatReader::getStringArray,
            GenericRecord::getStringArray, GenericRecord::getStringArray),
    OBJECT_ARRAY("getObjectArray", false, ClickHouseFormatReader::getObjectArray,
            ClickHouseFormatReader::getObjectArray,
            GenericRecord::getObjectArray, GenericRecord::getObjectArray),
    TUPLE("getTuple", false, ClickHouseFormatReader::getTuple, ClickHouseFormatReader::getTuple,
            GenericRecord::getTuple, GenericRecord::getTuple),
    ENUM8("getEnum8", true, ClickHouseFormatReader::getEnum8, ClickHouseFormatReader::getEnum8,
            GenericRecord::getEnum8, GenericRecord::getEnum8),
    ENUM16("getEnum16", true, ClickHouseFormatReader::getEnum16, ClickHouseFormatReader::getEnum16,
            GenericRecord::getEnum16, GenericRecord::getEnum16),
    CLICKHOUSE_BITMAP("getClickHouseBitmap", false, ClickHouseFormatReader::getClickHouseBitmap,
            ClickHouseFormatReader::getClickHouseBitmap,
            GenericRecord::getClickHouseBitmap, GenericRecord::getClickHouseBitmap);

    private final String methodName;
    private final boolean primitive;
    private final BiFunction<ClickHouseFormatReader, Integer, Object> byIndex;
    private final BiFunction<ClickHouseFormatReader, String, Object> byName;
    private final BiFunction<GenericRecord, Integer, Object> byRecordIndex;
    private final BiFunction<GenericRecord, String, Object> byRecordName;

    ReaderAccessor(String methodName, boolean primitive, BiFunction<ClickHouseFormatReader, Integer, Object> byIndex,
            BiFunction<ClickHouseFormatReader, String, Object> byName,
            BiFunction<GenericRecord, Integer, Object> byRecordIndex,
            BiFunction<GenericRecord, String, Object> byRecordName) {
        this.methodName = methodName;
        this.primitive = primitive;
        this.byIndex = byIndex;
        this.byName = byName;
        this.byRecordIndex = byRecordIndex;
        this.byRecordName = byRecordName;
    }

    String getMethodName() {
        return methodName;
    }

    /**
     * @return {@code true} when the accessor returns a primitive, so it cannot return {@code null}
     */
    boolean isPrimitive() {
        return primitive;
    }

    Object get(ClickHouseFormatReader reader, int index) {
        return byIndex.apply(reader, index);
    }

    Object get(ClickHouseFormatReader reader, String name) {
        return byName.apply(reader, name);
    }

    Object get(GenericRecord record, int index) {
        return byRecordIndex.apply(record, index);
    }

    Object get(GenericRecord record, String name) {
        return byRecordName.apply(record, name);
    }

    @Override
    public String toString() {
        return methodName;
    }
}
