package com.clickhouse.client.datatypes.readers;

import com.clickhouse.client.api.data_formats.ClickHouseFormatReader;

import java.util.function.BiFunction;

/**
 * Typed accessors of {@link ClickHouseFormatReader}. Each accessor is called both by column index and by name.
 */
enum ReaderAccessor {
    STRING("getString", false, ClickHouseFormatReader::getString, ClickHouseFormatReader::getString),
    BYTE("getByte", true, ClickHouseFormatReader::getByte, ClickHouseFormatReader::getByte),
    SHORT("getShort", true, ClickHouseFormatReader::getShort, ClickHouseFormatReader::getShort),
    INTEGER("getInteger", true, ClickHouseFormatReader::getInteger, ClickHouseFormatReader::getInteger),
    LONG("getLong", true, ClickHouseFormatReader::getLong, ClickHouseFormatReader::getLong),
    FLOAT("getFloat", true, ClickHouseFormatReader::getFloat, ClickHouseFormatReader::getFloat),
    DOUBLE("getDouble", true, ClickHouseFormatReader::getDouble, ClickHouseFormatReader::getDouble),
    BOOLEAN("getBoolean", true, ClickHouseFormatReader::getBoolean, ClickHouseFormatReader::getBoolean),
    BIG_INTEGER("getBigInteger", false, ClickHouseFormatReader::getBigInteger, ClickHouseFormatReader::getBigInteger),
    BIG_DECIMAL("getBigDecimal", false, ClickHouseFormatReader::getBigDecimal, ClickHouseFormatReader::getBigDecimal),
    INSTANT("getInstant", false, ClickHouseFormatReader::getInstant, ClickHouseFormatReader::getInstant),
    ZONED_DATE_TIME("getZonedDateTime", false, ClickHouseFormatReader::getZonedDateTime,
            ClickHouseFormatReader::getZonedDateTime),
    OFFSET_DATE_TIME("getOffsetDateTime", false, ClickHouseFormatReader::getOffsetDateTime,
            ClickHouseFormatReader::getOffsetDateTime),
    LOCAL_DATE("getLocalDate", false, ClickHouseFormatReader::getLocalDate, ClickHouseFormatReader::getLocalDate),
    LOCAL_TIME("getLocalTime", false, ClickHouseFormatReader::getLocalTime, ClickHouseFormatReader::getLocalTime),
    LOCAL_DATE_TIME("getLocalDateTime", false, ClickHouseFormatReader::getLocalDateTime,
            ClickHouseFormatReader::getLocalDateTime),
    DURATION("getDuration", false, ClickHouseFormatReader::getDuration, ClickHouseFormatReader::getDuration),
    TEMPORAL_AMOUNT("getTemporalAmount", false, ClickHouseFormatReader::getTemporalAmount,
            ClickHouseFormatReader::getTemporalAmount),
    INET4_ADDRESS("getInet4Address", false, ClickHouseFormatReader::getInet4Address,
            ClickHouseFormatReader::getInet4Address),
    INET6_ADDRESS("getInet6Address", false, ClickHouseFormatReader::getInet6Address,
            ClickHouseFormatReader::getInet6Address),
    UUID("getUUID", false, ClickHouseFormatReader::getUUID, ClickHouseFormatReader::getUUID),
    GEO_POINT("getGeoPoint", false, ClickHouseFormatReader::getGeoPoint, ClickHouseFormatReader::getGeoPoint),
    GEO_RING("getGeoRing", false, ClickHouseFormatReader::getGeoRing, ClickHouseFormatReader::getGeoRing),
    GEO_POLYGON("getGeoPolygon", false, ClickHouseFormatReader::getGeoPolygon, ClickHouseFormatReader::getGeoPolygon),
    GEO_MULTI_POLYGON("getGeoMultiPolygon", false, ClickHouseFormatReader::getGeoMultiPolygon,
            ClickHouseFormatReader::getGeoMultiPolygon),
    LIST("getList", false, ClickHouseFormatReader::getList, ClickHouseFormatReader::getList),
    BYTE_ARRAY("getByteArray", false, ClickHouseFormatReader::getByteArray, ClickHouseFormatReader::getByteArray),
    SHORT_ARRAY("getShortArray", false, ClickHouseFormatReader::getShortArray, ClickHouseFormatReader::getShortArray),
    INT_ARRAY("getIntArray", false, ClickHouseFormatReader::getIntArray, ClickHouseFormatReader::getIntArray),
    LONG_ARRAY("getLongArray", false, ClickHouseFormatReader::getLongArray, ClickHouseFormatReader::getLongArray),
    FLOAT_ARRAY("getFloatArray", false, ClickHouseFormatReader::getFloatArray, ClickHouseFormatReader::getFloatArray),
    DOUBLE_ARRAY("getDoubleArray", false, ClickHouseFormatReader::getDoubleArray,
            ClickHouseFormatReader::getDoubleArray),
    BOOLEAN_ARRAY("getBooleanArray", false, ClickHouseFormatReader::getBooleanArray,
            ClickHouseFormatReader::getBooleanArray),
    STRING_ARRAY("getStringArray", false, ClickHouseFormatReader::getStringArray,
            ClickHouseFormatReader::getStringArray),
    OBJECT_ARRAY("getObjectArray", false, ClickHouseFormatReader::getObjectArray,
            ClickHouseFormatReader::getObjectArray),
    TUPLE("getTuple", false, ClickHouseFormatReader::getTuple, ClickHouseFormatReader::getTuple),
    ENUM8("getEnum8", true, ClickHouseFormatReader::getEnum8, ClickHouseFormatReader::getEnum8),
    ENUM16("getEnum16", true, ClickHouseFormatReader::getEnum16, ClickHouseFormatReader::getEnum16),
    CLICKHOUSE_BITMAP("getClickHouseBitmap", false, ClickHouseFormatReader::getClickHouseBitmap,
            ClickHouseFormatReader::getClickHouseBitmap);

    private final String methodName;
    private final boolean primitive;
    private final BiFunction<ClickHouseFormatReader, Integer, Object> byIndex;
    private final BiFunction<ClickHouseFormatReader, String, Object> byName;

    ReaderAccessor(String methodName, boolean primitive, BiFunction<ClickHouseFormatReader, Integer, Object> byIndex,
            BiFunction<ClickHouseFormatReader, String, Object> byName) {
        this.methodName = methodName;
        this.primitive = primitive;
        this.byIndex = byIndex;
        this.byName = byName;
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

    @Override
    public String toString() {
        return methodName;
    }
}
