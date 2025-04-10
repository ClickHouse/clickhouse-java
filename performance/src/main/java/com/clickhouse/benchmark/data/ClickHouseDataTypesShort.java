package com.clickhouse.benchmark.data;

public enum ClickHouseDataTypesShort {

    Bool(false,1),
    Date(false,2),
    Date32(false,4),
    DateTime(true,0),
    DateTime32(true,4),
    DateTime64(true,8),
    Enum8(true,1),
    Enum16(true,2),
    FixedString(true,0),
    Int8(false,1),
    UInt8(false,1),
    Int16(false,2),
    UInt16(false,2),
    Int32(false,4),
    UInt32(false,4),
    Int64(false,8),
    IntervalYear(false,8),
    IntervalQuarter(false,8),
    IntervalMonth(false,8),
    IntervalWeek(false,8),
    IntervalDay(false,8),
    IntervalHour(false,8),
    IntervalMinute(false,8),
    IntervalSecond(false,8),
    IntervalMicrosecond(false,8),
    IntervalMillisecond(false,8),
    IntervalNanosecond(false,8),
    UInt64(false,8),
    Int128(false,16),
    UInt128(false,16),
    Int256(false,32),
    UInt256(false,32),
    Decimal(true,0),
    Decimal32(true,4),
    Decimal64(true,8),
    Decimal128(true,16),
    Decimal256(true,32),
    Float32(false,4),
    Float64(false,8),
    IPv4(false,4),
    IPv6(false,16),
    UUID(false,16),
    Point(false,33),
    Polygon(false,0),
    MultiPolygon(false,0),
    Ring(false,0),
    JSON(false,0),
    Object(true,0),
    String(false,0),
    Array(true,0),
    Map(true,0),
    Nested(true,0),
    Tuple(true,0),
    Nothing(false,0),
    SimpleAggregateFunction(true,0),
    AggregateFunction(true,0),
    Variant(true,0),
    Dynamic(true,0);

    private final boolean hasParameters;
    private final int fixedLength;
    private ClickHouseDataTypesShort(boolean hasParameters, int fixedLength) {
        this.hasParameters = hasParameters;
        this.fixedLength = fixedLength;
    }

    public boolean hasParameters() {
        return hasParameters;
    }

    public int getFixedLength() {
        return fixedLength;
    }

}
