package com.clickhouse.client.api.internal;

import com.clickhouse.data.ClickHouseDataType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class SchemaUtils {
    private static final ClickHouseDataType DEFAULT_DATA_TYPE = ClickHouseDataType.String;

    private static final List<ClickHouseDataType> DATA_TYPE_PRIORITY = ImmutableList.of(
            ClickHouseDataType.Bool,
            ClickHouseDataType.Int8,
            ClickHouseDataType.Int16,
            ClickHouseDataType.Int32,
            ClickHouseDataType.Int64,
            ClickHouseDataType.Int256,
            ClickHouseDataType.Float32,
            ClickHouseDataType.Float64,
            ClickHouseDataType.Decimal,
            ClickHouseDataType.String,
            ClickHouseDataType.UUID,
            ClickHouseDataType.IPv4,
            ClickHouseDataType.IPv6,
            ClickHouseDataType.DateTime64,
            ClickHouseDataType.Date,
            ClickHouseDataType.IntervalNanosecond,
            ClickHouseDataType.IntervalDay,
            ClickHouseDataType.Time64,
            ClickHouseDataType.Point,
            ClickHouseDataType.Ring,
            ClickHouseDataType.Polygon,
            ClickHouseDataType.MultiPolygon,
            ClickHouseDataType.Tuple,
            ClickHouseDataType.Geometry
    );

    private static final Map<Class<?>, ClickHouseDataType> CLASS_TO_DATA_TYPE = buildClassToDataTypeMap();

    private SchemaUtils() {
    }

    public static ClickHouseDataType inferDataType(Object value) {
        if (value == null) {
            return DEFAULT_DATA_TYPE;
        }

        // JSONEachRow has no type metadata, so structural values only infer the
        // top-level family; nested key/value/element types remain best-effort.
        if (value instanceof Map<?, ?>) {
            return ClickHouseDataType.Map;
        }

        Class<?> valueClass = ClickHouseDataType.toObjectType(value.getClass());
        if (value instanceof List<?> || valueClass.isArray()) {
            return ClickHouseDataType.Array;
        }

        ClickHouseDataType dataType = CLASS_TO_DATA_TYPE.get(valueClass);
        return dataType == null ? DEFAULT_DATA_TYPE : dataType;
    }

    private static Map<Class<?>, ClickHouseDataType> buildClassToDataTypeMap() {
        Map<Class<?>, ClickHouseDataType> map = new LinkedHashMap<>();

        for (ClickHouseDataType dataType : DATA_TYPE_PRIORITY) {
            addTypeMappings(map, dataType);
        }

        return ImmutableMap.copyOf(map);
    }

    private static void addTypeMappings(Map<Class<?>, ClickHouseDataType> map, ClickHouseDataType dataType) {
        Set<Class<?>> javaClasses = ClickHouseDataType.DATA_TYPE_TO_CLASS.get(dataType);
        if (javaClasses == null) {
            return;
        }

        for (Class<?> javaClass : javaClasses) {
            map.putIfAbsent(ClickHouseDataType.toObjectType(javaClass), dataType);
        }
    }
}
