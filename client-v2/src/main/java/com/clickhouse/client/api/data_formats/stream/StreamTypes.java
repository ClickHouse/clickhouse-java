package com.clickhouse.client.api.data_formats.stream;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Shared column shapes for the streaming reader and writer.
 */
final class StreamTypes {

    static final List<ClickHouseColumn> POINT_FIELDS = Collections.unmodifiableList(Arrays.asList(
            ClickHouseColumn.of("x", "Float64"),
            ClickHouseColumn.of("y", "Float64")));

    static final ClickHouseColumn POINT = ClickHouseColumn.of("p", "Point");

    static final ClickHouseColumn RING = ClickHouseColumn.of("r", "Ring");

    static final ClickHouseColumn POLYGON = ClickHouseColumn.of("g", "Polygon");

    static final ClickHouseColumn NOTHING = ClickHouseColumn.of("v", "Nothing");

    private StreamTypes() {
    }

    static boolean omitted(ClickHouseColumn column) {
        return column.hasDefault() && column.getDefaultValue() != ClickHouseColumn.DefaultValue.DEFAULT;
    }

    static ClickHouseColumn unwrap(ClickHouseColumn column) {
        if (column.getDataType() == ClickHouseDataType.SimpleAggregateFunction) {
            return column.getNestedColumns().get(0);
        }
        return column;
    }

    static boolean isOneOf(ClickHouseDataType actual, ClickHouseDataType... allowed) {
        for (int i = 0; i < allowed.length; i++) {
            if (actual == allowed[i]) {
                return true;
            }
        }
        return false;
    }

    static String describe(ClickHouseColumn column) {
        String type = column.getOriginalTypeName();
        if (type == null || type.isEmpty()) {
            type = column.getDataType().name();
        }
        String name = column.getColumnName();
        if (name == null || name.isEmpty()) {
            return type;
        }
        return name + " " + type;
    }

    /**
     * Element column of a length-prefixed array-like value, or null when {@code column} is not one.
     */
    static ClickHouseColumn arrayElement(ClickHouseColumn column) {
        ClickHouseDataType type = column.getDataType();
        if (type == ClickHouseDataType.Array || type == ClickHouseDataType.QBit) {
            return column.getNestedColumns().get(0);
        }
        if (type == ClickHouseDataType.Ring || type == ClickHouseDataType.LineString
                || type == ClickHouseDataType.MultiPoint) {
            return POINT;
        }
        if (type == ClickHouseDataType.Polygon || type == ClickHouseDataType.MultiLineString) {
            return RING;
        }
        if (type == ClickHouseDataType.MultiPolygon) {
            return POLYGON;
        }
        return null;
    }

    /**
     * Tuple fields, or the two coordinates of a {@code Point}. Null when {@code column} is neither.
     */
    static List<ClickHouseColumn> tupleFields(ClickHouseColumn column) {
        if (column.getDataType() == ClickHouseDataType.Tuple) {
            return column.getNestedColumns();
        }
        if (column.getDataType() == ClickHouseDataType.Point) {
            return POINT_FIELDS;
        }
        return null;
    }

    static int decimalBytes(int precision) {
        if (precision <= ClickHouseDataType.Decimal32.getMaxScale()) {
            return 4;
        }
        if (precision <= ClickHouseDataType.Decimal64.getMaxScale()) {
            return 8;
        }
        if (precision <= ClickHouseDataType.Decimal128.getMaxScale()) {
            return 16;
        }
        return 32;
    }

    /**
     * Rejects a runtime type whose dynamic tag encoding is incomplete and would desynchronize the stream.
     */
    static void rejectDynamicPayload(ClickHouseColumn column) {
        ArrayDeque<ClickHouseColumn> pending = new ArrayDeque<ClickHouseColumn>();
        pending.add(column);
        while (!pending.isEmpty()) {
            ClickHouseColumn current = pending.removeFirst();
            switch (current.getDataType()) {
                case Tuple:
                case Variant:
                case Dynamic:
                case JSON:
                case Nested:
                case QBit:
                case AggregateFunction:
                case SimpleAggregateFunction:
                case Geometry:
                    throw new ClientException("Serializing a " + current.getDataType().name()
                            + " value inside a Dynamic column is not supported");
                default:
                    break;
            }
            pending.addAll(current.getNestedColumns());
        }
    }
}
