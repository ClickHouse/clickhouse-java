package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.data.ClickHouseArraySequence;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseDeserializer;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.data.value.ClickHouseArrayValue;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

public class BinaryStreamReader {

    private final ClickHouseInputStream chInputStream;

    private final Logger log;

    BinaryStreamReader(ClickHouseInputStream chInputStream, Logger log) {
        this.chInputStream = chInputStream;
        this.log = log == null ? NOPLogger.NOP_LOGGER : log;
    }

    public <T> T readValue(ClickHouseColumn column) throws IOException {
        return readValueImpl(column);
    }

    private <T> T readValueImpl(ClickHouseColumn column) throws IOException {
        if (column.isNullable()) {
            if (BinaryStreamUtils.readNull(chInputStream)) {
                return (T) null;
            }
        }

        try {
            switch (column.getDataType()) {
                // Primitives
                case FixedString: {

                    return (T) BinaryStreamUtils.readFixedString(chInputStream, column.getEstimatedLength(), StandardCharsets.UTF_8).trim();
                }
                case String: {
                    // TODO: BinaryStreamUtils.readString() - requires reader that may be causing EOF exception
                    int len = chInputStream.readVarInt();
                    return (T) chInputStream.readUnicodeString(len);
                }
                case Int8:
                    return (T) Byte.valueOf(BinaryStreamUtils.readInt8(chInputStream));
                case UInt8:
                    return (T) Short.valueOf(BinaryStreamUtils.readUnsignedInt8(chInputStream));
                case Int16:
                    return (T) Short.valueOf(BinaryStreamUtils.readInt16(chInputStream));
                case UInt16:
                    return (T) Integer.valueOf(BinaryStreamUtils.readUnsignedInt16(chInputStream));
                case Int32:
                    return (T) Integer.valueOf(BinaryStreamUtils.readInt32(chInputStream));
                case UInt32:
                    return (T) Long.valueOf(BinaryStreamUtils.readUnsignedInt32(chInputStream));
                case Int64:
                    return (T) Long.valueOf(BinaryStreamUtils.readInt64(chInputStream));
                case UInt64:
                    return (T) BinaryStreamUtils.readUnsignedInt64(chInputStream);
                case Int128:
                    return (T) BinaryStreamUtils.readInt128(chInputStream);
                case UInt128:
                    return (T) BinaryStreamUtils.readUnsignedInt128(chInputStream);
                case Int256:
                    return (T) BinaryStreamUtils.readInt256(chInputStream);
                case UInt256:
                    return (T) BinaryStreamUtils.readUnsignedInt256(chInputStream);
                case Decimal:
                    return (T) BinaryStreamUtils.readDecimal(chInputStream, column.getPrecision(), column.getScale());
                case Decimal32:
                    return (T) BinaryStreamUtils.readDecimal32(chInputStream, column.getScale());
                case Decimal64:
                    return (T) BinaryStreamUtils.readDecimal64(chInputStream, column.getScale());
                case Decimal128:
                    return (T) BinaryStreamUtils.readDecimal128(chInputStream, column.getScale());
                case Decimal256:
                    return (T) BinaryStreamUtils.readDecimal256(chInputStream, column.getScale());
                case Float32:
                    return (T) Float.valueOf(BinaryStreamUtils.readFloat32(chInputStream));
                case Float64:
                    return (T) Double.valueOf(BinaryStreamUtils.readFloat64(chInputStream));

                case Bool:
                    return (T) Boolean.valueOf(BinaryStreamUtils.readBoolean(chInputStream));
                case Enum8:
                    return (T) Byte.valueOf(BinaryStreamUtils.readEnum8(chInputStream));
                case Enum16:
                    return (T) Short.valueOf(BinaryStreamUtils.readEnum16(chInputStream));

                case Date:
                    return (T) BinaryStreamUtils.readDate(chInputStream, column.getTimeZone());
                case Date32:
                    return (T) BinaryStreamUtils.readDate32(chInputStream, column.getTimeZone());
                case DateTime:
                    return (T) BinaryStreamUtils.readDateTime(chInputStream, column.getTimeZone());
                case DateTime32:
                    return (T) BinaryStreamUtils.readDateTime32(chInputStream, column.getTimeZone());
                case DateTime64:
                    return (T) BinaryStreamUtils.readDateTime64(chInputStream, column.getTimeZone());

                case IntervalYear:
                case IntervalQuarter:
                case IntervalMonth:
                case IntervalWeek:
                case IntervalDay:
                case IntervalHour:
                case IntervalMinute:
                case IntervalSecond:
                case IntervalMicrosecond:
                case IntervalMillisecond:
                case IntervalNanosecond:
                    return (T) BinaryStreamUtils.readUnsignedInt64(chInputStream);

                case IPv4:
                    return (T) BinaryStreamUtils.readInet4Address(chInputStream);
                case IPv6:
                    return (T) BinaryStreamUtils.readInet6Address(chInputStream);
                case UUID:
                    return (T) BinaryStreamUtils.readUuid(chInputStream);
                case Point:
                    return (T) BinaryStreamUtils.readGeoPoint(chInputStream);
                case Polygon:
                    return (T) BinaryStreamUtils.readGeoPolygon(chInputStream);
                case MultiPolygon:
                    return (T) BinaryStreamUtils.readGeoMultiPolygon(chInputStream);
                case Ring:
                    return (T) BinaryStreamUtils.readGeoRing(chInputStream);

//                case JSON: // obsolete https://clickhouse.com/docs/en/sql-reference/data-types/json#displaying-json-column
//                case Object:
                case Array:
                    return (T) readArray(column);
                case Map:
                    return (T) readMap(column);
//                case Nested:
                case Tuple:
                    return (T) readTuple(column);
                case Nothing:
                    return null;
//                case SimpleAggregateFunction:
//                case AggregateFunction:
                default:
                    throw new IllegalArgumentException("Unsupported data type: " + column.getDataType());
            }
        } catch (EOFException e) {
            throw e;
        } catch (IOException e) {
            // TODO: handle parse exception when stream is readable but data is not valid for the type
            log.error("Failed to read value of type: {}", column.getDataType(), e);
            throw new RuntimeException(e);
        }
    }

    private ArrayValue readArray(ClickHouseColumn column) throws IOException {
        Class<?> itemType = column.getArrayBaseColumn().getDataType().getWiderPrimitiveClass();
        int len = chInputStream.readVarInt();
        ArrayValue array = new ArrayValue(column.getArrayNestedLevel() > 1 ? ArrayValue.class : itemType, len);

        if (len == 0) {
            return array;
        }

        for (int i = 0; i < len; i++) {
            array.set(i, readValueImpl(column.getNestedColumns().get(0)));
        }

        return array;
    }

    public static class ArrayValue {

        final int length;

        final Class<?> itemType;

        final Object array;

        ArrayValue(Class<?> itemType, int length) {
            this.itemType = itemType;
            this.length = length;

            try {
                if (itemType.isArray()) {
                    array = Array.newInstance(ArrayValue.class, length);
                } else {
                    array = Array.newInstance(itemType, length);
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to create array of type: " + itemType, e);
            }
        }

        public int length() {
            return length;
        }

        public Object get(int index) {
            return Array.get(array, index);
        }

        public void set(int index, Object value) {
            try {
                Array.set(array, index, value);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Failed to set value at index: " + index +
                        " value " + value + " of class " + value.getClass().getName(), e);
            }
        }
    }

    private Map<?,?> readMap(ClickHouseColumn column) throws IOException {
        int len = chInputStream.readVarInt();

        if (len == 0) {
            return Collections.emptyMap();
        }

        ClickHouseColumn keyType = column.getKeyInfo();
        ClickHouseColumn valueType = column.getValueInfo();
        LinkedHashMap<Object, Object> map = new LinkedHashMap<>(len);
        for (int i = 0; i < len; i++) {
            Object key = readValueImpl(keyType);
            Object value = readValueImpl(valueType);
            map.put(key, value);
        }

        return map;
    }

    private Object[] readTuple(ClickHouseColumn column) throws IOException {
        int len = column.getNestedColumns().size();
        Object[] tuple = new Object[len];
        for (int i = 0; i < len; i++) {
            tuple[i] = readValueImpl(column.getNestedColumns().get(i));
        }

        return tuple;
    }
}
