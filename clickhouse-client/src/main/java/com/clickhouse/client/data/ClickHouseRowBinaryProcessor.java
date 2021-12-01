package com.clickhouse.client.data;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;
import java.util.function.Supplier;

import com.clickhouse.client.ClickHouseAggregateFunction;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseDataProcessor;
import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.client.ClickHouseDeserializer;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseSerializer;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;

/**
 * Data processor for handling {@link ClickHouseFormat#RowBinary} and
 * {@link ClickHouseFormat#RowBinaryWithNamesAndTypes} two formats.
 */
public class ClickHouseRowBinaryProcessor extends ClickHouseDataProcessor {
    public static class MappedFunctions {
        private static final MappedFunctions instance = new MappedFunctions();

        private void writeArray(ClickHouseValue value, ClickHouseConfig config, ClickHouseColumn column,
                OutputStream output) throws IOException {
            ClickHouseColumn nestedColumn = column.getNestedColumns().get(0);
            ClickHouseColumn baseColumn = column.getArrayBaseColumn();
            int level = column.getArrayNestedLevel();
            Class<?> javaClass = baseColumn.getDataType().getPrimitiveClass();
            if (level > 1 || !javaClass.isPrimitive()) {
                Object[] array = value.asArray();
                ClickHouseValue v = ClickHouseValues.newValue(nestedColumn);
                int length = array.length;
                BinaryStreamUtils.writeVarInt(output, length);
                for (int i = 0; i < length; i++) {
                    serialize(v.update(array[i]), config, nestedColumn, output);
                }
            } else {
                ClickHouseValue v = ClickHouseValues.newValue(baseColumn);
                if (byte.class == javaClass) {
                    byte[] array = (byte[]) value.asObject();
                    int length = array.length;
                    BinaryStreamUtils.writeVarInt(output, length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else if (short.class == javaClass) {
                    short[] array = (short[]) value.asObject();
                    int length = array.length;
                    BinaryStreamUtils.writeVarInt(output, length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else if (int.class == javaClass) {
                    int[] array = (int[]) value.asObject();
                    int length = array.length;
                    BinaryStreamUtils.writeVarInt(output, length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else if (long.class == javaClass) {
                    long[] array = (long[]) value.asObject();
                    int length = array.length;
                    BinaryStreamUtils.writeVarInt(output, length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else if (float.class == javaClass) {
                    float[] array = (float[]) value.asObject();
                    int length = array.length;
                    BinaryStreamUtils.writeVarInt(output, length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else if (double.class == javaClass) {
                    double[] array = (double[]) value.asObject();
                    int length = array.length;
                    BinaryStreamUtils.writeVarInt(output, length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else {
                    throw new IllegalArgumentException("Unsupported primitive type: " + javaClass);
                }
            }
        }

        private ClickHouseValue readArray(ClickHouseValue ref, ClickHouseConfig config, ClickHouseColumn nestedColumn,
                ClickHouseColumn baseColumn, ClickHouseInputStream input, int length, int level) throws IOException {
            Class<?> javaClass = baseColumn.getDataType().getPrimitiveClass();
            if (level > 1 || !javaClass.isPrimitive()) {
                Object[] array = (Object[]) ClickHouseValues.createPrimitiveArray(javaClass, length, level);
                for (int i = 0; i < length; i++) {
                    array[i] = deserialize(null, config, nestedColumn, input).asObject();
                }
                ref.update(array);
            } else {
                if (byte.class == javaClass) {
                    byte[] array = new byte[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(null, config, baseColumn, input).asByte();
                    }
                    ref.update(array);
                } else if (short.class == javaClass) {
                    short[] array = new short[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(null, config, baseColumn, input).asShort();
                    }
                    ref.update(array);
                } else if (int.class == javaClass) {
                    int[] array = new int[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(null, config, baseColumn, input).asInteger();
                    }
                    ref.update(array);
                } else if (long.class == javaClass) {
                    long[] array = new long[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(null, config, baseColumn, input).asLong();
                    }
                    ref.update(array);
                } else if (float.class == javaClass) {
                    float[] array = new float[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(null, config, baseColumn, input).asFloat();
                    }
                    ref.update(array);
                } else if (double.class == javaClass) {
                    double[] array = new double[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(null, config, baseColumn, input).asDouble();
                    }
                    ref.update(array);
                } else {
                    throw new IllegalArgumentException("Unsupported primitive type: " + javaClass);
                }
            }

            return ref;
        }

        private final Map<ClickHouseDataType, ClickHouseDeserializer<? extends ClickHouseValue>> deserializers;
        private final Map<ClickHouseDataType, ClickHouseSerializer<? extends ClickHouseValue>> serializers;

        private MappedFunctions() {
            deserializers = new EnumMap<>(ClickHouseDataType.class);
            serializers = new EnumMap<>(ClickHouseDataType.class);

            // enum and numbers
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseByteValue.of(r, BinaryStreamUtils.readInt8(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt8(o, v.asByte()), ClickHouseDataType.Enum,
                    ClickHouseDataType.Enum8, ClickHouseDataType.Int8);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseShortValue.of(r, BinaryStreamUtils.readUnsignedInt8(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeUnsignedInt8(o, v.asInteger()), ClickHouseDataType.UInt8);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseShortValue.of(r, BinaryStreamUtils.readInt16(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt16(o, v.asShort()), ClickHouseDataType.Enum16,
                    ClickHouseDataType.Int16);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseIntegerValue.of(r, BinaryStreamUtils.readUnsignedInt16(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeUnsignedInt16(o, v.asInteger()), ClickHouseDataType.UInt16);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseIntegerValue.of(r, BinaryStreamUtils.readInt32(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt32(o, v.asInteger()), ClickHouseDataType.Int32);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseLongValue.of(r, false, BinaryStreamUtils.readUnsignedInt32(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeUnsignedInt32(o, v.asLong()), ClickHouseDataType.UInt32);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseLongValue.of(r, false, BinaryStreamUtils.readInt64(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt64(o, v.asLong()), ClickHouseDataType.IntervalYear,
                    ClickHouseDataType.IntervalQuarter, ClickHouseDataType.IntervalMonth,
                    ClickHouseDataType.IntervalWeek, ClickHouseDataType.IntervalDay, ClickHouseDataType.IntervalHour,
                    ClickHouseDataType.IntervalMinute, ClickHouseDataType.IntervalSecond, ClickHouseDataType.Int64);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseLongValue.of(r, true, BinaryStreamUtils.readInt64(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt64(o, v.asLong()), ClickHouseDataType.UInt64);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseBigIntegerValue.of(r, BinaryStreamUtils.readInt128(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt128(o, v.asBigInteger()), ClickHouseDataType.Int128);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseBigIntegerValue.of(r, BinaryStreamUtils.readUnsignedInt128(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeUnsignedInt128(o, v.asBigInteger()),
                    ClickHouseDataType.UInt128);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseBigIntegerValue.of(r, BinaryStreamUtils.readInt256(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt256(o, v.asBigInteger()), ClickHouseDataType.Int256);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseBigIntegerValue.of(r, BinaryStreamUtils.readUnsignedInt256(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeUnsignedInt256(o, v.asBigInteger()),
                    ClickHouseDataType.UInt256);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseFloatValue.of(r, BinaryStreamUtils.readFloat32(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeFloat32(o, v.asFloat()), ClickHouseDataType.Float32);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseDoubleValue.of(r, BinaryStreamUtils.readFloat64(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeFloat64(o, v.asDouble()), ClickHouseDataType.Float64);

            // decimals
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseBigDecimalValue.of(r,
                            BinaryStreamUtils.readDecimal(i, c.getPrecision(), c.getScale())),
                    (v, f, c, o) -> BinaryStreamUtils.writeDecimal(o, v.asBigDecimal(c.getScale()), c.getPrecision(),
                            c.getScale()),
                    ClickHouseDataType.Decimal);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseBigDecimalValue.of(r, BinaryStreamUtils.readDecimal32(i, c.getScale())),
                    (v, f, c, o) -> BinaryStreamUtils.writeDecimal32(o, v.asBigDecimal(c.getScale()), c.getScale()),
                    ClickHouseDataType.Decimal32);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseBigDecimalValue.of(r, BinaryStreamUtils.readDecimal64(i, c.getScale())),
                    (v, f, c, o) -> BinaryStreamUtils.writeDecimal64(o, v.asBigDecimal(c.getScale()), c.getScale()),
                    ClickHouseDataType.Decimal64);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseBigDecimalValue.of(r, BinaryStreamUtils.readDecimal128(i, c.getScale())),
                    (v, f, c, o) -> BinaryStreamUtils.writeDecimal128(o, v.asBigDecimal(c.getScale()), c.getScale()),
                    ClickHouseDataType.Decimal128);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseBigDecimalValue.of(r, BinaryStreamUtils.readDecimal256(i, c.getScale())),
                    (v, f, c, o) -> BinaryStreamUtils.writeDecimal256(o, v.asBigDecimal(c.getScale()), c.getScale()),
                    ClickHouseDataType.Decimal256);

            // date, time, datetime and IPs
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseDateValue.of(r, BinaryStreamUtils.readDate(i, f.getServerTimeZone())),
                    (v, f, c, o) -> BinaryStreamUtils.writeDate(o, v.asDate(), f.getServerTimeZone()),
                    ClickHouseDataType.Date);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseDateValue.of(r, BinaryStreamUtils.readDate32(i, f.getServerTimeZone())),
                    (v, f, c, o) -> BinaryStreamUtils.writeDate(o, v.asDate(), f.getServerTimeZone()),
                    ClickHouseDataType.Date32);
            buildMappings(deserializers, serializers, (r, f, c, i) -> c.getTimeZone() == null
                    ? ClickHouseDateTimeValue.of(r,
                            (c.getScale() > 0 ? BinaryStreamUtils.readDateTime64(i, c.getScale(), f.getServerTimeZone())
                                    : BinaryStreamUtils.readDateTime(i, f.getServerTimeZone())),
                            c.getScale())
                    : ClickHouseOffsetDateTimeValue.of(r,
                            (c.getScale() > 0 ? BinaryStreamUtils.readDateTime64(i, c.getScale(), c.getTimeZone())
                                    : BinaryStreamUtils.readDateTime(i, c.getTimeZone())),
                            c.getScale(), c.getTimeZone()),
                    (v, f, c, o) -> BinaryStreamUtils.writeDateTime(o, v.asDateTime(), c.getScale(),
                            c.getTimeZoneOrDefault(f.getServerTimeZone())),
                    ClickHouseDataType.DateTime);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> c.getTimeZone() == null
                            ? ClickHouseDateTimeValue.of(r, BinaryStreamUtils.readDateTime(i, f.getServerTimeZone()), 0)
                            : ClickHouseOffsetDateTimeValue.of(r, BinaryStreamUtils.readDateTime(i, c.getTimeZone()), 0,
                                    c.getTimeZone()),
                    (v, f, c, o) -> BinaryStreamUtils.writeDateTime32(o, v.asDateTime(),
                            c.getTimeZoneOrDefault(f.getServerTimeZone())),
                    ClickHouseDataType.DateTime32);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> c.getTimeZone() == null ? ClickHouseDateTimeValue.of(r,
                            BinaryStreamUtils.readDateTime64(i, c.getScale(), f.getServerTimeZone()), c.getScale())
                            : ClickHouseOffsetDateTimeValue.of(r,
                                    BinaryStreamUtils.readDateTime64(i, c.getScale(), c.getTimeZone()), c.getScale(),
                                    c.getTimeZone()),
                    (v, f, c, o) -> BinaryStreamUtils.writeDateTime64(o, v.asDateTime(), c.getScale(),
                            c.getTimeZoneOrDefault(f.getServerTimeZone())),
                    ClickHouseDataType.DateTime64);

            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseIpv4Value.of(r, BinaryStreamUtils.readInet4Address(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInet4Address(o, v.asInet4Address()),
                    ClickHouseDataType.IPv4);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseIpv6Value.of(r, BinaryStreamUtils.readInet6Address(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInet6Address(o, v.asInet6Address()),
                    ClickHouseDataType.IPv6);

            // string and uuid
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseStringValue.of(r, BinaryStreamUtils.readFixedString(i, c.getPrecision())),
                    (v, f, c, o) -> BinaryStreamUtils.writeFixedString(o, v.asString(c.getPrecision()),
                            c.getPrecision()),
                    ClickHouseDataType.FixedString);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseStringValue.of(r, BinaryStreamUtils.readString(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeString(o, v.asString()), ClickHouseDataType.String);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseUuidValue.of(r, BinaryStreamUtils.readUuid(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeUuid(o, v.asUuid()), ClickHouseDataType.UUID);

            // geo types
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseGeoPointValue.of(r, BinaryStreamUtils.readGeoPoint(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeGeoPoint(o, v.asObject(double[].class)),
                    ClickHouseDataType.Point);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseGeoRingValue.of(r, BinaryStreamUtils.readGeoRing(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeGeoRing(o, v.asObject(double[][].class)),
                    ClickHouseDataType.Ring);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseGeoPolygonValue.of(r, BinaryStreamUtils.readGeoPolygon(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeGeoPolygon(o, v.asObject(double[][][].class)),
                    ClickHouseDataType.Polygon);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseGeoMultiPolygonValue.of(r, BinaryStreamUtils.readGeoMultiPolygon(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeGeoMultiPolygon(o, v.asObject(double[][][][].class)),
                    ClickHouseDataType.MultiPolygon);

            // advanced types
            buildMappings(deserializers, serializers, (r, f, c, i) -> {
                if (c.getAggregateFunction() == ClickHouseAggregateFunction.groupBitmap) {
                    return ClickHouseBitmapValue
                            .of(BinaryStreamUtils.readBitmap(i, c.getNestedColumns().get(0).getDataType()));
                }
                return null;
            }, (v, f, c, o) -> {
                if (c.getAggregateFunction() == ClickHouseAggregateFunction.groupBitmap) {
                    BinaryStreamUtils.writeBitmap(o, v.asObject(ClickHouseBitmap.class));
                }
            }, ClickHouseDataType.AggregateFunction);
            buildMappings(deserializers, serializers, (r, f, c, i) -> {
                int length = BinaryStreamUtils.readVarInt(i);
                if (r == null) {
                    r = ClickHouseValues.newValue(c);
                }
                return readArray(r, f, c.getNestedColumns().get(0), c.getArrayBaseColumn(), i, length,
                        c.getArrayNestedLevel());
            }, this::writeArray, ClickHouseDataType.Array);
            buildMappings(deserializers, serializers, (r, f, c, i) -> {
                Map<Object, Object> map = new LinkedHashMap<>();
                ClickHouseColumn keyCol = c.getKeyInfo();
                ClickHouseColumn valCol = c.getValueInfo();
                for (int k = 0, len = BinaryStreamUtils.readVarInt(i); k < len; k++) {
                    map.put(deserialize(null, f, keyCol, i).asObject(), deserialize(null, f, valCol, i).asObject());
                }
                return ClickHouseMapValue.of(map, valCol.getDataType().getObjectClass(),
                        valCol.getDataType().getObjectClass());
            }, (v, f, c, o) -> {
                Map<Object, Object> map = v.asMap();
                BinaryStreamUtils.writeVarInt(o, map.size());
                if (!map.isEmpty()) {
                    ClickHouseColumn keyCol = c.getKeyInfo();
                    ClickHouseColumn valCol = c.getValueInfo();
                    ClickHouseValue kVal = ClickHouseValues.newValue(keyCol);
                    ClickHouseValue vVal = ClickHouseValues.newValue(valCol);
                    for (Entry<Object, Object> e : map.entrySet()) {
                        serialize(kVal.update(e.getKey()), f, keyCol, o);
                        serialize(vVal.update(e.getValue()), f, valCol, o);
                    }
                }
            }, ClickHouseDataType.Map);
            buildMappings(deserializers, serializers, (r, f, c, i) -> {
                int count = c.getNestedColumns().size();
                String[] names = new String[count];
                Object[][] values = new Object[count][];
                int l = 0;
                for (ClickHouseColumn col : c.getNestedColumns()) {
                    names[l] = col.getColumnName();
                    int k = BinaryStreamUtils.readVarInt(i);
                    Object[] nvalues = new Object[k];
                    for (int j = 0; j < k; j++) {
                        nvalues[j] = deserialize(null, f, col, i).asObject();
                    }
                    values[l++] = nvalues;
                }
                return ClickHouseNestedValue.of(r, c.getNestedColumns(), values);
            }, (v, f, c, o) -> {
                Object[][] values = (Object[][]) v.asObject();
                int l = 0;
                for (ClickHouseColumn col : c.getNestedColumns()) {
                    Object[] nvalues = values[l++];
                    int k = nvalues.length;
                    ClickHouseValue nv = ClickHouseValues.newValue(col);
                    BinaryStreamUtils.writeVarInt(o, k);
                    for (int j = 0; j < k; j++) {
                        serialize(nv.update(nvalues[j]), f, col, o);
                    }
                }
            }, ClickHouseDataType.Nested);
            buildMappings(deserializers, serializers, (r, f, c, i) -> {
                List<Object> tupleValues = new ArrayList<>(c.getNestedColumns().size());
                for (ClickHouseColumn col : c.getNestedColumns()) {
                    tupleValues.add(deserialize(null, f, col, i).asObject());
                }
                return ClickHouseTupleValue.of(r, tupleValues);
            }, (v, f, c, o) -> {
                List<Object> tupleValues = v.asTuple();
                Iterator<Object> tupleIterator = tupleValues.iterator();
                for (ClickHouseColumn col : c.getNestedColumns()) {
                    // FIXME tooooo slow
                    ClickHouseValue tv = ClickHouseValues.newValue(col);
                    if (tupleIterator.hasNext()) {
                        serialize(tv.update(tupleIterator.next()), f, col, o);
                    } else {
                        serialize(tv, f, col, o);
                    }
                }
            }, ClickHouseDataType.Tuple);
        }

        @SuppressWarnings("unchecked")
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseConfig config, ClickHouseColumn column,
                ClickHouseInputStream input) throws IOException {
            if (column.isNullable() && BinaryStreamUtils.readNull(input)) {
                return ref == null ? ClickHouseValues.newValue(column) : ref.resetToNullOrEmpty();
            }

            ClickHouseDeserializer<ClickHouseValue> func = (ClickHouseDeserializer<ClickHouseValue>) deserializers
                    .get(column.getDataType());
            if (func == null) {
                throw new IllegalArgumentException(ERROR_UNKNOWN_DATA_TYPE + column.getDataType().name());
            }
            return func.deserialize(ref, config, column, input);
        }

        @SuppressWarnings("unchecked")
        public void serialize(ClickHouseValue value, ClickHouseConfig config, ClickHouseColumn column,
                OutputStream output) throws IOException {
            if (column.isNullable()) { // always false for geo types, and Array, Nested, Map and Tuple etc.
                if (value.isNullOrEmpty()) {
                    BinaryStreamUtils.writeNull(output);
                    return;
                } else {
                    BinaryStreamUtils.writeNonNull(output);
                }
            }

            ClickHouseSerializer<ClickHouseValue> func = (ClickHouseSerializer<ClickHouseValue>) serializers
                    .get(column.getDataType());
            if (func == null) {
                throw new IllegalArgumentException(ERROR_UNKNOWN_DATA_TYPE + column.getDataType().name());
            }
            func.serialize(value, config, column, output);
        }
    }

    public static MappedFunctions getMappedFunctions() {
        return MappedFunctions.instance;
    }

    // TODO this is where ASM should come into play...
    private class Records implements Iterator<ClickHouseRecord> {
        private final Supplier<ClickHouseSimpleRecord> factory;
        private ClickHouseSimpleRecord record;

        Records() {
            int size = columns.size();
            if (config.isReuseValueWrapper()) {
                ClickHouseValue[] values = new ClickHouseValue[size];
                record = new ClickHouseSimpleRecord(columns, values);
                factory = () -> record;
            } else {
                factory = () -> new ClickHouseSimpleRecord(columns, new ClickHouseValue[size]);
            }
        }

        ClickHouseRecord readNextRow() {
            int index = 0;
            int size = columns.size();
            ClickHouseSimpleRecord currentRow = factory.get();
            ClickHouseValue[] values = currentRow.getValues();
            ClickHouseColumn column = null;
            try {
                MappedFunctions m = getMappedFunctions();
                for (; index < size; index++) {
                    column = columns.get(index);
                    values[index] = m.deserialize(values[index], config, column, input);
                }
            } catch (EOFException e) {
                if (index == 0) { // end of the stream, which is fine
                    values = null;
                } else {
                    throw new UncheckedIOException(
                            ClickHouseUtils.format("Reached end of the stream when reading column #%d(total %d): %s",
                                    index + 1, size, column),
                            e);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(
                        ClickHouseUtils.format("Failed to read column #%d(total %d): %s", index + 1, size, column), e);
            }

            return currentRow;
        }

        @Override
        public boolean hasNext() {
            try {
                return input.available() > 0;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public ClickHouseRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more record");
            }

            return readNextRow();
        }
    }

    @Override
    protected List<ClickHouseColumn> readColumns() throws IOException {
        if (!config.getFormat().hasHeader()) {
            return Collections.emptyList();
        }

        int size = 0;
        try {
            size = BinaryStreamUtils.readVarInt(input);
        } catch (EOFException e) {
            // no result returned
            return Collections.emptyList();
        }

        String[] names = new String[ClickHouseChecker.between(size, "size", 0, Integer.MAX_VALUE)];
        for (int i = 0; i < size; i++) {
            names[i] = BinaryStreamUtils.readString(input);
        }

        List<ClickHouseColumn> columns = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            columns.add(ClickHouseColumn.of(names[i], BinaryStreamUtils.readString(input)));
        }

        return columns;
    }

    public ClickHouseRowBinaryProcessor(ClickHouseConfig config, ClickHouseInputStream input, OutputStream output,
            List<ClickHouseColumn> columns, Map<String, Object> settings) throws IOException {
        super(config, input, output, columns, settings);
    }

    @Override
    public Iterable<ClickHouseRecord> records() {
        return columns.isEmpty() ? Collections.emptyList() : new Iterable<ClickHouseRecord>() {
            @Override
            public Iterator<ClickHouseRecord> iterator() {
                return new Records();
            }
        };
    }
}
