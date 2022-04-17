package com.clickhouse.client.data;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.clickhouse.client.ClickHouseAggregateFunction;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseDataProcessor;
import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.client.ClickHouseDeserializer;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseOutputStream;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseSerializer;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;

/**
 * Data processor for handling {@link ClickHouseFormat#RowBinary} and
 * {@link ClickHouseFormat#RowBinaryWithNamesAndTypes} two formats.
 */
public class ClickHouseRowBinaryProcessor extends ClickHouseDataProcessor {
    public static class MappedFunctions
            implements ClickHouseDeserializer<ClickHouseValue>, ClickHouseSerializer<ClickHouseValue> {
        private static final MappedFunctions instance = new MappedFunctions();

        private void writeArray(ClickHouseValue value, ClickHouseConfig config, ClickHouseColumn column,
                ClickHouseOutputStream output) throws IOException {
            ClickHouseColumn nestedColumn = column.getNestedColumns().get(0);
            ClickHouseColumn baseColumn = column.getArrayBaseColumn();
            int level = column.getArrayNestedLevel();
            Class<?> javaClass = baseColumn.getPrimitiveClass();
            if (level > 1 || !javaClass.isPrimitive()) {
                Object[] array = value.asArray();
                ClickHouseValue v = ClickHouseValues.newValue(config, nestedColumn);
                int length = array.length;
                output.writeVarInt(length);
                for (int i = 0; i < length; i++) {
                    serialize(v.update(array[i]), config, nestedColumn, output);
                }
            } else {
                ClickHouseValue v = ClickHouseValues.newValue(config, baseColumn);
                if (byte.class == javaClass) {
                    byte[] array = (byte[]) value.asObject();
                    int length = array.length;
                    output.writeVarInt(length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else if (short.class == javaClass) {
                    short[] array = (short[]) value.asObject();
                    int length = array.length;
                    output.writeVarInt(length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else if (int.class == javaClass) {
                    int[] array = (int[]) value.asObject();
                    int length = array.length;
                    output.writeVarInt(length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else if (long.class == javaClass) {
                    long[] array = (long[]) value.asObject();
                    int length = array.length;
                    output.writeVarInt(length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else if (float.class == javaClass) {
                    float[] array = (float[]) value.asObject();
                    int length = array.length;
                    output.writeVarInt(length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else if (double.class == javaClass) {
                    double[] array = (double[]) value.asObject();
                    int length = array.length;
                    output.writeVarInt(length);
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
            Class<?> javaClass = baseColumn.getPrimitiveClass();
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

        private final Map<ClickHouseAggregateFunction, ClickHouseDeserializer<ClickHouseValue>> aggDeserializers;
        private final Map<ClickHouseAggregateFunction, ClickHouseSerializer<ClickHouseValue>> aggSerializers;

        private final Map<ClickHouseDataType, ClickHouseDeserializer<? extends ClickHouseValue>> deserializers;
        private final Map<ClickHouseDataType, ClickHouseSerializer<? extends ClickHouseValue>> serializers;

        private void buildMappingsForAggregateFunctions() {
            // aggregate functions
            // buildAggMappings(aggDeserializers, aggSerializers,
            // (r, f, c, i) -> {
            // BinaryStreamUtils.readInt8(i); // always 1?
            // return deserialize(r, f, c.getNestedColumns().get(0), i);
            // },
            // (v, f, c, o) -> {
            // // no that simple:
            // // * select anyState(n) from (select '5' where 0) => FFFF
            // // * select anyState(n) from (select '5') => 0200 0000 3500
            // BinaryStreamUtils.writeInt8(o, (byte) 1);
            // serialize(v, f, c.getNestedColumns().get(0), o);
            // }, ClickHouseAggregateFunction.any);
            buildAggMappings(aggDeserializers, aggSerializers,
                    (r, f, c, i) -> ClickHouseBitmapValue
                            .of(BinaryStreamUtils.readBitmap(i, c.getNestedColumns().get(0).getDataType())),
                    (v, f, c, o) -> BinaryStreamUtils.writeBitmap(o, v.asObject(ClickHouseBitmap.class)),
                    ClickHouseAggregateFunction.groupBitmap);

            // now the data type
            buildMappings(deserializers, serializers, (r, f, c, i) -> aggDeserializers
                    .getOrDefault(c.getAggregateFunction(), ClickHouseDeserializer.NOT_SUPPORTED)
                    .deserialize(r, f, c, i),
                    (v, f, c, o) -> aggSerializers
                            .getOrDefault(c.getAggregateFunction(), ClickHouseSerializer.NOT_SUPPORTED)
                            .serialize(v, f, c, o),
                    ClickHouseDataType.AggregateFunction);
        }

        private void buildMappingsForDataTypes() {
            // enums
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseEnumValue.of(r, c.getEnumConstants(), i.readByte()),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt8(o, v.asByte()), ClickHouseDataType.Enum,
                    ClickHouseDataType.Enum8);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseEnumValue.of(r, c.getEnumConstants(), BinaryStreamUtils.readInt16(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt16(o, v.asShort()), ClickHouseDataType.Enum16);
            // bool and numbers
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseBoolValue.of(r, BinaryStreamUtils.readBoolean(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeBoolean(o, v.asBoolean()), ClickHouseDataType.Bool);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseByteValue.of(r, i.readByte()),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt8(o, v.asByte()), ClickHouseDataType.Int8);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseShortValue.of(r, (short) (0xFF & i.readByte())),
                    (v, f, c, o) -> BinaryStreamUtils.writeUnsignedInt8(o, v.asInteger()), ClickHouseDataType.UInt8);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseShortValue.of(r, BinaryStreamUtils.readInt16(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt16(o, v.asShort()), ClickHouseDataType.Int16);
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
                    (r, f, c, i) -> ClickHouseDateValue.of(r,
                            BinaryStreamUtils.readDate(i, f.getTimeZoneForDate())),
                    (v, f, c, o) -> BinaryStreamUtils.writeDate(o, v.asDate(), f.getTimeZoneForDate()),
                    ClickHouseDataType.Date);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseDateValue.of(r,
                            BinaryStreamUtils.readDate32(i, f.getTimeZoneForDate())),
                    (v, f, c, o) -> BinaryStreamUtils.writeDate32(o, v.asDate(), f.getTimeZoneForDate()),
                    ClickHouseDataType.Date32);
            buildMappings(deserializers, serializers, (r, f, c, i) -> c.getTimeZone() == null
                    ? ClickHouseDateTimeValue.of(r,
                            (c.getScale() > 0 ? BinaryStreamUtils.readDateTime64(i, c.getScale(), f.getUseTimeZone())
                                    : BinaryStreamUtils.readDateTime(i, f.getUseTimeZone())),
                            c.getScale(), f.getUseTimeZone())
                    : ClickHouseOffsetDateTimeValue.of(r,
                            (c.getScale() > 0 ? BinaryStreamUtils.readDateTime64(i, c.getScale(), c.getTimeZone())
                                    : BinaryStreamUtils.readDateTime(i, c.getTimeZone())),
                            c.getScale(), c.getTimeZone()),
                    (v, f, c, o) -> BinaryStreamUtils.writeDateTime(o, v.asDateTime(), c.getScale(),
                            c.getTimeZoneOrDefault(f.getUseTimeZone())),
                    ClickHouseDataType.DateTime);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> c.getTimeZone() == null
                            ? ClickHouseDateTimeValue.of(r, BinaryStreamUtils.readDateTime(i, f.getUseTimeZone()), 0,
                                    f.getUseTimeZone())
                            : ClickHouseOffsetDateTimeValue.of(r, BinaryStreamUtils.readDateTime(i, c.getTimeZone()), 0,
                                    c.getTimeZone()),
                    (v, f, c, o) -> BinaryStreamUtils.writeDateTime32(o, v.asDateTime(),
                            c.getTimeZoneOrDefault(f.getUseTimeZone())),
                    ClickHouseDataType.DateTime32);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> c.getTimeZone() == null ? ClickHouseDateTimeValue.of(r,
                            BinaryStreamUtils.readDateTime64(i, c.getScale(), f.getUseTimeZone()), c.getScale(),
                            f.getUseTimeZone())
                            : ClickHouseOffsetDateTimeValue.of(r,
                                    BinaryStreamUtils.readDateTime64(i, c.getScale(), c.getTimeZone()), c.getScale(),
                                    c.getTimeZone()),
                    (v, f, c, o) -> BinaryStreamUtils.writeDateTime64(o, v.asDateTime(), c.getScale(),
                            c.getTimeZoneOrDefault(f.getUseTimeZone())),
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
                    (r, f, c, i) -> ClickHouseStringValue.of(r, i.readBytes(c.getPrecision())),
                    (v, f, c, o) -> o.write(v.asBinary(c.getPrecision())), ClickHouseDataType.FixedString);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ClickHouseStringValue.of(r, i.readBytes(i.readVarInt())),
                    (v, f, c, o) -> BinaryStreamUtils.writeString(o, v.asBinary()), ClickHouseDataType.String);
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
            buildMappings(deserializers, serializers, (r, f, c, i) -> deserialize(r, f, c.getNestedColumns().get(0), i),
                    (v, f, c, o) -> serialize(v, f, c.getNestedColumns().get(0), o),
                    ClickHouseDataType.SimpleAggregateFunction);
            buildMappings(deserializers, serializers, (r, f, c, i) -> {
                int length = BinaryStreamUtils.readVarInt(i);
                if (r == null) {
                    r = ClickHouseValues.newValue(f, c);
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
                return ClickHouseMapValue.of(map, keyCol.getObjectClass(), valCol.getObjectClass());
            }, (v, f, c, o) -> {
                Map<Object, Object> map = v.asMap();
                BinaryStreamUtils.writeVarInt(o, map.size());
                if (!map.isEmpty()) {
                    ClickHouseColumn keyCol = c.getKeyInfo();
                    ClickHouseColumn valCol = c.getValueInfo();
                    ClickHouseValue kVal = ClickHouseValues.newValue(f, keyCol);
                    ClickHouseValue vVal = ClickHouseValues.newValue(f, valCol);
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
                    ClickHouseValue nv = ClickHouseValues.newValue(f, col);
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
                    ClickHouseValue tv = ClickHouseValues.newValue(f, col);
                    if (tupleIterator.hasNext()) {
                        serialize(tv.update(tupleIterator.next()), f, col, o);
                    } else {
                        serialize(tv, f, col, o);
                    }
                }
            }, ClickHouseDataType.Tuple);
        }

        private MappedFunctions() {
            aggDeserializers = new EnumMap<>(ClickHouseAggregateFunction.class);
            aggSerializers = new EnumMap<>(ClickHouseAggregateFunction.class);

            deserializers = new EnumMap<>(ClickHouseDataType.class);
            serializers = new EnumMap<>(ClickHouseDataType.class);

            buildMappingsForAggregateFunctions();
            buildMappingsForDataTypes();
        }

        @SuppressWarnings("unchecked")
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseConfig config, ClickHouseColumn column,
                ClickHouseInputStream input) throws IOException {
            if (column.isNullable() && BinaryStreamUtils.readNull(input)) {
                return ref == null ? ClickHouseValues.newValue(config, column) : ref.resetToNullOrEmpty();
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
                ClickHouseOutputStream output) throws IOException {
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

    @Override
    protected void readAndFill(ClickHouseRecord r) throws IOException {
        MappedFunctions m = getMappedFunctions();
        for (; readPosition < columns.length; readPosition++) {
            templates[readPosition] = m.deserialize(r.getValue(readPosition), config, columns[readPosition], input);
        }
    }

    @Override
    protected void readAndFill(ClickHouseValue value, ClickHouseColumn column) throws IOException {
        templates[readPosition] = getMappedFunctions().deserialize(value, config, column, input);
    }

    @Override
    protected List<ClickHouseColumn> readColumns() throws IOException {
        if (!config.getFormat().hasHeader()) {
            return Collections.emptyList();
        }

        int size = 0;
        try {
            size = input.readVarInt();
        } catch (EOFException e) {
            // no result returned
            return Collections.emptyList();
        }

        String[] names = new String[ClickHouseChecker.between(size, "size", 0, Integer.MAX_VALUE)];
        for (int i = 0; i < size; i++) {
            names[i] = input.readUnicodeString();
        }

        List<ClickHouseColumn> columns = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            // a bit risky here - what if ClickHouse support user type?
            columns.add(ClickHouseColumn.of(names[i], input.readAsciiString()));
        }

        return columns;
    }

    /**
     * Default constructor.
     *
     * @param config   non-null confinguration contains information like format
     * @param input    input stream for deserialization, can be null when
     *                 {@code output} is available
     * @param output   outut stream for serialization, can be null when
     *                 {@code input} is available
     * @param columns  nullable columns
     * @param settings nullable settings
     * @throws IOException when failed to read columns from input stream
     */
    public ClickHouseRowBinaryProcessor(ClickHouseConfig config, ClickHouseInputStream input,
            ClickHouseOutputStream output, List<ClickHouseColumn> columns, Map<String, Object> settings)
            throws IOException {
        super(config, input, output, columns, settings);
    }

    @Override
    public void write(ClickHouseValue value, ClickHouseColumn column) throws IOException {
        if (output == null || column == null) {
            throw new IllegalArgumentException("Cannot write any value when output stream or column is null");
        }

        getMappedFunctions().serialize(value, config, column, output);
    }
}
