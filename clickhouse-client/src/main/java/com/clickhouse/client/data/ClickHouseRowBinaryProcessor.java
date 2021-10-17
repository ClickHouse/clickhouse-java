package com.clickhouse.client.data;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseDataProcessor;
import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.client.ClickHouseDeserializer;
import com.clickhouse.client.ClickHouseFormat;
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

        private ClickHouseValue readArray(ClickHouseValue ref, ClickHouseColumn nestedColumn,
                ClickHouseColumn baseColumn, InputStream input, int length, int level) throws IOException {
            Class<?> javaClass = baseColumn.getDataType().getPrimitiveClass();
            if (level > 1 || !javaClass.isPrimitive()) {
                Object[] array = (Object[]) ClickHouseValues.createPrimitiveArray(javaClass, length, level);
                for (int i = 0; i < length; i++) {
                    array[i] = deserialize(nestedColumn, null, input).asObject();
                }
                ref.update(array);
            } else {
                if (byte.class == javaClass) {
                    byte[] array = new byte[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(baseColumn, null, input).asByte();
                    }
                    ref.update(array);
                } else if (short.class == javaClass) {
                    short[] array = new short[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(baseColumn, null, input).asShort();
                    }
                    ref.update(array);
                } else if (int.class == javaClass) {
                    int[] array = new int[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(baseColumn, null, input).asInteger();
                    }
                    ref.update(array);
                } else if (long.class == javaClass) {
                    long[] array = new long[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(baseColumn, null, input).asLong();
                    }
                    ref.update(array);
                } else if (float.class == javaClass) {
                    float[] array = new float[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(baseColumn, null, input).asFloat();
                    }
                    ref.update(array);
                } else if (double.class == javaClass) {
                    double[] array = new double[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(baseColumn, null, input).asDouble();
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
                    (r, c, i) -> ClickHouseByteValue.of(r, BinaryStreamUtils.readInt8(i)),
                    (v, c, o) -> BinaryStreamUtils.writeInt8(o, v.asByte()), ClickHouseDataType.Enum,
                    ClickHouseDataType.Enum8, ClickHouseDataType.Int8);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseShortValue.of(r, BinaryStreamUtils.readUnsignedInt8(i)),
                    (v, c, o) -> BinaryStreamUtils.writeUnsignedInt8(o, v.asInteger()), ClickHouseDataType.UInt8);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseShortValue.of(r, BinaryStreamUtils.readInt16(i)),
                    (v, c, o) -> BinaryStreamUtils.writeInt16(o, v.asShort()), ClickHouseDataType.Enum16,
                    ClickHouseDataType.Int16);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseIntegerValue.of(r, BinaryStreamUtils.readUnsignedInt16(i)),
                    (v, c, o) -> BinaryStreamUtils.writeUnsignedInt16(o, v.asInteger()), ClickHouseDataType.UInt16);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseIntegerValue.of(r, BinaryStreamUtils.readInt32(i)),
                    (v, c, o) -> BinaryStreamUtils.writeInt32(o, v.asInteger()), ClickHouseDataType.Int32);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseLongValue.of(r, false, BinaryStreamUtils.readUnsignedInt32(i)),
                    (v, c, o) -> BinaryStreamUtils.writeUnsignedInt32(o, v.asLong()), ClickHouseDataType.UInt32);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseLongValue.of(r, false, BinaryStreamUtils.readInt64(i)),
                    (v, c, o) -> BinaryStreamUtils.writeInt64(o, v.asLong()), ClickHouseDataType.IntervalYear,
                    ClickHouseDataType.IntervalQuarter, ClickHouseDataType.IntervalMonth,
                    ClickHouseDataType.IntervalWeek, ClickHouseDataType.IntervalDay, ClickHouseDataType.IntervalHour,
                    ClickHouseDataType.IntervalMinute, ClickHouseDataType.IntervalSecond, ClickHouseDataType.Int64);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseLongValue.of(r, true, BinaryStreamUtils.readInt64(i)),
                    (v, c, o) -> BinaryStreamUtils.writeInt64(o, v.asLong()), ClickHouseDataType.UInt64);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseBigIntegerValue.of(r, BinaryStreamUtils.readInt128(i)),
                    (v, c, o) -> BinaryStreamUtils.writeInt128(o, v.asBigInteger()), ClickHouseDataType.Int128);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseBigIntegerValue.of(r, BinaryStreamUtils.readUnsignedInt128(i)),
                    (v, c, o) -> BinaryStreamUtils.writeUnsignedInt128(o, v.asBigInteger()),
                    ClickHouseDataType.UInt128);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseBigIntegerValue.of(r, BinaryStreamUtils.readInt256(i)),
                    (v, c, o) -> BinaryStreamUtils.writeInt256(o, v.asBigInteger()), ClickHouseDataType.Int256);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseBigIntegerValue.of(r, BinaryStreamUtils.readUnsignedInt256(i)),
                    (v, c, o) -> BinaryStreamUtils.writeUnsignedInt256(o, v.asBigInteger()),
                    ClickHouseDataType.UInt256);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseFloatValue.of(r, BinaryStreamUtils.readFloat32(i)),
                    (v, c, o) -> BinaryStreamUtils.writeFloat32(o, v.asFloat()), ClickHouseDataType.Float32);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseDoubleValue.of(r, BinaryStreamUtils.readFloat64(i)),
                    (v, c, o) -> BinaryStreamUtils.writeFloat64(o, v.asDouble()), ClickHouseDataType.Float64);

            // decimals
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseBigDecimalValue.of(r,
                            BinaryStreamUtils.readDecimal(i, c.getPrecision(), c.getScale())),
                    (v, c, o) -> BinaryStreamUtils.writeDecimal(o, v.asBigDecimal(c.getScale()), c.getPrecision(),
                            c.getScale()),
                    ClickHouseDataType.Decimal);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseBigDecimalValue.of(r, BinaryStreamUtils.readDecimal32(i, c.getScale())),
                    (v, c, o) -> BinaryStreamUtils.writeDecimal32(o, v.asBigDecimal(c.getScale()), c.getScale()),
                    ClickHouseDataType.Decimal32);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseBigDecimalValue.of(r, BinaryStreamUtils.readDecimal64(i, c.getScale())),
                    (v, c, o) -> BinaryStreamUtils.writeDecimal64(o, v.asBigDecimal(c.getScale()), c.getScale()),
                    ClickHouseDataType.Decimal64);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseBigDecimalValue.of(r, BinaryStreamUtils.readDecimal128(i, c.getScale())),
                    (v, c, o) -> BinaryStreamUtils.writeDecimal128(o, v.asBigDecimal(c.getScale()), c.getScale()),
                    ClickHouseDataType.Decimal128);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseBigDecimalValue.of(r, BinaryStreamUtils.readDecimal256(i, c.getScale())),
                    (v, c, o) -> BinaryStreamUtils.writeDecimal256(o, v.asBigDecimal(c.getScale()), c.getScale()),
                    ClickHouseDataType.Decimal256);

            // date, time, datetime and IPs
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseDateValue.of(r, BinaryStreamUtils.readDate(i)),
                    (v, c, o) -> BinaryStreamUtils.writeDate(o, v.asDate()), ClickHouseDataType.Date);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseDateValue.of(r, BinaryStreamUtils.readDate32(i)),
                    (v, c, o) -> BinaryStreamUtils.writeDate(o, v.asDate()), ClickHouseDataType.Date32);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseDateTimeValue.of(r,
                            (c.getScale() > 0 ? BinaryStreamUtils.readDateTime64(i, c.getScale())
                                    : BinaryStreamUtils.readDateTime(i)),
                            c.getScale()),
                    (v, c, o) -> BinaryStreamUtils.writeDateTime(o, v.asDateTime(), c.getScale()),
                    ClickHouseDataType.DateTime);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseDateTimeValue.of(r, BinaryStreamUtils.readDateTime(i), 0),
                    (v, c, o) -> BinaryStreamUtils.writeDateTime32(o, v.asDateTime()), ClickHouseDataType.DateTime32);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseDateTimeValue.of(r, BinaryStreamUtils.readDateTime64(i, c.getScale()),
                            c.getScale()),
                    (v, c, o) -> BinaryStreamUtils.writeDateTime64(o, v.asDateTime(), c.getScale()),
                    ClickHouseDataType.DateTime64);

            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseIpv4Value.of(r, BinaryStreamUtils.readInet4Address(i)),
                    (v, c, o) -> BinaryStreamUtils.writeInet4Address(o, v.asInet4Address()), ClickHouseDataType.IPv4);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseIpv6Value.of(r, BinaryStreamUtils.readInet6Address(i)),
                    (v, c, o) -> BinaryStreamUtils.writeInet6Address(o, v.asInet6Address()), ClickHouseDataType.IPv6);

            // string and uuid
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseStringValue.of(r, BinaryStreamUtils.readFixedString(i, c.getPrecision())),
                    (v, c, o) -> BinaryStreamUtils.writeFixedString(o, v.asString(c.getPrecision()), c.getPrecision()),
                    ClickHouseDataType.FixedString);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseStringValue.of(r, BinaryStreamUtils.readString(i)),
                    (v, c, o) -> BinaryStreamUtils.writeString(o, v.asString()), ClickHouseDataType.String);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseUuidValue.of(r, BinaryStreamUtils.readUuid(i)),
                    (v, c, o) -> BinaryStreamUtils.writeUuid(o, v.asUuid()), ClickHouseDataType.UUID);

            // geo types
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseGeoPointValue.of(r, BinaryStreamUtils.readGeoPoint(i)), (v, c, o) -> {
                    }, ClickHouseDataType.Point);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseGeoRingValue.of(r, BinaryStreamUtils.readGeoRing(i)), (v, c, o) -> {
                    }, ClickHouseDataType.Ring);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseGeoPolygonValue.of(r, BinaryStreamUtils.readGeoPolygon(i)), (v, c, o) -> {
                    }, ClickHouseDataType.Polygon);
            buildMappings(deserializers, serializers,
                    (r, c, i) -> ClickHouseGeoMultiPolygonValue.of(r, BinaryStreamUtils.readGeoMultiPolygon(i)),
                    (v, c, o) -> {
                    }, ClickHouseDataType.MultiPolygon);

            // advanced types
            buildMappings(deserializers, serializers, (r, c, i) -> {
                int length = BinaryStreamUtils.readVarInt(i);
                if (r == null) {
                    r = ClickHouseValues.newValue(c);
                }
                return readArray(r, c.getNestedColumns().get(0), c.getArrayBaseColumn(), i, length,
                        c.getArrayNestedLevel());
            }, (v, c, o) -> {
            }, ClickHouseDataType.Array);
            buildMappings(deserializers, serializers, (r, c, i) -> {
                Map<Object, Object> map = new LinkedHashMap<>();
                ClickHouseColumn keyCol = c.getKeyInfo();
                ClickHouseColumn valCol = c.getValueInfo();
                for (int k = 0, len = BinaryStreamUtils.readVarInt(i); k < len; k++) {
                    map.put(deserialize(keyCol, null, i).asObject(), deserialize(valCol, null, i).asObject());
                }
                return ClickHouseMapValue.of(map, valCol.getDataType().getObjectClass(),
                        valCol.getDataType().getObjectClass());
            }, (v, c, o) -> {
            }, ClickHouseDataType.Map);
            buildMappings(deserializers, serializers, (r, c, i) -> {
                int count = c.getNestedColumns().size();
                String[] names = new String[count];
                Object[][] values = new Object[count][];
                int l = 0;
                for (ClickHouseColumn col : c.getNestedColumns()) {
                    names[l] = col.getColumnName();
                    int k = BinaryStreamUtils.readVarInt(i);
                    Object[] nvalues = new Object[k];
                    for (int j = 0; j < k; j++) {
                        nvalues[j] = deserialize(col, null, i).asObject();
                    }
                    values[l++] = nvalues;
                }
                return ClickHouseNestedValue.of(r, c.getNestedColumns(), values);
            }, (v, c, o) -> {
            }, ClickHouseDataType.Nested);
            buildMappings(deserializers, serializers, (r, c, i) -> {
                List<Object> tupleValues = new ArrayList<>(c.getNestedColumns().size());
                for (ClickHouseColumn col : c.getNestedColumns()) {
                    tupleValues.add(deserialize(col, null, i).asObject());
                }
                return ClickHouseTupleValue.of(r, tupleValues);
            }, (v, c, o) -> {
            }, ClickHouseDataType.Tuple);
        }

        @SuppressWarnings("unchecked")
        public ClickHouseValue deserialize(ClickHouseColumn column, ClickHouseValue ref, InputStream input)
                throws IOException {
            if (column.isNullable() && BinaryStreamUtils.readNull(input)) {
                return ref == null ? ClickHouseValues.newValue(column) : ref.resetToNullOrEmpty();
            }

            ClickHouseDeserializer<ClickHouseValue> func = (ClickHouseDeserializer<ClickHouseValue>) deserializers
                    .get(column.getDataType());
            if (func == null) {
                throw new IllegalArgumentException(ERROR_UNKNOWN_DATA_TYPE + column.getDataType().name());
            }
            return func.deserialize(ref, column, input);
        }

        @SuppressWarnings("unchecked")
        public void serialize(ClickHouseColumn column, ClickHouseValue value, OutputStream output) throws IOException {
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
            func.serialize(value, column, output);
        }
    }

    public static MappedFunctions getMappedFunctions() {
        return MappedFunctions.instance;
    }

    private class Records implements Iterator<ClickHouseRecord> {
        private final Supplier<ClickHouseValue[]> factory;
        private ClickHouseValue[] values;

        Records() {
            int size = columns.size();
            if (config.isReuseValueWrapper()) {
                values = new ClickHouseValue[size];
                factory = () -> values;
            } else {
                factory = () -> new ClickHouseValue[size];
            }
        }

        void readNextRow() {
            int index = 0;
            int size = columns.size();
            values = factory.get();
            ClickHouseColumn column = null;
            try {
                MappedFunctions m = getMappedFunctions();
                for (; index < size; index++) {
                    column = columns.get(index);
                    values[index] = m.deserialize(column, values[index], input);
                }
            } catch (EOFException e) {
                if (index == 0) { // end of the stream, which is fine
                    values = null;
                } else {
                    throw new IllegalStateException(
                            ClickHouseUtils.format("Reached end of the stream when reading column #%d(total %d): %s",
                                    index + 1, size, column),
                            e);
                }
            } catch (IOException e) {
                throw new IllegalStateException(
                        ClickHouseUtils.format("Failed to read column #%d(total %d): %s", index + 1, size, column), e);
            }
        }

        @Override
        public boolean hasNext() {
            try {
                return input.available() > 0;
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public ClickHouseRecord next() {
            readNextRow();

            return new ClickHouseRecord() {
                @Override
                public int size() {
                    return values.length;
                }

                @Override
                public ClickHouseValue getValue(int index) throws IOException {
                    return values[index];
                }

                @Override
                public ClickHouseValue getValue(String columnName) throws IOException {
                    int index = 0;
                    for (ClickHouseColumn c : columns) {
                        if (c.getColumnName().equals(columnName)) {
                            getValue(index);
                        }
                        index++;
                    }

                    throw new IllegalArgumentException("Not able to find a column named: " + columnName);
                }
            };
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

    public ClickHouseRowBinaryProcessor(ClickHouseConfig config, InputStream input, OutputStream output,
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
