package com.clickhouse.data.format;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.clickhouse.config.ClickHouseRenameMethod;
import com.clickhouse.data.ClickHouseAggregateFunction;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseDeserializer;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseSerializer;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.value.ClickHouseBitmap;
import com.clickhouse.data.value.ClickHouseBitmapValue;

/**
 * Data processor for handling {@link ClickHouseFormat#RowBinary} and
 * {@link ClickHouseFormat#RowBinaryWithNamesAndTypes} two formats.
 */
@Deprecated
public class ClickHouseRowBinaryProcessor extends ClickHouseDataProcessor {
    public static class BitmapSerDe implements ClickHouseDeserializer, ClickHouseSerializer {
        private final ClickHouseDataType innerType;

        public BitmapSerDe(ClickHouseDataConfig config, ClickHouseColumn column) {
            this.innerType = column.getNestedColumns().get(0).getDataType();
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            return ref.update(ClickHouseBitmap.deserialize(input, innerType));
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            ClickHouseBitmapValue bitmapValue = (ClickHouseBitmapValue) value;
            output.write(bitmapValue.getValue().toBytes());
        }
    }

    public static class MapDeserializer extends ClickHouseDeserializer.CompositeDeserializer {
        private final ClickHouseValue keyValue;
        private final ClickHouseValue valValue;

        public MapDeserializer(ClickHouseDataConfig config, ClickHouseColumn column,
                ClickHouseDeserializer... deserializers) {
            super(deserializers);

            if (deserializers.length != 2) {
                throw new IllegalArgumentException("Expect 2 deserializers but got " + deserializers.length);
            }

            this.keyValue = column.getKeyInfo().newValue(config);
            this.valValue = column.getValueInfo().newValue(config);
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            int len = input.readVarInt();
            if (len == 0) {
                return ref.resetToNullOrEmpty();
            }

            Map<Object, Object> map = new LinkedHashMap<>(len * 4 / 3 + 1);
            ClickHouseDeserializer kd = deserializers[0];
            ClickHouseDeserializer vd = deserializers[1];
            for (int i = 0; i < len; i++) {
                map.put(kd.deserialize(keyValue, input).asObject(),
                        vd.deserialize(valValue, input).asObject());
            }
            return ref.update(map);
        }
    }

    public static class MapSerializer extends ClickHouseSerializer.CompositeSerializer {
        private final ClickHouseValue keyValue;
        private final ClickHouseValue valValue;

        public MapSerializer(ClickHouseDataConfig config, ClickHouseColumn column,
                ClickHouseSerializer... serializers) {
            super(serializers);

            if (serializers.length != 2) {
                throw new IllegalArgumentException("Expect 2 serializers but got " + serializers.length);
            }

            this.keyValue = column.getKeyInfo().newValue(config);
            this.valValue = column.getValueInfo().newValue(config);
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            Map<Object, Object> map = value.asMap();
            output.writeVarInt(map.size());
            if (!map.isEmpty()) {
                ClickHouseSerializer ks = serializers[0];
                ClickHouseSerializer vs = serializers[1];
                for (Entry<Object, Object> e : map.entrySet()) {
                    ks.serialize(keyValue.update(e.getKey()), output);
                    vs.serialize(valValue.update(e.getValue()), output);
                }
            }
        }
    }

    public static class NestedDeserializer extends ClickHouseDeserializer.CompositeDeserializer {
        protected final ClickHouseValue[] values;

        public NestedDeserializer(ClickHouseDataConfig config, ClickHouseColumn column,
                ClickHouseDeserializer... deserializers) {
            super(deserializers);

            List<ClickHouseColumn> nestedCols = column.getNestedColumns();
            int len = nestedCols.size();
            if (deserializers.length != len) {
                throw new IllegalArgumentException(
                        ClickHouseUtils.format("Expect %d deserializers but got %d", len, deserializers.length));
            }
            values = new ClickHouseValue[len];
            for (int i = 0; i < len; i++) {
                values[i] = nestedCols.get(i).newValue(config);
            }
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            int size = input.readVarInt();
            int len = values.length;
            Object[][] vals = new Object[size][];
            for (int i = 0; i < size; i++) {
                Object[] objs = new Object[len];
                for (int j = 0; j < len; j++) {
                    ClickHouseDeserializer d = deserializers[j];
                    objs[j] = d.deserialize(values[j], input).asObject();
                }
                vals[i] = objs;
            }
            // ClickHouseNestedValue.of(r, c.getNestedColumns(), values)
            return ref.update(vals);
        }
    }

    public static class NestedSerializer extends ClickHouseSerializer.CompositeSerializer {
        private final ClickHouseValue[] values;

        public NestedSerializer(ClickHouseDataConfig config, ClickHouseColumn column,
                ClickHouseSerializer... serializers) {
            super(serializers);

            List<ClickHouseColumn> nestedCols = column.getNestedColumns();
            int len = nestedCols.size();
            if (serializers.length != len) {
                throw new IllegalArgumentException(
                        ClickHouseUtils.format("Expect %d serializers but got %d", len, serializers.length));
            }
            values = new ClickHouseValue[len];
            for (int i = 0; i < len; i++) {
                values[i] = nestedCols.get(i).newValue(config);
            }
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            Object[][] vals = (Object[][]) value.asObject();
            int size = vals.length;
            int len = values.length;
            output.writeVarInt(size);
            for (int i = 0; i < size; i++) {
                Object[] objs = vals[i];
                for (int j = 0; j < len; j++) {
                    serializers[j].serialize(values[j].update(objs[j]), output);
                }
            }
        }
    }

    public static class TupleDeserializer extends ClickHouseDeserializer.CompositeDeserializer {
        private final ClickHouseValue[] values;

        public TupleDeserializer(ClickHouseDataConfig config, ClickHouseColumn column,
                ClickHouseDeserializer... deserializers) {
            super(deserializers);

            List<ClickHouseColumn> nestedCols = column.getNestedColumns();
            int len = nestedCols.size();
            if (deserializers.length != len) {
                throw new IllegalArgumentException(
                        ClickHouseUtils.format("Expect %d deserializers but got %d", len, deserializers.length));
            }
            values = new ClickHouseValue[len];
            for (int i = 0; i < len; i++) {
                values[i] = nestedCols.get(i).newValue(config);
            }
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            int len = values.length;
            Object[] tupleValues = new Object[len];
            for (int i = 0; i < len; i++) {
                tupleValues[i] = deserializers[i].deserialize(values[i], input).asObject();
            }
            return ref.update(tupleValues);
        }
    }

    public static class TupleSerializer extends ClickHouseSerializer.CompositeSerializer {
        private final ClickHouseValue[] values;

        public TupleSerializer(ClickHouseDataConfig config, ClickHouseColumn column,
                ClickHouseSerializer... serializers) {
            super(serializers);

            List<ClickHouseColumn> nestedCols = column.getNestedColumns();
            int len = nestedCols.size();
            if (serializers.length != len) {
                throw new IllegalArgumentException(
                        ClickHouseUtils.format("Expect %d serializers but got %d", len, serializers.length));
            }
            values = new ClickHouseValue[len];
            for (int i = 0; i < len; i++) {
                values[i] = nestedCols.get(i).newValue(config);
            }
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            List<Object> tupleValues = value.asTuple();
            for (int i = 0, len = serializers.length; i < len; i++) {
                serializers[i].serialize(values[i].update(tupleValues.get(i)), output);
            }
        }
    }

    public static class VariantDeserializer extends ClickHouseDeserializer.CompositeDeserializer {
        private final ClickHouseValue[] values;
        public VariantDeserializer(ClickHouseDataConfig config, ClickHouseColumn column,
                ClickHouseDeserializer... deserializers) {
            super(deserializers);

            List<ClickHouseColumn> nestedCols = column.getNestedColumns();
            int len = nestedCols.size();
            if (deserializers.length != len) {
                throw new IllegalArgumentException(
                        ClickHouseUtils.format("Expect %d deserializers but got %d", len, deserializers.length));
            }
            values = new ClickHouseValue[len];
            for (int i = 0; i < len; i++) {
                values[i] = nestedCols.get(i).newValue(config);
            }
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            int len = values.length;
            Object[] tupleValues = new Object[len];
            int ordTypeNum = BinaryStreamUtils.readInt8(input);
            for (int i = 0; i < len; i++) {
                if (ordTypeNum == i) {
                    tupleValues[i] = deserializers[i].deserialize(values[i], input).asObject();
                } else {
                    tupleValues[i] = null;
                }
            }
            return ref.update(tupleValues);
        }
    }

    public static class VariantSerializer extends ClickHouseSerializer.CompositeSerializer {
        private final ClickHouseValue[] values;

        public VariantSerializer(ClickHouseDataConfig config, ClickHouseColumn column,
                ClickHouseSerializer... serializers) {
            super(serializers);

            List<ClickHouseColumn> nestedCols = column.getNestedColumns();
            int len = nestedCols.size();
            if (serializers.length != len) {
                throw new IllegalArgumentException(
                        ClickHouseUtils.format("Expect %d serializers but got %d", len, serializers.length));
            }
            values = new ClickHouseValue[len];
            for (int i = 0; i < len; i++) {
                values[i] = nestedCols.get(i).newValue(config);
            }
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            List<Object> tupleValues = value.asTuple();
            // TODO: variant index
            for (int i = 0, len = serializers.length; i < len; i++) {
                serializers[i].serialize(values[i].update(tupleValues.get(i)), output);
            }
        }
    }

    @Override
    protected void readAndFill(ClickHouseRecord r) throws IOException {
        ClickHouseInputStream in = input;
        ClickHouseDeserializer[] tbl = serde.deserializers;

        for (int i = readPosition, len = serde.columns.length; i < len; i++) {
            tbl[i].deserialize(r.getValue(i), in);
            readPosition = i;
        }

        readPosition = 0;
    }

    @Override
    protected List<ClickHouseColumn> readColumns() throws IOException {
        if (input.available() < 1) {
            input.close();
            return Collections.emptyList();
        } else if (!config.getFormat().hasHeader()) {
            return Collections.emptyList();
        }

        int size = input.readVarInt();
        String[] names = new String[ClickHouseChecker.between(size, "size", 0, Integer.MAX_VALUE)];
        for (int i = 0; i < size; i++) {
            names[i] = input.readUnicodeString();
        }

        ClickHouseRenameMethod m = config.getColumnRenameMethod();
        List<ClickHouseColumn> columns = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            columns.add(ClickHouseColumn.of(m.rename(names[i]), input.readUnicodeString()));
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
    public ClickHouseRowBinaryProcessor(ClickHouseDataConfig config, ClickHouseInputStream input,
            ClickHouseOutputStream output, List<ClickHouseColumn> columns, Map<String, Serializable> settings)
            throws IOException {
        super(config, input, output, columns, settings);
    }

    protected ClickHouseDeserializer[] getArrayDeserializers(ClickHouseDataConfig config,
            List<ClickHouseColumn> columns) {
        ClickHouseDeserializer[] array = new ClickHouseDeserializer[columns.size()];
        ClickHouseDataConfig modifiedConfig = new UseObjectConfig(config);
        int i = 0;
        for (ClickHouseColumn column : columns) {
            array[i++] = getDeserializer(modifiedConfig,
                    ClickHouseColumn.of(column.getColumnName(), ClickHouseDataType.Array, false, column));
        }
        return array;
    }

    protected ClickHouseSerializer[] getArraySerializers(ClickHouseDataConfig config, List<ClickHouseColumn> columns) {
        ClickHouseSerializer[] array = new ClickHouseSerializer[columns.size()];
        ClickHouseDataConfig modifiedConfig = new UseObjectConfig(config);
        int i = 0;
        for (ClickHouseColumn column : columns) {
            array[i++] = getSerializer(modifiedConfig,
                    ClickHouseColumn.of(column.getColumnName(), ClickHouseDataType.Array, false, column));
        }
        return array;
    }

    @Override
    public ClickHouseDeserializer getDeserializer(ClickHouseDataConfig config, ClickHouseColumn column) {
        final ClickHouseDeserializer deserializer;
        switch (column.getDataType()) {
            case Bool:
                deserializer = BinaryDataProcessor::readBool;
                break;
            case Date:
                deserializer = BinaryDataProcessor.DateSerDe.of(config);
                break;
            case Date32:
                deserializer = BinaryDataProcessor.Date32SerDe.of(config);
                break;
            case DateTime:
                deserializer = column.getScale() > 0 ? BinaryDataProcessor.DateTime64SerDe.of(config, column)
                        : BinaryDataProcessor.DateTime32SerDe.of(config, column);
                break;
            case DateTime32:
                deserializer = BinaryDataProcessor.DateTime32SerDe.of(config, column);
                break;
            case DateTime64:
                deserializer = BinaryDataProcessor.DateTime64SerDe.of(config, column);
                break;
            case Enum8:
                deserializer = BinaryDataProcessor::readEnum8;
                break;
            case Enum16:
                deserializer = BinaryDataProcessor::readEnum16;
                break;
            case FixedString:
                deserializer = config.isUseBinaryString() ? new BinaryDataProcessor.FixedBytesSerDe(column)
                        : new BinaryDataProcessor.FixedStringSerDe(column);
                break;
            case Int8:
                deserializer = BinaryDataProcessor::readByte;
                break;
            case UInt8:
                deserializer = config.isWidenUnsignedTypes() ? BinaryDataProcessor::readUInt8AsShort
                        : BinaryDataProcessor::readByte;
                break;
            case Int16:
                deserializer = BinaryDataProcessor::readShort;
                break;
            case UInt16:
                deserializer = config.isWidenUnsignedTypes() ? BinaryDataProcessor::readUInt16AsInt
                        : BinaryDataProcessor::readShort;
                break;
            case Int32:
                deserializer = BinaryDataProcessor::readInteger;
                break;
            case UInt32:
                deserializer = config.isWidenUnsignedTypes() ? BinaryDataProcessor::readUInt32AsLong
                        : BinaryDataProcessor::readInteger;
                break;
            case Int64:
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
            case UInt64:
                deserializer = BinaryDataProcessor::readLong;
                break;
            case Int128:
                deserializer = BinaryDataProcessor::readInt128;
                break;
            case UInt128:
                deserializer = BinaryDataProcessor::readUInt128;
                break;
            case Int256:
                deserializer = BinaryDataProcessor::readInt256;
                break;
            case UInt256:
                deserializer = BinaryDataProcessor::readUInt256;
                break;
            case Decimal:
                deserializer = BinaryDataProcessor.DecimalSerDe.of(column);
                break;
            case Decimal32:
                deserializer = BinaryDataProcessor.Decimal32SerDe.of(column);
                break;
            case Decimal64:
                deserializer = BinaryDataProcessor.Decimal64SerDe.of(column);
                break;
            case Decimal128:
                deserializer = BinaryDataProcessor.Decimal128SerDe.of(column);
                break;
            case Decimal256:
                deserializer = BinaryDataProcessor.Decimal256SerDe.of(column);
                break;
            case Float32:
                deserializer = BinaryDataProcessor::readFloat;
                break;
            case Float64:
                deserializer = BinaryDataProcessor::readDouble;
                break;
            case IPv4:
                deserializer = BinaryDataProcessor::readIpv4;
                break;
            case IPv6:
                deserializer = BinaryDataProcessor::readIpv6;
                break;
            case UUID:
                deserializer = BinaryDataProcessor::readUuid;
                break;
            // Geo types
            case Point:
                deserializer = BinaryDataProcessor::readGeoPoint;
                break;
            case Ring:
                deserializer = BinaryDataProcessor::readGeoRing;
                break;
            case Polygon:
                deserializer = BinaryDataProcessor::readGeoPolygon;
                break;
            case MultiPolygon:
                deserializer = BinaryDataProcessor::readGeoMultiPolygon;
                break;
            // String
            case JSON:
            case Object:
            case String:
                deserializer = config.isUseBinaryString() ? BinaryDataProcessor::readBinaryString
                        : BinaryDataProcessor::readTextString;
                break;
            // nested
            case Array: {
                if (column.getArrayNestedLevel() == 1) {
                    ClickHouseColumn baseColumn = column.getArrayBaseColumn();
                    Class<?> javaClass = baseColumn.getObjectClassForArray(config);
                    if (!baseColumn.isNullable() && javaClass.isPrimitive()) {
                        int byteLength = baseColumn.getDataType().getByteLength();
                        if (byteLength == Byte.BYTES) { // Bool, *Int8
                            deserializer = BinaryDataProcessor::readByteArray;
                        } else if (byteLength == Short.BYTES) { // *Int16
                            deserializer = BinaryDataProcessor::readShortArray;
                        } else if (int.class == javaClass) { // Int32
                            deserializer = BinaryDataProcessor::readIntegerArray;
                        } else if (long.class == javaClass) { // UInt32, *Int64
                            deserializer = byteLength == Long.BYTES ? BinaryDataProcessor::readLongArray
                                    : BinaryDataProcessor::readIntegerArray;
                        } else if (float.class == javaClass) { // Float32
                            deserializer = BinaryDataProcessor::readFloatArray;
                        } else if (double.class == javaClass) { // Float64
                            deserializer = BinaryDataProcessor::readDoubleArray;
                        } else {
                            throw new IllegalArgumentException("Unsupported primitive type: " + javaClass);
                        }
                    } else {
                        deserializer = new BinaryDataProcessor.ArrayDeserializer(config, column, true,
                                getDeserializer(config, column.getNestedColumns().get(0)));
                    }
                } else {
                    deserializer = new BinaryDataProcessor.ArrayDeserializer(config, column, true,
                            getDeserializer(config, column.getNestedColumns().get(0)));
                }
                break;
            }
            case Map:
                deserializer = new MapDeserializer(config, column,
                        getDeserializers(config, column.getNestedColumns()));
                break;
            case Nested:
                deserializer = new NestedDeserializer(config, column,
                        getDeserializers(config, column.getNestedColumns()));
                break;
            case Tuple:
                deserializer = new TupleDeserializer(config, column,
                        getDeserializers(config, column.getNestedColumns()));
                break;
            // special
            case Nothing:
                deserializer = ClickHouseDeserializer.EMPTY_VALUE;
                break;
            case SimpleAggregateFunction:
                deserializer = getDeserializer(config, column.getNestedColumns().get(0));
                break;
            case AggregateFunction:
                if (column.getAggregateFunction() != ClickHouseAggregateFunction.groupBitmap) {
                    throw new IllegalArgumentException("Only groupMap is supported at this point");
                }
                deserializer = new BitmapSerDe(config, column)::deserialize;
                break;
            case Variant:
                deserializer = new VariantDeserializer(config, column,
                        getDeserializers(config, column.getNestedColumns()));
                break;
            default:
                throw new IllegalArgumentException("Unsupported column:" + column.toString());
        }

        return column.isNullable() ? new BinaryDataProcessor.NullableDeserializer(deserializer) : deserializer;
    }

    @Override
    public ClickHouseSerializer getSerializer(ClickHouseDataConfig config, ClickHouseColumn column) {
        final ClickHouseSerializer serializer;
        switch (column.getDataType()) {
            case Bool:
                serializer = BinaryDataProcessor::writeBool;
                break;
            case Date:
                serializer = BinaryDataProcessor.DateSerDe.of(config);
                break;
            case Date32:
                serializer = BinaryDataProcessor.Date32SerDe.of(config);
                break;
            case DateTime:
                serializer = column.getScale() > 0 ? BinaryDataProcessor.DateTime64SerDe.of(config, column)
                        : BinaryDataProcessor.DateTime32SerDe.of(config, column);
                break;
            case DateTime32:
                serializer = BinaryDataProcessor.DateTime32SerDe.of(config, column);
                break;
            case DateTime64:
                serializer = BinaryDataProcessor.DateTime64SerDe.of(config, column);
                break;
            case Enum8:
                serializer = BinaryDataProcessor::writeEnum8;
                break;
            case Enum16:
                serializer = BinaryDataProcessor::writeEnum16;
                break;
            case FixedString:
                serializer = config.isUseBinaryString() ? new BinaryDataProcessor.FixedBytesSerDe(column)
                        : new BinaryDataProcessor.FixedStringSerDe(column);
                break;
            case Int8:
            case UInt8:
                serializer = BinaryDataProcessor::writeByte;
                break;
            case Int16:
            case UInt16:
                serializer = BinaryDataProcessor::writeShort;
                break;
            case Int32:
            case UInt32:
                serializer = BinaryDataProcessor::writeInteger;
                break;
            case Int64:
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
            case UInt64:
                serializer = BinaryDataProcessor::writeLong;
                break;
            case Int128:
                serializer = BinaryDataProcessor::writeInt128;
                break;
            case UInt128:
                serializer = BinaryDataProcessor::writeUInt128;
                break;
            case Int256:
                serializer = BinaryDataProcessor::writeInt256;
                break;
            case UInt256:
                serializer = BinaryDataProcessor::writeUInt256;
                break;
            case Decimal:
                serializer = BinaryDataProcessor.DecimalSerDe.of(column);
                break;
            case Decimal32:
                serializer = BinaryDataProcessor.Decimal32SerDe.of(column);
                break;
            case Decimal64:
                serializer = new BinaryDataProcessor.Decimal64SerDe(column);
                break;
            case Decimal128:
                serializer = new BinaryDataProcessor.Decimal128SerDe(column);
                break;
            case Decimal256:
                serializer = new BinaryDataProcessor.Decimal256SerDe(column);
                break;
            case Float32:
                serializer = BinaryDataProcessor::writeFloat;
                break;
            case Float64:
                serializer = BinaryDataProcessor::writeDouble;
                break;
            case IPv4:
                serializer = BinaryDataProcessor::writeIpv4;
                break;
            case IPv6:
                serializer = BinaryDataProcessor::writeIpv6;
                break;
            case UUID:
                serializer = BinaryDataProcessor::writeUuid;
                break;
            // Geo types
            case Point:
                serializer = BinaryDataProcessor::writeGeoPoint;
                break;
            case Ring:
                serializer = BinaryDataProcessor::writeGeoRing;
                break;
            case Polygon:
                serializer = BinaryDataProcessor::writeGeoPolygon;
                break;
            case MultiPolygon:
                serializer = BinaryDataProcessor::writeGeoMultiPolygon;
                break;
            // String
            case JSON:
            case Object:
            case String:
                serializer = config.isUseBinaryString() ? BinaryDataProcessor::writeBinaryString
                        : BinaryDataProcessor::writeTextString;
                break;
            // nested
            case Array:
                serializer = new BinaryDataProcessor.ArraySerializer(config, column, true,
                        getSerializer(config, column.getNestedColumns().get(0)));
                break;
            case Map:
                serializer = new MapSerializer(config, column, getSerializers(config, column.getNestedColumns()));
                break;
            case Nested:
                serializer = new NestedSerializer(config, column, getSerializers(config, column.getNestedColumns()));
                break;
            case Tuple:
                serializer = new TupleSerializer(config, column, getSerializers(config, column.getNestedColumns()));
                break;
            // special
            case Nothing:
                serializer = ClickHouseSerializer.DO_NOTHING;
                break;
            case SimpleAggregateFunction:
                serializer = getSerializer(config, column.getNestedColumns().get(0));
                break;
            case AggregateFunction:
                if (column.getAggregateFunction() != ClickHouseAggregateFunction.groupBitmap) {
                    throw new IllegalArgumentException("Only groupMap is supported at this point");
                }
                serializer = new BitmapSerDe(config, column)::serialize;
                break;
            case Variant:
                serializer = new VariantSerializer(config, column, getSerializers(config, column.getNestedColumns()));
                break;
            default:
                throw new IllegalArgumentException("Unsupported column:" + column.toString());
        }

        return column.isNullable() ? new BinaryDataProcessor.NullableSerializer(serializer) : serializer;
    }
}
