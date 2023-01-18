package com.clickhouse.data.format;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.clickhouse.data.ClickHouseArraySequence;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseDeserializer;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseSerializer;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.value.ClickHouseByteValue;
import com.clickhouse.data.value.ClickHouseEmptyValue;
import com.clickhouse.data.value.ClickHouseIntegerValue;
import com.clickhouse.data.value.ClickHouseLongValue;
import com.clickhouse.data.value.ClickHouseShortValue;
import com.clickhouse.data.value.array.ClickHouseByteArrayValue;
import com.clickhouse.data.value.array.ClickHouseIntArrayValue;
import com.clickhouse.data.value.array.ClickHouseLongArrayValue;
import com.clickhouse.data.value.array.ClickHouseShortArrayValue;

public class ClickHouseNativeProcessor extends ClickHouseDataProcessor {
    private static final ClickHouseColumn[] lowCardinalityIndexColumns = new ClickHouseColumn[] {
            ClickHouseColumn.of("", ClickHouseDataType.UInt8, false),
            ClickHouseColumn.of("", ClickHouseDataType.UInt16, false),
            ClickHouseColumn.of("", ClickHouseDataType.UInt32, false),
            ClickHouseColumn.of("", ClickHouseDataType.UInt64, false)
    };
    private static final ClickHouseDeserializer[] lowCardinalityIndexDeserializers = new ClickHouseDeserializer[] {
            ClickHouseNativeProcessor::readByteArray, ClickHouseNativeProcessor::readShortArray,
            ClickHouseNativeProcessor::readIntegerArray, ClickHouseNativeProcessor::readLongArray
    };
    private static final ClickHouseValue[] lowCardinalityIndexValues = new ClickHouseValue[] {
            ClickHouseByteValue.ofUnsignedNull(), ClickHouseShortValue.ofUnsignedNull(),
            ClickHouseIntegerValue.ofUnsignedNull(), ClickHouseLongValue.ofUnsignedNull()
    };
    private static final ClickHouseArraySequence[] lowCardinalityIndexArrays = new ClickHouseArraySequence[] {
            ClickHouseByteArrayValue.ofUnsignedEmpty(), ClickHouseShortArrayValue.ofUnsignedEmpty(),
            ClickHouseIntArrayValue.ofUnsignedEmpty(), ClickHouseLongArrayValue.ofUnsignedEmpty()
    };

    static class ArrayColumnDeserializer extends ClickHouseDeserializer.CompositeDeserializer {
        private final long length;
        private final int nestedLevel;
        private final Class<?> valClass;
        private final ClickHouseValue valValue;

        public ArrayColumnDeserializer(ClickHouseDataConfig config, ClickHouseColumn column, long length,
                boolean nullable, ClickHouseDeserializer... deserializers) {
            super(deserializers);

            this.length = length;

            ClickHouseColumn baseColumn = column.getArrayBaseColumn();
            nestedLevel = column.getArrayNestedLevel();
            valClass = baseColumn.getObjectClassForArray(config);
            valValue = column.getNestedColumns().get(0).newValue(config);
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            final long l = this.length;
            final int len;
            if (l == -1L) {
                len = input.readVarInt();
            } else if (l < 0L) {
                len = (int) input.readBuffer(8).asLong();
            } else {
                len = (int) l;
            }

            if (len == 0) {
                return ref.resetToNullOrEmpty();
            }
            ClickHouseArraySequence arr = (ClickHouseArraySequence) ref;
            arr.allocate(len, valClass, nestedLevel);
            ClickHouseDeserializer d = deserializers[0];
            for (int i = 0; i < len; i++) {
                arr.setValue(i, d.deserialize(valValue, input));
            }
            return ref;
        }
    }

    static class LowCardinalityColumnDeserializer implements ClickHouseDeserializer {
        private final ClickHouseDeserializer deserializer;
        private final Class<?> javaClass;
        private final ClickHouseValue baseValue;
        private final ClickHouseArraySequence arrValue;
        private final boolean nullable;

        public LowCardinalityColumnDeserializer(ClickHouseDataConfig config, ClickHouseColumn column,
                ClickHouseDeserializer d) {
            this.javaClass = column.getObjectClassForArray(config);
            if (!column.isNullable() && javaClass.isPrimitive()) {
                int byteLength = column.getDataType().getByteLength();
                if (byteLength == Byte.BYTES) { // Bool, *Int8
                    deserializer = ClickHouseNativeProcessor::readByteArray;
                } else if (byteLength == Short.BYTES) { // *Int16
                    deserializer = ClickHouseNativeProcessor::readShortArray;
                } else if (int.class == javaClass) { // Int32
                    deserializer = ClickHouseNativeProcessor::readIntegerArray;
                } else if (long.class == javaClass) { // UInt32, *Int64
                    deserializer = byteLength == Long.BYTES ? ClickHouseNativeProcessor::readLongArray
                            : ClickHouseNativeProcessor::readIntegerArray;
                } else if (float.class == javaClass) { // Float32
                    deserializer = ClickHouseNativeProcessor::readFloatArray;
                } else if (double.class == javaClass) { // Float64
                    deserializer = ClickHouseNativeProcessor::readDoubleArray;
                } else {
                    throw new IllegalArgumentException("Unsupported primitive type: " + javaClass);
                }
            } else {
                deserializer = ClickHouseChecker.nonNull(d, "Deserializer");
            }
            this.baseValue = column.newValue(config);
            this.arrValue = column.newArrayValue(config);
            this.nullable = column.isNullable();
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            long version = input.readBuffer(8).asLong();
            if (version != 1L) {
                throw new IOException(
                        ClickHouseUtils.format("Unexpected low cardinality version %d", version));
            }
            ClickHouseArraySequence value = (ClickHouseArraySequence) ref;
            long serializationType = input.readBuffer(8).asLong();
            // Lowest byte contains info about key type
            int keyType = (int) (serializationType & 0xFL);
            if (keyType < 0 || keyType >= lowCardinalityIndexColumns.length) {
                throw new IOException("Unsupported key type: " + keyType);
            }
            // int indexSize = (int) input.readBuffer(8).asLong();
            // ClickHouseValue v = baseColumn.newValue(config);
            ClickHouseArraySequence dict = (ClickHouseArraySequence) deserializer.deserialize(arrValue.copy(), input);
            if (nullable) {
                dict.setValue(0, ClickHouseEmptyValue.INSTANCE);
            }

            // ClickHouseColumn indexCol = lowCardinalityIndexColumns[keyType];
            ClickHouseValue indexVal = lowCardinalityIndexValues[keyType].copy();
            ClickHouseArraySequence indexArrVal = lowCardinalityIndexArrays[keyType].copy();
            // int rows = (int) BinaryStreamUtils.readInt64(input);
            lowCardinalityIndexDeserializers[keyType].deserialize(indexArrVal, input);

            int rows = indexArrVal.length();
            value.allocate(rows, javaClass);
            for (int i = 0; i < rows; i++) {
                int keyIndex = indexArrVal.getValue(i, indexVal).asInteger();
                value.setValue(i, dict.getValue(keyIndex, baseValue));
            }
            return value;
        }
    }

    static class LowCardinalitySerializer implements ClickHouseSerializer {
        private final ClickHouseSerializer serializer;

        public LowCardinalitySerializer(ClickHouseSerializer serializer) {
            this.serializer = ClickHouseChecker.nonNull(serializer, "Serializer");
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            if (value.isNullOrEmpty()) {
                output.writeBoolean(true);
            } else {
                serializer.serialize(value, output.writeBoolean(false));
            }
        }
    }

    static class NullableColumnDeserializer implements ClickHouseDeserializer {
        private final ClickHouseDeserializer deserializer;

        public NullableColumnDeserializer(ClickHouseDeserializer deserializer) {
            this.deserializer = ClickHouseChecker.nonNull(deserializer, "Deserializer");
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            return ref;
        }
    }

    // public static class MappedFunctions {
    // private static final MappedFunctions instance = new MappedFunctions();

    // private ClickHouseArraySequence readFlattenArray(ClickHouseArraySequence ref,
    // ClickHouseConfig config,
    // ClickHouseColumn column, ClickHouseInputStream input) throws IOException {
    // if (ref == null) {
    // ref = column.newArrayValue(config);
    // }
    // ClickHouseColumn baseColumn = column.getArrayBaseColumn();
    // int level = column.getArrayNestedLevel();
    // if (level <= 1) {
    // int length = (int) BinaryStreamUtils.readInt64(input);
    // if (baseColumn.isNullable()) {
    // boolean[] nullFlags = input.readBuffer(length).asBooleanArray();
    // ClickHouseDeserializer func = getDeserializer(baseColumn.getDataType());
    // if (func == null) {
    // throw new IllegalArgumentException(ERROR_UNKNOWN_DATA_TYPE +
    // baseColumn.getDataType().name());
    // }
    // ref.allocate(length, baseColumn.getObjectClass(config), level);
    // ClickHouseValue v = baseColumn.newValue(config);
    // for (int i = 0; i < length; i++) {
    // func.deserialize(v, config, baseColumn, input);
    // if (nullFlags[i]) {
    // v.resetToNullOrEmpty();
    // }
    // ref.setValue(i, v);
    // }
    // } else {
    // readArrayContent((ClickHouseArraySequence) ref, config,
    // column.getNestedColumns().get(0),
    // column.getArrayBaseColumn(), input, length, level);
    // }
    // } else {
    // int length = 0;
    // int[][] offsets = new int[level][];
    // for (int i = 1; i < level; i++) {
    // int len = (int) BinaryStreamUtils.readInt64(input);
    // int[] idx = new int[len];
    // offsets[i - 1] = idx;
    // for (int j = 0; j < len; j++) {
    // idx[j] = (int) BinaryStreamUtils.readInt64(input);
    // }
    // }

    // ClickHouseArraySequence parent = ref;
    // ClickHouseColumn nested = column;
    // for (int i = 0; i < level; i++) {
    // int[] idx = offsets[i];
    // int len = idx.length;
    // nested = column.getNestedColumns().get(0);
    // ClickHouseArraySequence arr = nested.newArrayValue(config);
    // for (int j = 0; j < len; j++) {

    // }
    // }
    // ClickHouseArraySequence arr = ClickHouseColumn.of("",
    // ClickHouseDataType.Array, false, baseColumn)
    // .newArrayValue(config);
    // // readArray(innerMost, config, baseColumn, baseColumn, input, length, 1);
    // }
    // return ref;
    // }

    // private ClickHouseArraySequence readLowCardinality(ClickHouseArraySequence
    // value, ClickHouseConfig config,
    // ClickHouseColumn arrayColumn, ClickHouseColumn baseColumn,
    // ClickHouseInputStream input)
    // throws IOException {
    // long version = BinaryStreamUtils.readInt64(input);
    // if (version != 1L) {
    // throw new IOException(
    // ClickHouseUtils.format("Unexpected low cardinality version %d when reading
    // column %s",
    // version, baseColumn.getOriginalTypeName()));
    // }
    // long serializationType = BinaryStreamUtils.readInt64(input);
    // // Lowest byte contains info about key type
    // int keyType = (int) (serializationType & 0xFL);
    // if (keyType < 0 || keyType >= lowCardinalityIndexColumns.length) {
    // throw new IOException("Unsupported key type: " + keyType);
    // }
    // int indexSize = (int) BinaryStreamUtils.readInt64(input);
    // ClickHouseValue v = baseColumn.newValue(config);
    // ClickHouseArraySequence dict = readArrayContent(value.copy(), config,
    // baseColumn,
    // arrayColumn.getArrayBaseColumn(), input, indexSize,
    // arrayColumn.getArrayNestedLevel());
    // if (baseColumn.isNullable()) {
    // dict.setValue(0, ClickHouseEmptyValue.INSTANCE);
    // }

    // ClickHouseColumn indexCol = lowCardinalityIndexColumns[keyType];
    // ClickHouseValue indexVal = lowCardinalityIndexValues[keyType].copy();
    // ClickHouseArraySequence indexArrVal =
    // lowCardinalityIndexArrays[keyType].copy();
    // int rows = (int) BinaryStreamUtils.readInt64(input);
    // readArrayContent(indexArrVal, config, indexCol, indexCol, input, rows, 1);

    // value.allocate(rows, baseColumn.getObjectClassForArray(config));
    // for (int i = 0; i < rows; i++) {
    // int keyIndex = indexArrVal.getValue(i, indexVal).asInteger();
    // value.setValue(i, dict.getValue(keyIndex, v));
    // }
    // return value;
    // }

    // private ClickHouseArraySequence readColumn(ClickHouseArraySequence value,
    // ClickHouseConfig config,
    // ClickHouseColumn arrayColumn, ClickHouseColumn baseColumn,
    // ClickHouseInputStream input, int rows)
    // throws IOException {
    // if (baseColumn.isLowCardinality()) {
    // return readLowCardinality(value, config, arrayColumn, baseColumn, input);
    // }

    // return readArrayContent(value, config, baseColumn,
    // arrayColumn.getArrayBaseColumn(), input, rows,
    // arrayColumn.getArrayNestedLevel());
    // }

    // protected MappedFunctions() {
    // super();
    // }

    // @Override
    // public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream
    // input) throws IOException {
    // if (column.isLowCardinality()) {
    // if (ref == null) {
    // ref = column.newValue(config);
    // }
    // column.disableLowCardinality();
    // ClickHouseColumn arrCol = ClickHouseColumn.of("", ClickHouseDataType.Array,
    // false, column);
    // ClickHouseArraySequence arrVal =
    // readLowCardinality(arrCol.newArrayValue(config), config, arrCol,
    // column, input);
    // return arrVal.getValue(0, ref);
    // } else if (column.isArray()) {
    // return readFlattenArray((ClickHouseArraySequence) ref, config, column,
    // input);
    // } else {
    // if (!column.isLowCardinalityDisabled() && column.isNullable() &&
    // BinaryStreamUtils.readNull(input)) {
    // return ref == null ? column.newValue(config) : ref.resetToNullOrEmpty();
    // } else if (config.isWidenUnsignedTypes()) {
    // switch (column.getDataType()) {
    // case UInt8:
    // return ClickHouseShortValue.of(ref, input.readUnsignedByte(), false);
    // case UInt16:
    // return ClickHouseIntegerValue.of(ref, input.readBuffer(2).asUnsignedShort(),
    // false);
    // case UInt32:
    // return ClickHouseLongValue.of(ref, input.readBuffer(4).asUnsignedInteger(),
    // false);
    // default:
    // break;
    // }
    // }
    // }

    // ClickHouseDeserializer func = getDeserializer(column.getDataType());
    // if (func == null) {
    // throw new IllegalArgumentException(ERROR_UNKNOWN_DATA_TYPE +
    // column.getDataType().name());
    // }
    // return func.deserialize(ref, config, column, input);
    // }

    // @Override
    // public void serialize(ClickHouseValue value, ClickHouseOutputStream output)
    // throws IOException {
    // if (column.isLowCardinality()) {
    // column.disableLowCardinality();
    // ClickHouseColumn arrCol = ClickHouseColumn.of("", ClickHouseDataType.Array,
    // false, column);
    // // ClickHouseArraySequence arrVal =
    // // readLowCardinality(arrCol.newArrayValue(config), config, arrCol,
    // // column, input);
    // } else if (column.isArray()) {
    // // write flatten array
    // } else {
    // if (!column.isLowCardinalityDisabled() && column.isNullable()) { // always
    // false for geo types, and
    // // Array, Nested, Map and Tuple etc.
    // if (value.isNullOrEmpty()) {
    // BinaryStreamUtils.writeNull(output);
    // return;
    // } else {
    // BinaryStreamUtils.writeNonNull(output);
    // }
    // }

    // ClickHouseSerializer func = getSerializer(column.getDataType());
    // if (func == null) {
    // throw new IllegalArgumentException(ERROR_UNKNOWN_DATA_TYPE +
    // column.getDataType().name());
    // }
    // func.serialize(value, config, column, output);
    // }
    // }
    // }

    private static int readArrayLength(ClickHouseInputStream input) throws IOException {
        return (int) input.readBuffer(8).asLong();
    }

    private static ClickHouseValue readByteArray(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(readArrayLength(input)).compact().array());
    }

    private static ClickHouseValue readShortArray(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(readArrayLength(input) * 2).asShortArray());
    }

    private static ClickHouseValue readIntegerArray(ClickHouseValue ref, ClickHouseInputStream input)
            throws IOException {
        return ref.update(input.readBuffer(readArrayLength(input) * 4).asIntegerArray());
    }

    private static ClickHouseValue readLongArray(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(readArrayLength(input) * 8).asLongArray());
    }

    private static ClickHouseValue readFloatArray(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
        return ref.update(input.readBuffer(readArrayLength(input) * 4).asFloatArray());
    }

    private static ClickHouseValue readDoubleArray(ClickHouseValue ref, ClickHouseInputStream input)
            throws IOException {
        return ref.update(input.readBuffer(readArrayLength(input) * 8).asDoubleArray());
    }

    private static ClickHouseValue readFixedByteArray(ClickHouseValue ref, ClickHouseInputStream input)
            throws IOException {
        ClickHouseArraySequence arr = (ClickHouseArraySequence) ref;
        // input.readFully((byte[]) arr.asRawObject());
        return arr;
    }

    private static ClickHouseValue readFixedShortArray(ClickHouseValue ref, ClickHouseInputStream input)
            throws IOException {
        ClickHouseArraySequence arr = (ClickHouseArraySequence) ref;
        return ref.update(input.readBuffer(arr.length() * 2).asShortArray());
    }

    private static ClickHouseValue readFixedIntegerArray(ClickHouseValue ref, ClickHouseInputStream input)
            throws IOException {
        ClickHouseArraySequence arr = (ClickHouseArraySequence) ref;
        return ref.update(input.readBuffer(arr.length() * 4).asIntegerArray());
    }

    private static ClickHouseValue readFixedLongArray(ClickHouseValue ref, ClickHouseInputStream input)
            throws IOException {
        ClickHouseArraySequence arr = (ClickHouseArraySequence) ref;
        return ref.update(input.readBuffer(arr.length() * 8).asLongArray());
    }

    private static ClickHouseValue readFixedFloatArray(ClickHouseValue ref, ClickHouseInputStream input)
            throws IOException {
        ClickHouseArraySequence arr = (ClickHouseArraySequence) ref;
        return ref.update(input.readBuffer(arr.length() * 4).asFloatArray());
    }

    private static ClickHouseValue readFixedDoubleArray(ClickHouseValue ref, ClickHouseInputStream input)
            throws IOException {
        ClickHouseArraySequence arr = (ClickHouseArraySequence) ref;
        return ref.update(input.readBuffer(arr.length() * 8).asDoubleArray());
    }

    private ClickHouseDeserializer[] colDeserializers;
    private ClickHouseArraySequence[] currentBlock;

    private int blockRows;
    private int currentRow;

    private void readBlock() throws IOException {
        int colCount = input.readVarInt();
        int rowCount = input.readVarInt();

        if (colDeserializers == null) {
            colDeserializers = new ClickHouseDeserializer[colCount];
            currentBlock = new ClickHouseArraySequence[colCount];
        }
        for (int i = 0; i < colCount; i++) {
            // String name = input.readUnicodeString();
            // String type = input.readUnicodeString();
            input.skip(input.readVarInt());
            input.skip(input.readVarInt());

            ClickHouseColumn col = columns[i];
            ClickHouseDeserializer d = colDeserializers[i];
            ClickHouseArraySequence arr;
            if (d == null) {
                ClickHouseColumn arrCol = ClickHouseColumn.of("", ClickHouseDataType.Array, false, col);
                colDeserializers[i] = d = col.isLowCardinality()
                        ? new LowCardinalityColumnDeserializer(config, col, getArrayDeserializer(config, arrCol, -2L))
                        : getArrayDeserializer(config, arrCol, rowCount);
                currentBlock[i] = arr = col.newArrayValue(config);
            } else {
                arr = currentBlock[i];
            }
            arr.allocate(rowCount, col.getObjectClass(config));
            d.deserialize(arr, input);
        }
        blockRows = rowCount;
        currentRow = 0;

        readPosition = 0;
    }

    @Override
    protected ClickHouseRecord createRecord() {
        return new ClickHouseSimpleRecord(getColumns(), templates);
    }

    @Override
    protected boolean hasMoreToRead() throws UncheckedIOException {
        if (currentRow < blockRows) {
            return true;
        }

        try {
            if (input.available() <= 0) {
                input.close();
                return false;
            } else {
                readBlock();
            }
            return true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void readAndFill(ClickHouseRecord r) throws IOException {
        for (int i = readPosition, len = columns.length; i < len; i++) {
            currentBlock[readPosition].getValue(currentRow, r.getValue(readPosition));
            readPosition = i;
        }

        readPosition = 0;
        currentRow++;
    }

    @Override
    protected void readAndFill(ClickHouseValue value) throws IOException {
        if (currentRow >= blockRows) {
            readBlock();
        }

        currentBlock[readPosition++].getValue(currentRow, value);
        if (readPosition >= columns.length) {
            readPosition = 0;
            currentRow++;
        }
    }

    @Override
    protected List<ClickHouseColumn> readColumns() throws IOException {
        if (input.available() <= 0) {
            input.close();
            // no result returned
            colDeserializers = new ClickHouseDeserializer[0];
            currentBlock = new ClickHouseArraySequence[0];
            return Collections.emptyList();
        }

        int colCount = input.readVarInt();
        int rowCount = input.readVarInt();
        List<ClickHouseColumn> columns = new ArrayList<>(colCount);
        colDeserializers = new ClickHouseDeserializer[colCount];
        ClickHouseArraySequence[] arrays = new ClickHouseArraySequence[colCount];

        for (int i = 0; i < colCount; i++) {
            String name = input.readUnicodeString();
            ClickHouseColumn baseColumn = ClickHouseColumn.of(name, input.readUnicodeString());
            ClickHouseColumn arraryColumn = ClickHouseColumn.of("", ClickHouseDataType.Array, false, baseColumn);
            ClickHouseArraySequence value = arraryColumn.newArrayValue(config);
            value.allocate(rowCount);

            columns.add(baseColumn);
            ClickHouseDeserializer d = baseColumn.isLowCardinality()
                    ? new LowCardinalityColumnDeserializer(config, baseColumn,
                            getArrayDeserializer(config, arraryColumn, -2L))
                    : getArrayDeserializer(config, arraryColumn, rowCount);
            colDeserializers[i] = d;
            value.allocate(rowCount, baseColumn.getObjectClass(config));
            arrays[i] = (ClickHouseArraySequence) d.deserialize(value, input);
        }
        currentBlock = arrays;
        blockRows = rowCount;
        currentRow = 0;
        return columns;
    }

    public ClickHouseNativeProcessor(ClickHouseDataConfig config, ClickHouseInputStream input,
            ClickHouseOutputStream output, List<ClickHouseColumn> columns, Map<String, Serializable> settings)
            throws IOException {
        super(config, input, output, columns, settings);
    }

    private ClickHouseDeserializer getArrayDeserializer(ClickHouseDataConfig config, ClickHouseColumn column,
            long length) {
        final boolean fixedLength = length >= 0L;
        final ClickHouseDeserializer deserializer;
        ClickHouseColumn baseColumn = column.getArrayBaseColumn();
        Class<?> javaClass = baseColumn.getObjectClassForArray(config);
        if (column.getArrayNestedLevel() == 1 && !baseColumn.isNullable() && javaClass.isPrimitive()) {
            int byteLength = baseColumn.getDataType().getByteLength();
            if (byteLength == Byte.BYTES) { // Bool, *Int8
                deserializer = fixedLength ? ClickHouseNativeProcessor::readFixedByteArray
                        : ClickHouseNativeProcessor::readByteArray;
            } else if (byteLength == Short.BYTES) { // *Int16
                deserializer = fixedLength ? ClickHouseNativeProcessor::readFixedShortArray
                        : ClickHouseNativeProcessor::readShortArray;
            } else if (int.class == javaClass) { // Int32
                deserializer = fixedLength ? ClickHouseNativeProcessor::readFixedIntegerArray
                        : ClickHouseNativeProcessor::readIntegerArray;
            } else if (long.class == javaClass) { // UInt32, *Int64
                if (byteLength == Long.BYTES) {
                    deserializer = fixedLength ? ClickHouseNativeProcessor::readFixedLongArray
                            : ClickHouseNativeProcessor::readLongArray;
                } else {
                    deserializer = fixedLength ? ClickHouseNativeProcessor::readFixedIntegerArray
                            : ClickHouseNativeProcessor::readIntegerArray;
                }
            } else if (float.class == javaClass) { // Float32
                deserializer = fixedLength ? ClickHouseNativeProcessor::readFixedFloatArray
                        : ClickHouseNativeProcessor::readFloatArray;
            } else if (double.class == javaClass) { // Float64
                deserializer = fixedLength ? ClickHouseNativeProcessor::readFixedDoubleArray
                        : ClickHouseNativeProcessor::readDoubleArray;
            } else {
                throw new IllegalArgumentException("Unsupported primitive type: " + javaClass);
            }
        } else {
            ClickHouseColumn nestedCol = column.getNestedColumns().get(0);
            deserializer = new BinaryDataProcessor.ArrayDeserializer(config, column, length,
                    getDeserializer(config, nestedCol));
        }
        return deserializer;
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
                deserializer = BinaryDataProcessor::readEnum8;
                break;
            case FixedString:
                deserializer = new BinaryDataProcessor.FixedStringSerDe(column);
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
            case Array:
                deserializer = getArrayDeserializer(config, column, -1L);
                break;
            case Map:
                deserializer = new ClickHouseRowBinaryProcessor.MapDeserializer(config, column,
                        getDeserializers(config, column.getNestedColumns()));
                break;
            case Nested:
                deserializer = ClickHouseDeserializer.EMPTY_VALUE;
                // new ClickHouseRowBinaryProcessor.NestedDeserializer(config, column,
                // getArraySerDeializers(config, column.getNestedColumns()));
                break;
            case Tuple:
                deserializer = new ClickHouseRowBinaryProcessor.TupleDeserializer(config, column,
                        getDeserializers(config, column.getNestedColumns()));
                break;
            // special
            case Nothing:
                deserializer = ClickHouseDeserializer.EMPTY_VALUE;
                break;
            case SimpleAggregateFunction:
                deserializer = getDeserializer(config, column.getNestedColumns().get(0));
                break;
            default:
                throw new IllegalArgumentException("Unsupported column:" + column.toString());
        }

        return deserializer;
        // return !column.isLowCardinality() && column.isNullable()
        // ? new BinaryDataProcessor.NullableDeserializer(deserializer)
        // : deserializer;
    }

    @Override
    public ClickHouseSerializer getSerializer(ClickHouseDataConfig config, ClickHouseColumn column) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void write(ClickHouseValue value) throws IOException {
        // TODO Auto-generated method stub

    }
}
