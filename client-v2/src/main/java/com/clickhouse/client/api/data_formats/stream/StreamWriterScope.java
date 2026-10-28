package com.clickhouse.client.api.data_formats.stream;

import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import com.clickhouse.data.ClickHouseAggregateFunction;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.data.value.ClickHouseBitmap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Scope implementation shared by a row and every nested container.
 */
final class StreamWriterScope implements ValueWriter {

    private final RowBinaryStreamWriter owner;

    private final StreamWriterScope parent;

    private final SlotCursor cursor;

    private final OutputStream out;

    private final ByteArrayOutputStream buffer;

    /** JSON column whose typed paths apply, when this scope is a JSON object. */
    private ClickHouseColumn jsonType;

    /** Dotted prefix contributed by enclosing {@code beginObject} calls. */
    private String pathPrefix = "";

    /** Nested object whose leaf paths belong to the parent JSON value. */
    private boolean flattensIntoParent;

    private StreamWriterScope child;

    private boolean closed;

    private static final ClickHouseColumn DYN_STRING = ClickHouseColumn.of("v", "String");

    private static final ClickHouseColumn DYN_INT32 = ClickHouseColumn.of("v", "Int32");

    private static final ClickHouseColumn DYN_INT64 = ClickHouseColumn.of("v", "Int64");

    private static final ClickHouseColumn DYN_FLOAT64 = ClickHouseColumn.of("v", "Float64");

    private static final ClickHouseColumn DYN_BOOL = ClickHouseColumn.of("v", "Bool");

    private static final ClickHouseColumn DYN_JSON = ClickHouseColumn.of("v", "JSON");

    private StreamWriterScope(RowBinaryStreamWriter owner, StreamWriterScope parent, SlotCursor cursor,
                              OutputStream out, ByteArrayOutputStream buffer) {
        this.owner = owner;
        this.parent = parent;
        this.cursor = cursor;
        this.out = out;
        this.buffer = buffer;
    }

    static StreamWriterScope row(RowBinaryStreamWriter owner, List<ClickHouseColumn> columns) {
        return new StreamWriterScope(owner, null, SlotCursor.row(columns), owner.out(), null);
    }

    boolean isClosed() {
        return closed;
    }

    @Override
    public void writeInt8(byte value) throws IOException {
        prepare(ClickHouseDataType.Int8);
        body(() -> BinaryStreamUtils.writeInt8(out, value));
    }

    @Override
    public void writeInt8(int value) throws IOException {
        checkRange(value, Byte.MIN_VALUE, Byte.MAX_VALUE, "Int8");
        writeInt8((byte) value);
    }

    @Override
    public void writeInt16(short value) throws IOException {
        prepare(ClickHouseDataType.Int16);
        body(() -> BinaryStreamUtils.writeInt16(out, value));
    }

    @Override
    public void writeInt16(int value) throws IOException {
        checkRange(value, Short.MIN_VALUE, Short.MAX_VALUE, "Int16");
        writeInt16((short) value);
    }

    @Override
    public void writeInt32(int value) throws IOException {
        prepare(ClickHouseDataType.Int32);
        body(() -> BinaryStreamUtils.writeInt32(out, value));
    }

    @Override
    public void writeInt32(long value) throws IOException {
        if (value != (int) value) {
            owner.fail();
            throw new IllegalArgumentException("Int32 overflow: " + value);
        }
        writeInt32((int) value);
    }

    @Override
    public void writeInt64(long value) throws IOException {
        prepare(ClickHouseDataType.Int64);
        body(() -> BinaryStreamUtils.writeInt64(out, value));
    }

    @Override
    public void writeInt128(BigInteger value) throws IOException {
        requireValue(value);
        prepare(ClickHouseDataType.Int128);
        body(() -> BinaryStreamUtils.writeInt128(out, value));
    }

    @Override
    public void writeInt256(BigInteger value) throws IOException {
        requireValue(value);
        prepare(ClickHouseDataType.Int256);
        body(() -> BinaryStreamUtils.writeInt256(out, value));
    }

    @Override
    public void writeUInt8(int value) throws IOException {
        checkRange(value, 0, 255, "UInt8");
        prepare(ClickHouseDataType.UInt8);
        body(() -> BinaryStreamUtils.writeUnsignedInt8(out, value));
    }

    @Override
    public void writeUInt16(int value) throws IOException {
        checkRange(value, 0, 65535, "UInt16");
        prepare(ClickHouseDataType.UInt16);
        body(() -> BinaryStreamUtils.writeUnsignedInt16(out, value));
    }

    @Override
    public void writeUInt32(long value) throws IOException {
        if (value < 0 || value > 0xFFFFFFFFL) {
            owner.fail();
            throw new IllegalArgumentException("UInt32 overflow: " + value);
        }
        prepare(ClickHouseDataType.UInt32);
        body(() -> BinaryStreamUtils.writeUnsignedInt32(out, value));
    }

    @Override
    public void writeUInt64(long value) throws IOException {
        prepare(ClickHouseDataType.UInt64);
        body(() -> BinaryStreamUtils.writeUnsignedInt64(out, value));
    }

    @Override
    public void writeUInt64(BigInteger value) throws IOException {
        requireValue(value);
        prepare(ClickHouseDataType.UInt64);
        body(() -> BinaryStreamUtils.writeUnsignedInt64(out, value));
    }

    @Override
    public void writeUInt128(BigInteger value) throws IOException {
        requireValue(value);
        prepare(ClickHouseDataType.UInt128);
        body(() -> BinaryStreamUtils.writeUnsignedInt128(out, value));
    }

    @Override
    public void writeUInt256(BigInteger value) throws IOException {
        requireValue(value);
        prepare(ClickHouseDataType.UInt256);
        body(() -> BinaryStreamUtils.writeUnsignedInt256(out, value));
    }

    @Override
    public void writeFloat32(float value) throws IOException {
        prepare(ClickHouseDataType.Float32);
        body(() -> BinaryStreamUtils.writeFloat32(out, value));
    }

    @Override
    public void writeFloat64(double value) throws IOException {
        prepare(ClickHouseDataType.Float64);
        body(() -> BinaryStreamUtils.writeFloat64(out, value));
    }

    @Override
    public void writeBFloat16(float value) throws IOException {
        prepare(ClickHouseDataType.BFloat16);
        body(() -> BinaryStreamUtils.writeBFloat16(out, value));
    }

    @Override
    public void writeBool(boolean value) throws IOException {
        prepare(ClickHouseDataType.Bool);
        body(() -> BinaryStreamUtils.writeBoolean(out, value));
    }

    @Override
    public void writeString(String value) throws IOException {
        if (value == null) {
            writeNullFor(ClickHouseDataType.String, ClickHouseDataType.JSON);
            return;
        }
        prepare(ClickHouseDataType.String, ClickHouseDataType.JSON);
        body(() -> BinaryStreamUtils.writeString(out, value));
    }

    @Override
    public void writeString(byte[] value) throws IOException {
        if (value == null) {
            writeNullFor(ClickHouseDataType.String, ClickHouseDataType.JSON);
            return;
        }
        prepare(ClickHouseDataType.String, ClickHouseDataType.JSON);
        body(() -> BinaryStreamUtils.writeString(out, value));
    }

    @Override
    public void writeString(ByteBuffer value) throws IOException {
        if (value == null) {
            writeString((byte[]) null);
            return;
        }
        ByteBuffer view = value.duplicate();
        int length = view.remaining();
        prepare(ClickHouseDataType.String, ClickHouseDataType.JSON);
        body(() -> {
            SerializerUtils.writeVarInt(out, length);
            if (view.hasArray()) {
                out.write(view.array(), view.arrayOffset() + view.position(), length);
            } else {
                byte[] copy = new byte[length];
                view.get(copy);
                out.write(copy);
            }
        });
    }

    @Override
    public void writeFixedString(String value) throws IOException {
        if (value == null) {
            writeNullFor(ClickHouseDataType.FixedString);
            return;
        }
        ClickHouseColumn column = prepare(ClickHouseDataType.FixedString);
        int length = column.getPrecision();
        body(() -> BinaryStreamUtils.writeFixedString(out, value, length));
    }

    @Override
    public void writeFixedString(byte[] value) throws IOException {
        if (value == null) {
            writeNullFor(ClickHouseDataType.FixedString);
            return;
        }
        ClickHouseColumn column = prepare(ClickHouseDataType.FixedString);
        int length = column.getPrecision();
        body(() -> SerializerUtils.writeFixedStringBytes(out, value, length));
    }

    @Override
    public void writeDate(LocalDate value) throws IOException {
        requireValue(value);
        prepare(ClickHouseDataType.Date);
        body(() -> SerializerUtils.writeDate(out, value, ZoneOffset.UTC));
    }

    @Override
    public void writeDate32(LocalDate value) throws IOException {
        requireValue(value);
        prepare(ClickHouseDataType.Date32);
        body(() -> SerializerUtils.writeDate32(out, value, ZoneOffset.UTC));
    }

    @Override
    public void writeDateTime(LocalDateTime value) throws IOException {
        requireValue(value);
        ClickHouseColumn column = prepare(ClickHouseDataType.DateTime, ClickHouseDataType.DateTime32);
        ZoneId zone = zone(column);
        body(() -> SerializerUtils.writeDateTime(out, value, zone));
    }

    @Override
    public void writeDateTime(ZonedDateTime value) throws IOException {
        requireValue(value);
        ClickHouseColumn column = prepare(ClickHouseDataType.DateTime, ClickHouseDataType.DateTime32);
        ZoneId zone = zone(column);
        body(() -> SerializerUtils.writeDateTime(out, value, zone));
    }

    @Override
    public void writeDateTime(Instant value) throws IOException {
        requireValue(value);
        ClickHouseColumn column = prepare(ClickHouseDataType.DateTime, ClickHouseDataType.DateTime32);
        ZoneId zone = zone(column);
        body(() -> SerializerUtils.writeDateTime(out, value, zone));
    }

    @Override
    public void writeDateTime64(LocalDateTime value) throws IOException {
        requireValue(value);
        ClickHouseColumn column = prepare(ClickHouseDataType.DateTime64);
        int scale = column.getScale();
        ZoneId zone = zone(column);
        body(() -> SerializerUtils.writeDateTime64(out, value, scale, zone));
    }

    @Override
    public void writeDateTime64(ZonedDateTime value) throws IOException {
        requireValue(value);
        ClickHouseColumn column = prepare(ClickHouseDataType.DateTime64);
        int scale = column.getScale();
        ZoneId zone = zone(column);
        body(() -> SerializerUtils.writeDateTime64(out, value, scale, zone));
    }

    @Override
    public void writeUUID(UUID value) throws IOException {
        requireValue(value);
        prepare(ClickHouseDataType.UUID);
        body(() -> BinaryStreamUtils.writeUuid(out, value));
    }

    @Override
    public void writeIPv4(Inet4Address value) throws IOException {
        requireValue(value);
        prepare(ClickHouseDataType.IPv4);
        body(() -> BinaryStreamUtils.writeInet4Address(out, value));
    }

    @Override
    public void writeIPv6(Inet6Address value) throws IOException {
        requireValue(value);
        prepare(ClickHouseDataType.IPv6);
        body(() -> BinaryStreamUtils.writeInet6Address(out, value));
    }

    @Override
    public void writeDecimal(BigDecimal value) throws IOException {
        requireValue(value);
        ClickHouseColumn column = prepare(ClickHouseDataType.Decimal, ClickHouseDataType.Decimal32,
                ClickHouseDataType.Decimal64, ClickHouseDataType.Decimal128, ClickHouseDataType.Decimal256);
        int precision = column.getPrecision();
        int scale = column.getScale();
        body(() -> BinaryStreamUtils.writeDecimal(out, value, precision, scale));
    }

    @Override
    public void writeEnum8(int value) throws IOException {
        checkRange(value, Byte.MIN_VALUE, Byte.MAX_VALUE, "Enum8");
        prepare(ClickHouseDataType.Enum8);
        body(() -> BinaryStreamUtils.writeInt8(out, value));
    }

    @Override
    public void writeEnum8(String name) throws IOException {
        writeEnum(name, ClickHouseDataType.Enum8);
    }

    @Override
    public void writeEnum16(int value) throws IOException {
        checkRange(value, Short.MIN_VALUE, Short.MAX_VALUE, "Enum16");
        prepare(ClickHouseDataType.Enum16);
        body(() -> BinaryStreamUtils.writeInt16(out, value));
    }

    @Override
    public void writeEnum16(String name) throws IOException {
        writeEnum(name, ClickHouseDataType.Enum16);
    }

    @Override
    public void writeNull() throws IOException {
        ensureWritable();
        if (cursor.kind == SlotCursor.Kind.MAP && !cursor.expectingValue()) {
            owner.fail();
            throw new IllegalArgumentException("Map keys are not nullable");
        }
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        if (effective.isNullable()) {
            try {
                if (cursor.kind == SlotCursor.Kind.ROW && owner.defaultsSupport()) {
                    SerializerUtils.writeNonNull(out);
                }
                SerializerUtils.writeNull(out);
            } catch (IOException e) {
                owner.fail();
                throw e;
            }
            cursor.advance();
            return;
        }
        if (effective.getDataType() == ClickHouseDataType.Dynamic) {
            writePresent(slot);
            body(() -> SerializerUtils.writeDynamicTypeTag(out, StreamTypes.NOTHING));
            return;
        }
        if (effective.getDataType() == ClickHouseDataType.Variant
                || effective.getDataType() == ClickHouseDataType.Geometry) {
            writePresent(slot);
            body(() -> out.write(0xFF));
            return;
        }
        owner.fail();
        throw new IllegalArgumentException("Cannot write null into non-nullable " + StreamTypes.describe(effective));
    }

    @Override
    public void writeDefault() throws IOException {
        ensureWritable();
        if (cursor.kind != SlotCursor.Kind.ROW || !owner.defaultsSupport()) {
            owner.fail();
            throw new IllegalStateException("writeDefault is only valid for a top-level column of RowBinaryWithDefaults");
        }
        ClickHouseColumn slot = slot();
        if (!slot.hasDefault() || slot.getDefaultValue() != ClickHouseColumn.DefaultValue.DEFAULT) {
            owner.fail();
            throw new IllegalArgumentException("Column '" + slot.getColumnName() + "' has no DEFAULT");
        }
        try {
            SerializerUtils.writeNull(out);
        } catch (IOException e) {
            owner.fail();
            throw e;
        }
        cursor.advance();
    }

    @Override
    public void writeBitmap(ClickHouseBitmap value) throws IOException {
        if (value == null) {
            owner.fail();
            throw new IllegalArgumentException("groupBitmap value is null");
        }
        ensureWritable();
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        if (effective.getDataType() != ClickHouseDataType.AggregateFunction
                || effective.getAggregateFunction() != ClickHouseAggregateFunction.groupBitmap) {
            owner.fail();
            throw new IllegalStateException("writeBitmap is only valid for AggregateFunction(groupBitmap, ...) but was "
                    + StreamTypes.describe(effective));
        }
        writePresent(slot);
        body(() -> out.write(value.toBytes()));
    }

    @Override
    public ValueWriter beginArray() throws IOException {
        return openArray(SlotCursor.UNKNOWN);
    }

    @Override
    public ValueWriter beginArray(int size) throws IOException {
        if (size < 0) {
            owner.fail();
            throw new IllegalArgumentException("Array size is negative");
        }
        return openArray(size);
    }

    @Override
    public ValueWriter beginTuple() throws IOException {
        ensureWritable();
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        List<ClickHouseColumn> fields = StreamTypes.tupleFields(effective);
        if (fields == null) {
            owner.fail();
            throw new IllegalStateException("beginTuple is not valid for " + StreamTypes.describe(effective));
        }
        writePresent(slot);
        return nest(new StreamWriterScope(owner, this, SlotCursor.tuple(fields), out, null));
    }

    @Override
    public ValueWriter beginMap() throws IOException {
        return openMap(SlotCursor.UNKNOWN);
    }

    @Override
    public ValueWriter beginMap(int pairs) throws IOException {
        if (pairs < 0) {
            owner.fail();
            throw new IllegalArgumentException("Map size is negative");
        }
        return openMap(pairs);
    }

    @Override
    public ValueWriter beginNested() throws IOException {
        return openNested(SlotCursor.UNKNOWN);
    }

    @Override
    public ValueWriter beginNested(int rows) throws IOException {
        if (rows < 0) {
            owner.fail();
            throw new IllegalArgumentException("Nested row count is negative");
        }
        return openNested(rows);
    }

    @Override
    public ValueWriter beginVariant(int ordinal) throws IOException {
        if (ordinal < 0) {
            owner.fail();
            throw new IllegalArgumentException("Variant ordinal is negative");
        }
        ensureWritable();
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        if (effective.getDataType() != ClickHouseDataType.Variant
                && effective.getDataType() != ClickHouseDataType.Geometry) {
            owner.fail();
            throw new IllegalStateException("beginVariant is not valid for " + StreamTypes.describe(effective));
        }
        List<ClickHouseColumn> alternatives = effective.getNestedColumns();
        if (ordinal >= alternatives.size()) {
            owner.fail();
            throw new IllegalArgumentException("Variant ordinal " + ordinal + " is out of range for "
                    + StreamTypes.describe(effective));
        }
        writePresent(slot);
        try {
            BinaryStreamUtils.writeUnsignedInt8(out, ordinal);
        } catch (IOException e) {
            owner.fail();
            throw e;
        }
        return nest(new StreamWriterScope(owner, this, SlotCursor.single(alternatives.get(ordinal)), out, null));
    }

    @Override
    public ValueWriter beginDynamic(ClickHouseColumn runtimeType) throws IOException {
        if (runtimeType == null) {
            owner.fail();
            throw new IllegalArgumentException("Dynamic runtime type is required");
        }
        ensureWritable();
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        if (effective.getDataType() != ClickHouseDataType.Dynamic) {
            owner.fail();
            throw new IllegalStateException("beginDynamic is not valid for " + StreamTypes.describe(effective));
        }
        try {
            StreamTypes.rejectDynamicPayload(runtimeType);
        } catch (RuntimeException e) {
            owner.fail();
            throw e;
        }
        writePresent(slot);
        try {
            SerializerUtils.writeDynamicTypeTag(out, runtimeType);
        } catch (IOException e) {
            owner.fail();
            throw e;
        } catch (RuntimeException e) {
            owner.fail();
            throw e;
        }
        StreamWriterScope childScope = new StreamWriterScope(owner, this, SlotCursor.single(runtimeType), out, null);
        if (StreamTypes.unwrap(runtimeType).getDataType() == ClickHouseDataType.Nothing) {
            childScope.cursor.forceComplete();
        }
        return nest(childScope);
    }

    @Override
    public ValueWriter beginJson() throws IOException {
        return openDocument(SlotCursor.UNKNOWN);
    }

    @Override
    public ValueWriter beginJson(int paths) throws IOException {
        if (paths < 0) {
            owner.fail();
            throw new IllegalArgumentException("JSON path count is negative");
        }
        return openDocument(paths);
    }

    private ValueWriter openDocument(int paths) throws IOException {
        if (cursor.kind == SlotCursor.Kind.JSON) {
            owner.fail();
            throw new IllegalStateException("Inside a JSON object use beginJson(path, paths)");
        }
        ensureWritable();
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        if (effective.getDataType() != ClickHouseDataType.JSON) {
            owner.fail();
            throw new IllegalStateException("beginJson is not valid for " + StreamTypes.describe(effective));
        }
        writePresent(slot);
        return openJsonValue(effective, "", paths);
    }

    @Override
    public void writeString(String path, String value) throws IOException {
        if (value == null) {
            writeNull(path);
            return;
        }
        writeJsonScalar(path, ClickHouseDataType.String, DYN_STRING, () -> BinaryStreamUtils.writeString(out, value));
    }

    @Override
    public void writeInt32(String path, int value) throws IOException {
        writeJsonScalar(path, ClickHouseDataType.Int32, DYN_INT32, () -> BinaryStreamUtils.writeInt32(out, value));
    }

    @Override
    public void writeInt64(String path, long value) throws IOException {
        writeJsonScalar(path, ClickHouseDataType.Int64, DYN_INT64, () -> BinaryStreamUtils.writeInt64(out, value));
    }

    @Override
    public void writeFloat64(String path, double value) throws IOException {
        writeJsonScalar(path, ClickHouseDataType.Float64, DYN_FLOAT64, () -> BinaryStreamUtils.writeFloat64(out, value));
    }

    @Override
    public void writeBool(String path, boolean value) throws IOException {
        writeJsonScalar(path, ClickHouseDataType.Bool, DYN_BOOL, () -> BinaryStreamUtils.writeBoolean(out, value));
    }

    @Override
    public void writeNull(String path) throws IOException {
        ensureJson();
        String full = fullPath(path);
        ClickHouseColumn typed = typedPath(full);
        writePathName(full);
        try {
            if (typed == null) {
                SerializerUtils.writeDynamicTypeTag(out, StreamTypes.NOTHING);
            } else if (typed.isNullable()) {
                SerializerUtils.writeNull(out);
            } else {
                throw new IllegalArgumentException("Cannot write null into non-nullable JSON path '" + full + "'");
            }
        } catch (IOException e) {
            owner.fail();
            throw e;
        } catch (RuntimeException e) {
            owner.fail();
            throw e;
        }
        cursor.advance();
    }

    @Override
    public ValueWriter beginObject(String path) throws IOException {
        return openObject(path, SlotCursor.UNKNOWN);
    }

    @Override
    public ValueWriter beginObject(String path, int paths) throws IOException {
        if (paths < 0) {
            owner.fail();
            throw new IllegalArgumentException("JSON path count is negative");
        }
        return openObject(path, paths);
    }

    private ValueWriter openObject(String path, int paths) throws IOException {
        ensureJson();
        StreamWriterScope nested = new StreamWriterScope(owner, this, SlotCursor.json(paths), out, null);
        nested.jsonType = jsonType;
        nested.pathPrefix = fullPath(path) + ".";
        nested.flattensIntoParent = true;
        return nest(nested);
    }

    @Override
    public ValueWriter beginArray(String path, ClickHouseColumn elementType) throws IOException {
        return beginJsonArray(path, elementType, SlotCursor.UNKNOWN);
    }

    @Override
    public ValueWriter beginArray(String path, ClickHouseColumn elementType, int size) throws IOException {
        if (size < 0) {
            owner.fail();
            throw new IllegalArgumentException("Array size is negative");
        }
        return beginJsonArray(path, elementType, size);
    }

    @Override
    public ValueWriter beginJson(String path) throws IOException {
        return beginNestedJson(path, SlotCursor.UNKNOWN);
    }

    @Override
    public ValueWriter beginJson(String path, int paths) throws IOException {
        if (paths < 0) {
            owner.fail();
            throw new IllegalArgumentException("JSON path count is negative");
        }
        return beginNestedJson(path, paths);
    }

    @Override
    public ValueWriter beginQBit() throws IOException {
        ensureWritable();
        ClickHouseColumn effective = StreamTypes.unwrap(slot());
        if (effective.getDataType() != ClickHouseDataType.QBit) {
            owner.fail();
            throw new IllegalStateException("beginQBit is not valid for " + StreamTypes.describe(effective));
        }
        return beginArray(effective.getPrecision());
    }

    @Override
    public void writeInt8Array(byte[] values) throws IOException {
        writePrimitiveArray(values, ClickHouseDataType.Int8, (index) -> BinaryStreamUtils.writeInt8(out, values[index]));
    }

    @Override
    public void writeInt16Array(short[] values) throws IOException {
        writePrimitiveArray(values, values == null ? 0 : values.length, ClickHouseDataType.Int16,
                (index) -> BinaryStreamUtils.writeInt16(out, values[index]));
    }

    @Override
    public void writeInt32Array(int[] values) throws IOException {
        writePrimitiveArray(values, values == null ? 0 : values.length, ClickHouseDataType.Int32,
                (index) -> BinaryStreamUtils.writeInt32(out, values[index]));
    }

    @Override
    public void writeInt64Array(long[] values) throws IOException {
        writePrimitiveArray(values, values == null ? 0 : values.length, ClickHouseDataType.Int64,
                (index) -> BinaryStreamUtils.writeInt64(out, values[index]));
    }

    @Override
    public void writeFloat32Array(float[] values) throws IOException {
        writePrimitiveArray(values, values == null ? 0 : values.length, ClickHouseDataType.Float32,
                (index) -> BinaryStreamUtils.writeFloat32(out, values[index]));
    }

    @Override
    public void writeFloat64Array(double[] values) throws IOException {
        writePrimitiveArray(values, values == null ? 0 : values.length, ClickHouseDataType.Float64,
                (index) -> BinaryStreamUtils.writeFloat64(out, values[index]));
    }

    @Override
    public void writeBFloat16Array(float[] values) throws IOException {
        writePrimitiveArray(values, values == null ? 0 : values.length, ClickHouseDataType.BFloat16,
                (index) -> BinaryStreamUtils.writeBFloat16(out, values[index]));
    }

    @Override
    public void writeBoolArray(boolean[] values) throws IOException {
        writePrimitiveArray(values, values == null ? 0 : values.length, ClickHouseDataType.Bool,
                (index) -> BinaryStreamUtils.writeBoolean(out, values[index]));
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        if (owner.isFailed()) {
            closed = true;
            return;
        }
        if (child != null) {
            owner.fail();
            closed = true;
            throw new IllegalStateException("Unclosed nested value");
        }
        if (flattensIntoParent) {
            try {
                cursor.checkComplete();
            } catch (RuntimeException e) {
                owner.fail();
                closed = true;
                throw e;
            }
            closed = true;
            parent.child = null;
            parent.cursor.credit(cursor.completed());
            return;
        }
        try {
            cursor.checkComplete();
            if (buffer != null) {
                SerializerUtils.writeVarInt(parent.out, cursor.completed());
                buffer.writeTo(parent.out);
            }
        } catch (RuntimeException e) {
            owner.fail();
            closed = true;
            throw e;
        } catch (IOException e) {
            owner.fail();
            closed = true;
            throw e;
        }
        closed = true;
        if (parent != null) {
            parent.child = null;
            parent.cursor.advance();
        } else {
            owner.onRowClosed();
        }
    }

    private void writeEnum(String name, ClickHouseDataType type) throws IOException {
        if (name == null) {
            writeNullFor(type);
            return;
        }
        ensureWritable();
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        if (effective.getDataType() != type) {
            owner.fail();
            throw new IllegalStateException("Cannot write " + type + " into " + StreamTypes.describe(effective));
        }
        int wire;
        try {
            wire = effective.getEnumConstants().value(name);
        } catch (IllegalArgumentException e) {
            owner.fail();
            throw e;
        }
        writePresent(slot);
        if (type == ClickHouseDataType.Enum8) {
            body(() -> BinaryStreamUtils.writeInt8(out, wire));
        } else {
            body(() -> BinaryStreamUtils.writeInt16(out, wire));
        }
    }

    private void writeNullFor(ClickHouseDataType... allowed) throws IOException {
        ensureWritable();
        ClickHouseColumn effective = StreamTypes.unwrap(slot());
        if (!StreamTypes.isOneOf(effective.getDataType(), allowed)) {
            owner.fail();
            throw new IllegalStateException("Cannot write " + allowed[0] + " into " + StreamTypes.describe(effective));
        }
        writeNull();
    }

    private ValueWriter openJsonValue(ClickHouseColumn type, String prefix, int paths) throws IOException {
        OutputStream destination = out;
        ByteArrayOutputStream childBuffer = null;
        if (paths == SlotCursor.UNKNOWN) {
            childBuffer = new ByteArrayOutputStream();
            destination = childBuffer;
        } else {
            try {
                SerializerUtils.writeVarInt(out, paths);
            } catch (IOException e) {
                owner.fail();
                throw e;
            }
        }
        StreamWriterScope nested = new StreamWriterScope(owner, this, SlotCursor.json(paths), destination, childBuffer);
        nested.jsonType = type;
        nested.pathPrefix = prefix;
        return nest(nested);
    }

    private void writeJsonScalar(String path, ClickHouseDataType type, ClickHouseColumn dynamicType, IOAction action)
            throws IOException {
        ensureJson();
        String full = fullPath(path);
        ClickHouseColumn typed = typedPath(full);
        writePathName(full);
        try {
            if (typed == null) {
                SerializerUtils.writeDynamicTypeTag(out, dynamicType);
                action.run();
            } else {
                ClickHouseColumn effective = StreamTypes.unwrap(typed);
                if (effective.getDataType() != type) {
                    throw new IllegalStateException("Cannot write " + type + " into JSON path '" + full
                            + "' of type " + typed.getOriginalTypeName());
                }
                if (typed.isNullable()) {
                    SerializerUtils.writeNonNull(out);
                }
                action.run();
            }
        } catch (IOException e) {
            owner.fail();
            throw e;
        } catch (RuntimeException e) {
            owner.fail();
            throw e;
        }
        cursor.advance();
    }

    private ValueWriter beginJsonArray(String path, ClickHouseColumn elementType, int size) throws IOException {
        ensureJson();
        if (elementType == null) {
            owner.fail();
            throw new IllegalArgumentException("JSON array element type is required");
        }
        String full = fullPath(path);
        ClickHouseColumn typed = typedPath(full);
        writePathName(full);
        ClickHouseColumn element = elementType;
        try {
            if (typed == null) {
                ClickHouseColumn arrayType = ClickHouseColumn.of("v", "Array(" + elementType.getOriginalTypeName() + ")");
                SerializerUtils.writeDynamicTypeTag(out, arrayType);
                element = StreamTypes.arrayElement(arrayType);
            } else {
                ClickHouseColumn effective = StreamTypes.unwrap(typed);
                if (effective.getDataType() != ClickHouseDataType.Array) {
                    throw new IllegalStateException("Cannot write an array into JSON path '" + full
                            + "' of type " + typed.getOriginalTypeName());
                }
                element = StreamTypes.arrayElement(effective);
                if (typed.isNullable()) {
                    SerializerUtils.writeNonNull(out);
                }
            }
        } catch (IOException e) {
            owner.fail();
            throw e;
        } catch (RuntimeException e) {
            owner.fail();
            throw e;
        }
        return openLengthPrefixed(SlotCursor.array(element, size), size);
    }

    private ValueWriter beginNestedJson(String path, int paths) throws IOException {
        ensureJson();
        String full = fullPath(path);
        ClickHouseColumn typed = typedPath(full);
        writePathName(full);
        ClickHouseColumn nestedType = DYN_JSON;
        try {
            if (typed == null) {
                SerializerUtils.writeDynamicTypeTag(out, DYN_JSON);
            } else {
                nestedType = StreamTypes.unwrap(typed);
                if (nestedType.getDataType() != ClickHouseDataType.JSON) {
                    throw new IllegalStateException("Cannot write JSON into path '" + full
                            + "' of type " + typed.getOriginalTypeName());
                }
                if (typed.isNullable()) {
                    SerializerUtils.writeNonNull(out);
                }
            }
        } catch (IOException e) {
            owner.fail();
            throw e;
        } catch (RuntimeException e) {
            owner.fail();
            throw e;
        }
        return openJsonValue(nestedType, "", paths);
    }

    private void ensureJson() {
        ensureWritable();
        if (cursor.kind != SlotCursor.Kind.JSON) {
            owner.fail();
            throw new IllegalStateException("A JSON path is written inside beginJson or beginObject");
        }
    }

    private String fullPath(String path) {
        if (path == null || path.isEmpty()) {
            owner.fail();
            throw new IllegalArgumentException("JSON path is empty");
        }
        return pathPrefix + path;
    }

    private ClickHouseColumn typedPath(String full) {
        if (jsonType == null || jsonType.getJsonPredefinedPaths() == null) {
            return null;
        }
        return jsonType.getJsonPredefinedPaths().get(full);
    }

    private void writePathName(String full) throws IOException {
        try {
            BinaryStreamUtils.writeString(out, full);
        } catch (IOException e) {
            owner.fail();
            throw e;
        }
    }

    private ValueWriter openArray(int size) throws IOException {
        ensureWritable();
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        ClickHouseColumn element = StreamTypes.arrayElement(effective);
        if (element == null) {
            owner.fail();
            throw new IllegalStateException("beginArray is not valid for " + StreamTypes.describe(effective));
        }
        if (effective.getDataType() == ClickHouseDataType.QBit && size != effective.getPrecision()) {
            owner.fail();
            throw new IllegalArgumentException("QBit column '" + effective.getColumnName()
                    + "' expects exactly " + effective.getPrecision() + " elements but got "
                    + (size == SlotCursor.UNKNOWN ? "an unknown size" : Integer.toString(size)));
        }
        writePresent(slot);
        return openLengthPrefixed(SlotCursor.array(element, size), size);
    }

    private ValueWriter openNested(int rows) throws IOException {
        ensureWritable();
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        if (effective.getDataType() != ClickHouseDataType.Nested) {
            owner.fail();
            throw new IllegalStateException("beginNested is not valid for " + StreamTypes.describe(effective));
        }
        ClickHouseColumn tuple = ClickHouseColumn.tuple(effective.getColumnName(), false,
                effective.getNestedColumns().toArray(new ClickHouseColumn[0]));
        writePresent(slot);
        return openLengthPrefixed(SlotCursor.array(tuple, rows), rows);
    }

    private ValueWriter openMap(int pairs) throws IOException {
        ensureWritable();
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        if (effective.getDataType() != ClickHouseDataType.Map) {
            owner.fail();
            throw new IllegalStateException("beginMap is not valid for " + StreamTypes.describe(effective));
        }
        writePresent(slot);
        return openLengthPrefixed(SlotCursor.map(effective.getKeyInfo(), effective.getValueInfo(), pairs), pairs);
    }

    private ValueWriter openLengthPrefixed(SlotCursor childCursor, int size) throws IOException {
        OutputStream destination = out;
        ByteArrayOutputStream childBuffer = null;
        if (size == SlotCursor.UNKNOWN) {
            childBuffer = new ByteArrayOutputStream();
            destination = childBuffer;
        } else {
            try {
                SerializerUtils.writeVarInt(out, size);
            } catch (IOException e) {
                owner.fail();
                throw e;
            }
        }
        return nest(new StreamWriterScope(owner, this, childCursor, destination, childBuffer));
    }

    private void writePrimitiveArray(byte[] values, ClickHouseDataType elementType, ElementWriter writer) throws IOException {
        if (values == null) {
            writeNullFor(ClickHouseDataType.Array, ClickHouseDataType.QBit);
            return;
        }
        writePrimitiveArray(values, values.length, elementType, writer);
    }

    private void writePrimitiveArray(Object values, int length, ClickHouseDataType elementType, ElementWriter writer)
            throws IOException {
        if (values == null) {
            writeNullFor(ClickHouseDataType.Array, ClickHouseDataType.QBit);
            return;
        }
        ensureWritable();
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        ClickHouseDataType dataType = effective.getDataType();
        if (dataType != ClickHouseDataType.Array && dataType != ClickHouseDataType.QBit) {
            owner.fail();
            throw new IllegalStateException("Cannot write a primitive array into " + StreamTypes.describe(effective));
        }
        if (dataType == ClickHouseDataType.QBit && length != effective.getPrecision()) {
            owner.fail();
            throw new IllegalArgumentException("QBit column '" + effective.getColumnName()
                    + "' expects exactly " + effective.getPrecision() + " elements but got " + length);
        }
        ClickHouseColumn element = StreamTypes.unwrap(StreamTypes.arrayElement(effective));
        if (element.getDataType() != elementType) {
            owner.fail();
            throw new IllegalStateException("Cannot write " + elementType + " array into " + StreamTypes.describe(effective));
        }
        boolean nullable = element.isNullable();
        writePresent(slot);
        try {
            SerializerUtils.writeVarInt(out, length);
            for (int i = 0; i < length; i++) {
                if (nullable) {
                    SerializerUtils.writeNonNull(out);
                }
                writer.write(i);
            }
        } catch (IOException e) {
            owner.fail();
            throw e;
        } catch (RuntimeException e) {
            owner.fail();
            throw e;
        }
        cursor.advance();
    }

    private ClickHouseColumn prepare(ClickHouseDataType... allowed) throws IOException {
        ensureWritable();
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        if (!StreamTypes.isOneOf(effective.getDataType(), allowed)) {
            owner.fail();
            throw new IllegalStateException("Cannot write " + allowed[0] + " into " + StreamTypes.describe(effective));
        }
        writePresent(slot);
        return effective;
    }

    private void writePresent(ClickHouseColumn slot) throws IOException {
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        try {
            if (cursor.kind == SlotCursor.Kind.ROW && owner.defaultsSupport()) {
                SerializerUtils.writeNonNull(out);
            }
            if (effective.isNullable()) {
                SerializerUtils.writeNonNull(out);
            }
        } catch (IOException e) {
            owner.fail();
            throw e;
        }
    }

    private void body(IOAction action) throws IOException {
        try {
            action.run();
        } catch (IOException e) {
            owner.fail();
            throw e;
        } catch (RuntimeException e) {
            owner.fail();
            throw e;
        }
        cursor.advance();
    }

    private ClickHouseColumn slot() {
        try {
            return cursor.current();
        } catch (RuntimeException e) {
            owner.fail();
            throw e;
        }
    }

    private void ensureWritable() {
        if (owner.isFailed()) {
            throw new IllegalStateException("Writer is unusable after a previous failure");
        }
        if (closed) {
            throw new IllegalStateException("Scope is closed");
        }
        if (child != null) {
            owner.fail();
            throw new IllegalStateException("A nested value is still open");
        }
    }

    private void requireValue(Object value) {
        if (value == null) {
            owner.fail();
            throw new IllegalArgumentException("Value is null");
        }
    }

    private void checkRange(long value, long min, long max, String type) {
        if (value < min || value > max) {
            owner.fail();
            throw new IllegalArgumentException(type + " overflow: " + value);
        }
    }

    private ZoneId zone(ClickHouseColumn column) {
        if (column.getTimeZone() != null) {
            return column.getTimeZone().toZoneId();
        }
        return owner.timeZone().toZoneId();
    }

    private StreamWriterScope nest(StreamWriterScope childScope) {
        this.child = childScope;
        return childScope;
    }

    private interface IOAction {
        void run() throws IOException;
    }

    private interface ElementWriter {
        void write(int index) throws IOException;
    }
}
