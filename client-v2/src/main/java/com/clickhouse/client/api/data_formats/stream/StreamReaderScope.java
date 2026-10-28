package com.clickhouse.client.api.data_formats.stream;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.data_formats.internal.StringValue;
import com.clickhouse.client.api.query.NullValueException;
import com.clickhouse.data.ClickHouseAggregateFunction;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.value.ClickHouseBitmap;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Scope implementation shared by a row and every nested container on the read path.
 */
final class StreamReaderScope implements ValueReader {

    private final RowBinaryStreamReader owner;

    private final StreamReaderScope parent;

    private final SlotCursor cursor;

    private final int variantOrdinal;

    private StreamReaderScope child;

    private boolean markerConsumed;

    private boolean closed;

    private StreamReaderScope(RowBinaryStreamReader owner, StreamReaderScope parent, SlotCursor cursor, int variantOrdinal) {
        this.owner = owner;
        this.parent = parent;
        this.cursor = cursor;
        this.variantOrdinal = variantOrdinal;
    }

    static StreamReaderScope row(RowBinaryStreamReader owner, List<ClickHouseColumn> columns) {
        return new StreamReaderScope(owner, null, SlotCursor.row(columns), -1);
    }

    boolean isClosed() {
        return closed;
    }

    @Override
    public byte readInt8() throws IOException {
        prepare(ClickHouseDataType.Int8);
        return finishByte(owner.binary().readByte());
    }

    @Override
    public short readInt16() throws IOException {
        prepare(ClickHouseDataType.Int16);
        return finishShort(owner.binary().readShortLE());
    }

    @Override
    public int readInt32() throws IOException {
        prepare(ClickHouseDataType.Int32);
        return finishInt(owner.binary().readIntLE());
    }

    @Override
    public long readInt64() throws IOException {
        prepare(ClickHouseDataType.Int64);
        return finishLong(owner.binary().readLongLE());
    }

    @Override
    public BigInteger readInt128() throws IOException {
        prepare(ClickHouseDataType.Int128);
        return finish(owner.binary().readBigIntegerLE(BinaryStreamReader.INT128_SIZE, false));
    }

    @Override
    public BigInteger readInt256() throws IOException {
        prepare(ClickHouseDataType.Int256);
        return finish(owner.binary().readBigIntegerLE(BinaryStreamReader.INT256_SIZE, false));
    }

    @Override
    public int readUInt8() throws IOException {
        prepare(ClickHouseDataType.UInt8);
        return finishInt(owner.binary().readUnsignedByte());
    }

    @Override
    public int readUInt16() throws IOException {
        prepare(ClickHouseDataType.UInt16);
        return finishInt(owner.binary().readUnsignedShortLE());
    }

    @Override
    public long readUInt32() throws IOException {
        prepare(ClickHouseDataType.UInt32);
        return finishLong(owner.binary().readUnsignedIntLE());
    }

    @Override
    public BigInteger readUInt64() throws IOException {
        prepare(ClickHouseDataType.UInt64);
        return finish(owner.binary().readBigIntegerLE(BinaryStreamReader.INT64_SIZE, true));
    }

    @Override
    public BigInteger readUInt128() throws IOException {
        prepare(ClickHouseDataType.UInt128);
        return finish(owner.binary().readBigIntegerLE(BinaryStreamReader.INT128_SIZE, true));
    }

    @Override
    public BigInteger readUInt256() throws IOException {
        prepare(ClickHouseDataType.UInt256);
        return finish(owner.binary().readBigIntegerLE(BinaryStreamReader.INT256_SIZE, true));
    }

    @Override
    public float readFloat32() throws IOException {
        prepare(ClickHouseDataType.Float32);
        return finishFloat(owner.binary().readFloatLE());
    }

    @Override
    public double readFloat64() throws IOException {
        prepare(ClickHouseDataType.Float64);
        return finishDouble(owner.binary().readDoubleLE());
    }

    @Override
    public float readBFloat16() throws IOException {
        prepare(ClickHouseDataType.BFloat16);
        return finishFloat(owner.binary().readBFloat16LE());
    }

    @Override
    public boolean readBool() throws IOException {
        prepare(ClickHouseDataType.Bool);
        int value = owner.binary().readByte() & 0xFF;
        cursor.advance();
        return value == 1;
    }

    @Override
    public String readString() throws IOException {
        return readStringValue().asString();
    }

    @Override
    public StringValue readStringValue() throws IOException {
        ensureReadable();
        ClickHouseColumn effective = StreamTypes.unwrap(slot());
        ClickHouseDataType type = effective.getDataType();
        if (type != ClickHouseDataType.String && type != ClickHouseDataType.FixedString
                && type != ClickHouseDataType.JSON) {
            owner.fail();
            throw new IllegalStateException("Cannot read a string from " + StreamTypes.describe(effective));
        }
        consumePresent(effective);
        try {
            StringValue value = type == ClickHouseDataType.FixedString
                    ? owner.binary().readFixedStringValue(effective.getPrecision())
                    : owner.binary().readStringValue();
            cursor.advance();
            return value;
        } catch (IOException e) {
            owner.fail();
            throw e;
        }
    }

    @Override
    public String readFixedString() throws IOException {
        ensureReadable();
        ClickHouseColumn effective = StreamTypes.unwrap(slot());
        if (effective.getDataType() != ClickHouseDataType.FixedString) {
            owner.fail();
            throw new IllegalStateException("Cannot read FixedString from " + StreamTypes.describe(effective));
        }
        return readStringValue().asString();
    }

    @Override
    public LocalDate readDate() throws IOException {
        prepare(ClickHouseDataType.Date);
        return finish(owner.binary().readDateAsLocalDate());
    }

    @Override
    public LocalDate readDate32() throws IOException {
        prepare(ClickHouseDataType.Date32);
        return finish(owner.binary().readDate32AsLocalDate());
    }

    @Override
    public ZonedDateTime readDateTime() throws IOException {
        ClickHouseColumn column = prepare(ClickHouseDataType.DateTime, ClickHouseDataType.DateTime32);
        TimeZone zone = column.getTimeZone() != null ? column.getTimeZone() : owner.timeZone();
        return finish(BinaryStreamReader.readDateTime32(owner.stream(), owner.scratch(), zone));
    }

    @Override
    public ZonedDateTime readDateTime64() throws IOException {
        ClickHouseColumn column = prepare(ClickHouseDataType.DateTime64);
        TimeZone zone = column.getTimeZone() != null ? column.getTimeZone() : owner.timeZone();
        return finish(BinaryStreamReader.readDateTime64(owner.stream(), owner.scratch(), column.getScale(), zone));
    }

    @Override
    public UUID readUUID() throws IOException {
        prepare(ClickHouseDataType.UUID);
        return finish(new UUID(owner.binary().readLongLE(), owner.binary().readLongLE()));
    }

    @Override
    public Inet4Address readIPv4() throws IOException {
        prepare(ClickHouseDataType.IPv4);
        byte[] raw = new byte[4];
        BinaryStreamReader.readNBytesLE(owner.stream(), raw, 0, 4);
        return finish((Inet4Address) InetAddress.getByAddress(raw));
    }

    @Override
    public Inet6Address readIPv6() throws IOException {
        prepare(ClickHouseDataType.IPv6);
        byte[] raw = BinaryStreamReader.readNBytes(owner.stream(), 16);
        return finish((Inet6Address) InetAddress.getByAddress(raw));
    }

    @Override
    public BigDecimal readDecimal() throws IOException {
        ClickHouseColumn column = prepare(ClickHouseDataType.Decimal, ClickHouseDataType.Decimal32,
                ClickHouseDataType.Decimal64, ClickHouseDataType.Decimal128, ClickHouseDataType.Decimal256);
        return finish(owner.binary().readDecimal(column.getPrecision(), column.getScale()));
    }

    @Override
    public int readEnum8() throws IOException {
        prepare(ClickHouseDataType.Enum8);
        return finishInt(owner.binary().readByte());
    }

    @Override
    public int readEnum16() throws IOException {
        prepare(ClickHouseDataType.Enum16);
        return finishInt(owner.binary().readShortLE());
    }

    @Override
    public ClickHouseBitmap readBitmap() throws IOException {
        ensureReadable();
        ClickHouseColumn effective = StreamTypes.unwrap(slot());
        if (effective.getDataType() != ClickHouseDataType.AggregateFunction
                || effective.getAggregateFunction() != ClickHouseAggregateFunction.groupBitmap) {
            owner.fail();
            throw new IllegalStateException("readBitmap is only valid for AggregateFunction(groupBitmap, ...) but was "
                    + StreamTypes.describe(effective));
        }
        consumePresent(effective);
        try {
            ClickHouseBitmap value = ClickHouseBitmap.deserialize(owner.stream(),
                    effective.getNestedColumns().get(0).getDataType());
            cursor.advance();
            return value;
        } catch (IOException e) {
            owner.fail();
            throw e;
        }
    }

    @Override
    public boolean isNull() throws IOException {
        ensureReadable();
        if (markerConsumed) {
            return false;
        }
        if (cursor.kind == SlotCursor.Kind.MAP && !cursor.expectingValue()) {
            return false;
        }
        ClickHouseColumn effective = StreamTypes.unwrap(slot());
        if (effective.isNullable()) {
            int marker = readMarker();
            if (marker == 1) {
                cursor.advance();
                return true;
            }
            markerConsumed = true;
            return false;
        }
        if (effective.getDataType() == ClickHouseDataType.Variant
                || effective.getDataType() == ClickHouseDataType.Geometry) {
            int ordinal = owner.binary().readByte() & 0xFF;
            if (ordinal == 0xFF) {
                cursor.advance();
                return true;
            }
            owner.unread(ordinal);
            return false;
        }
        if (effective.getDataType() == ClickHouseDataType.Dynamic) {
            return false;
        }
        return false;
    }

    @Override
    public boolean hasNext() {
        return cursor.hasNext();
    }

    @Override
    public int size() {
        return cursor.size();
    }

    @Override
    public int variantOrdinal() {
        if (variantOrdinal < 0) {
            throw new IllegalStateException("variantOrdinal is only valid on a variant scope");
        }
        return variantOrdinal;
    }

    @Override
    public ClickHouseColumn runtimeType() {
        if (cursor.kind != SlotCursor.Kind.SINGLE) {
            throw new IllegalStateException("runtimeType is only valid on a variant or dynamic scope");
        }
        return cursor.repeated();
    }

    @Override
    public ValueReader beginArray() throws IOException {
        return openCounted(false, false);
    }

    @Override
    public ValueReader beginTuple() throws IOException {
        ensureReadable();
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        List<ClickHouseColumn> fields = StreamTypes.tupleFields(effective);
        if (fields == null) {
            owner.fail();
            throw new IllegalStateException("beginTuple is not valid for " + StreamTypes.describe(effective));
        }
        consumePresent(effective);
        return nest(new StreamReaderScope(owner, this, SlotCursor.tuple(fields), -1));
    }

    @Override
    public ValueReader beginMap() throws IOException {
        ensureReadable();
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        if (effective.getDataType() != ClickHouseDataType.Map) {
            owner.fail();
            throw new IllegalStateException("beginMap is not valid for " + StreamTypes.describe(effective));
        }
        consumePresent(effective);
        int pairs = readCount();
        return nest(new StreamReaderScope(owner, this,
                SlotCursor.map(effective.getKeyInfo(), effective.getValueInfo(), pairs), -1));
    }

    @Override
    public ValueReader beginNested() throws IOException {
        return openCounted(true, false);
    }

    @Override
    public ValueReader beginVariant() throws IOException {
        ensureReadable();
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        if (effective.getDataType() != ClickHouseDataType.Variant
                && effective.getDataType() != ClickHouseDataType.Geometry) {
            owner.fail();
            throw new IllegalStateException("beginVariant is not valid for " + StreamTypes.describe(effective));
        }
        consumePresent(effective);
        int ordinal = owner.binary().readByte() & 0xFF;
        if (ordinal == 0xFF) {
            cursor.advance();
            throw new NullValueException("Variant value is null");
        }
        List<ClickHouseColumn> alternatives = effective.getNestedColumns();
        if (ordinal >= alternatives.size()) {
            owner.fail();
            throw new ClientException("Variant ordinal " + ordinal + " is out of range");
        }
        return nest(new StreamReaderScope(owner, this, SlotCursor.single(alternatives.get(ordinal)), ordinal));
    }

    @Override
    public ValueReader beginDynamic() throws IOException {
        ensureReadable();
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        if (effective.getDataType() != ClickHouseDataType.Dynamic) {
            owner.fail();
            throw new IllegalStateException("beginDynamic is not valid for " + StreamTypes.describe(effective));
        }
        consumePresent(effective);
        ClickHouseColumn runtime = owner.binary().readDynamicType();
        StreamReaderScope childScope = new StreamReaderScope(owner, this, SlotCursor.single(runtime), -1);
        if (runtime.getDataType() == ClickHouseDataType.Nothing) {
            childScope.cursor.forceComplete();
        }
        return nest(childScope);
    }

    @Override
    public ValueReader beginQBit() throws IOException {
        return openCounted(false, true);
    }

    @Override
    public int readInto(int[] dst, int off, int len) throws IOException {
        ensureReadable();
        if (dst == null) {
            throw new IllegalArgumentException("Destination array is required");
        }
        if (off < 0 || len < 0 || off > dst.length - len) {
            throw new IllegalArgumentException("readInto range is outside the destination array");
        }
        if (cursor.kind != SlotCursor.Kind.ARRAY) {
            owner.fail();
            throw new IllegalStateException("readInto is only valid inside an array");
        }
        ClickHouseColumn element = StreamTypes.unwrap(cursor.repeated());
        if (element.getDataType() != ClickHouseDataType.Int32) {
            owner.fail();
            throw new IllegalStateException("readInto expects Array(Int32) but was " + StreamTypes.describe(element));
        }
        int count = Math.min(len, cursor.remaining());
        try {
            for (int i = 0; i < count; i++) {
                if (element.isNullable()) {
                    int marker = readMarker();
                    if (marker == 1) {
                        owner.fail();
                        throw new NullValueException("Array element is null");
                    }
                }
                dst[off + i] = owner.binary().readIntLE();
                cursor.advance();
            }
        } catch (IOException e) {
            owner.fail();
            throw e;
        }
        return count;
    }

    @Override
    public void skip() throws IOException {
        ensureReadable();
        ClickHouseColumn effective = StreamTypes.unwrap(slot());
        if (markerConsumed) {
            markerConsumed = false;
        } else if (effective.isNullable()) {
            int marker = readMarker();
            if (marker == 1) {
                cursor.advance();
                return;
            }
        }
        try {
            skipBody(effective);
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
        try {
            cursor.checkComplete();
        } catch (RuntimeException e) {
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

    private ValueReader openCounted(boolean nested, boolean qbit) throws IOException {
        ensureReadable();
        ClickHouseColumn slot = slot();
        ClickHouseColumn effective = StreamTypes.unwrap(slot);
        ClickHouseColumn element;
        if (nested) {
            if (effective.getDataType() != ClickHouseDataType.Nested) {
                owner.fail();
                throw new IllegalStateException("beginNested is not valid for " + StreamTypes.describe(effective));
            }
            element = ClickHouseColumn.tuple(effective.getColumnName(), false,
                    effective.getNestedColumns().toArray(new ClickHouseColumn[0]));
        } else if (qbit) {
            if (effective.getDataType() != ClickHouseDataType.QBit) {
                owner.fail();
                throw new IllegalStateException("beginQBit is not valid for " + StreamTypes.describe(effective));
            }
            element = StreamTypes.arrayElement(effective);
        } else {
            element = StreamTypes.arrayElement(effective);
            if (element == null || effective.getDataType() == ClickHouseDataType.QBit) {
                owner.fail();
                throw new IllegalStateException("beginArray is not valid for " + StreamTypes.describe(effective));
            }
        }
        consumePresent(effective);
        int count = readCount();
        if (qbit && count != effective.getPrecision()) {
            owner.fail();
            throw new ClientException("QBit column '" + effective.getColumnName() + "' expected exactly "
                    + effective.getPrecision() + " elements but received " + count);
        }
        return nest(new StreamReaderScope(owner, this, SlotCursor.array(element, count), -1));
    }

    private ClickHouseColumn prepare(ClickHouseDataType... allowed) throws IOException {
        ensureReadable();
        ClickHouseColumn effective = StreamTypes.unwrap(slot());
        if (!StreamTypes.isOneOf(effective.getDataType(), allowed)) {
            owner.fail();
            throw new IllegalStateException("Cannot read " + allowed[0] + " from " + StreamTypes.describe(effective));
        }
        consumePresent(effective);
        return effective;
    }

    private void consumePresent(ClickHouseColumn effective) throws IOException {
        if (markerConsumed) {
            markerConsumed = false;
            return;
        }
        if (effective.isNullable()) {
            int marker = readMarker();
            if (marker == 1) {
                cursor.advance();
                throw new NullValueException("Column '" + effective.getColumnName() + "' is null");
            }
        }
    }

    private int readMarker() throws IOException {
        int marker = owner.binary().readByte() & 0xFF;
        if (marker != 0 && marker != 1) {
            owner.fail();
            throw new ClientException("Invalid null marker: " + marker);
        }
        return marker;
    }

    private int readCount() throws IOException {
        try {
            return BinaryStreamReader.readVarInt(owner.stream());
        } catch (IOException e) {
            owner.fail();
            throw e;
        }
    }

    private ClickHouseColumn slot() {
        try {
            return cursor.current();
        } catch (RuntimeException e) {
            owner.fail();
            throw e;
        }
    }

    private void ensureReadable() {
        if (owner.isFailed()) {
            throw new IllegalStateException("Reader is unusable after a previous failure");
        }
        if (closed) {
            throw new IllegalStateException("Scope is closed");
        }
        if (child != null) {
            owner.fail();
            throw new IllegalStateException("A nested value is still open");
        }
    }

    private byte finishByte(byte value) {
        cursor.advance();
        return value;
    }

    private short finishShort(short value) {
        cursor.advance();
        return value;
    }

    private int finishInt(int value) {
        cursor.advance();
        return value;
    }

    private long finishLong(long value) {
        cursor.advance();
        return value;
    }

    private float finishFloat(float value) {
        cursor.advance();
        return value;
    }

    private double finishDouble(double value) {
        cursor.advance();
        return value;
    }

    private <T> T finish(T value) {
        cursor.advance();
        return value;
    }

    private StreamReaderScope nest(StreamReaderScope childScope) {
        this.child = childScope;
        return childScope;
    }

    private void skipBody(ClickHouseColumn column) throws IOException {
        ClickHouseDataType type = column.getDataType();
        switch (type) {
            case Int8:
            case UInt8:
            case Bool:
            case Enum8:
                owner.binary().readByte();
                return;
            case Int16:
            case UInt16:
            case Date:
            case Enum16:
            case BFloat16:
                owner.skipFully(2);
                return;
            case Int32:
            case UInt32:
            case Float32:
            case Date32:
            case DateTime:
            case DateTime32:
            case Decimal32:
            case IPv4:
            case Time:
                owner.skipFully(4);
                return;
            case Int64:
            case UInt64:
            case Float64:
            case DateTime64:
            case Decimal64:
            case Time64:
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
                owner.skipFully(8);
                return;
            case Int128:
            case UInt128:
            case Decimal128:
            case UUID:
            case IPv6:
                owner.skipFully(16);
                return;
            case Int256:
            case UInt256:
            case Decimal256:
                owner.skipFully(32);
                return;
            case Decimal:
                owner.skipFully(StreamTypes.decimalBytes(column.getPrecision()));
                return;
            case String:
            case JSON:
                owner.skipFully(BinaryStreamReader.readVarInt(owner.stream()));
                return;
            case FixedString:
                owner.skipFully(column.getPrecision());
                return;
            case Nothing:
                return;
            case Array:
            case QBit:
                skipRepeated(column.getNestedColumns().get(0), BinaryStreamReader.readVarInt(owner.stream()));
                return;
            case Nested:
                int rows = BinaryStreamReader.readVarInt(owner.stream());
                List<ClickHouseColumn> fields = column.getNestedColumns();
                for (int r = 0; r < rows; r++) {
                    for (int f = 0; f < fields.size(); f++) {
                        skipValue(fields.get(f));
                    }
                }
                return;
            case Map:
                int pairs = BinaryStreamReader.readVarInt(owner.stream());
                for (int i = 0; i < pairs; i++) {
                    skipValue(column.getKeyInfo());
                    skipValue(column.getValueInfo());
                }
                return;
            case Tuple:
                List<ClickHouseColumn> tupleFields = column.getNestedColumns();
                for (int i = 0; i < tupleFields.size(); i++) {
                    skipValue(tupleFields.get(i));
                }
                return;
            case Variant:
            case Geometry:
                int ordinal = owner.binary().readByte() & 0xFF;
                if (ordinal == 0xFF) {
                    return;
                }
                skipValue(column.getNestedColumns().get(ordinal));
                return;
            case Point:
                owner.skipFully(16);
                return;
            case Ring:
            case LineString:
            case MultiPoint:
                owner.skipFully(BinaryStreamReader.readVarInt(owner.stream()) * 16);
                return;
            case Polygon:
            case MultiLineString:
                skipRings(BinaryStreamReader.readVarInt(owner.stream()));
                return;
            case MultiPolygon:
                int polygons = BinaryStreamReader.readVarInt(owner.stream());
                for (int i = 0; i < polygons; i++) {
                    skipRings(BinaryStreamReader.readVarInt(owner.stream()));
                }
                return;
            case SimpleAggregateFunction:
            case LowCardinality:
                skipValue(column.getNestedColumns().get(0));
                return;
            case Dynamic:
                skipValue(owner.binary().readDynamicType());
                return;
            case AggregateFunction:
                if (column.getAggregateFunction() != ClickHouseAggregateFunction.groupBitmap) {
                    throw new ClientException("Cannot skip aggregate function " + column.getAggregateFunction());
                }
                ClickHouseBitmap.deserialize(owner.stream(), column.getNestedColumns().get(0).getDataType());
                return;
            default:
                throw new ClientException("Cannot skip " + StreamTypes.describe(column));
        }
    }

    private void skipRepeated(ClickHouseColumn element, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            skipValue(element);
        }
    }

    private void skipRings(int count) throws IOException {
        for (int i = 0; i < count; i++) {
            owner.skipFully(BinaryStreamReader.readVarInt(owner.stream()) * 16);
        }
    }

    private void skipValue(ClickHouseColumn column) throws IOException {
        ClickHouseColumn effective = StreamTypes.unwrap(column);
        if (effective.isNullable()) {
            int marker = owner.binary().readByte() & 0xFF;
            if (marker == 1) {
                return;
            }
            if (marker != 0) {
                throw new ClientException("Invalid null marker: " + marker);
            }
        }
        skipBody(effective);
    }
}
