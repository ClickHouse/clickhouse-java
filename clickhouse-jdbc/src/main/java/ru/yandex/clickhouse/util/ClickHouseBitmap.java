package ru.yandex.clickhouse.util;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import ru.yandex.clickhouse.domain.ClickHouseDataType;

public abstract class ClickHouseBitmap {
    static class ClickHouseRoaringBitmap extends ClickHouseBitmap {
        private final RoaringBitmap rb;

        protected ClickHouseRoaringBitmap(RoaringBitmap bitmap, ClickHouseDataType innerType) {
            super(bitmap, innerType);

            this.rb = Objects.requireNonNull(bitmap);
        }

        @Override
        public int getCardinality() {
            return rb.getCardinality();
        }

        @Override
        public void serialize(ByteBuffer buffer) {
            rb.serialize(buffer);
        }

        @Override
        public int serializedSizeInBytes() {
            return rb.serializedSizeInBytes();
        }

        @Override
        public int[] toIntArray() {
            return rb.toArray();
        }
    }

    static class ClickHouseImmutableRoaringBitmap extends ClickHouseBitmap {
        private final ImmutableRoaringBitmap rb;

        protected ClickHouseImmutableRoaringBitmap(ImmutableRoaringBitmap rb, ClickHouseDataType innerType) {
            super(rb, innerType);

            this.rb = Objects.requireNonNull(rb);
        }

        @Override
        public int getCardinality() {
            return rb.getCardinality();
        }

        @Override
        public void serialize(ByteBuffer buffer) {
            rb.serialize(buffer);
        }

        @Override
        public int serializedSizeInBytes() {
            return rb.serializedSizeInBytes();
        }

        @Override
        public int[] toIntArray() {
            return rb.toArray();
        }
    }

    static class ClickHouseMutableRoaringBitmap extends ClickHouseBitmap {
        private final MutableRoaringBitmap rb;

        protected ClickHouseMutableRoaringBitmap(MutableRoaringBitmap bitmap, ClickHouseDataType innerType) {
            super(bitmap, innerType);

            this.rb = Objects.requireNonNull(bitmap);
        }

        @Override
        public int getCardinality() {
            return rb.getCardinality();
        }

        @Override
        public void serialize(ByteBuffer buffer) {
            rb.serialize(buffer);
        }

        @Override
        public int serializedSizeInBytes() {
            return rb.serializedSizeInBytes();
        }

        @Override
        public int[] toIntArray() {
            return rb.toArray();
        }
    }

    static class ClickHouseRoaring64NavigableMap extends ClickHouseBitmap {
        private final Roaring64NavigableMap rb;

        protected ClickHouseRoaring64NavigableMap(Roaring64NavigableMap bitmap, ClickHouseDataType innerType) {
            super(bitmap, innerType);

            this.rb = Objects.requireNonNull(bitmap);
        }

        @Override
        public int getCardinality() {
            return rb.getIntCardinality();
        }

        @Override
        public long getLongCardinality() {
            return rb.getLongCardinality();
        }

        @Override
        public void serialize(ByteBuffer buffer) {
            int size = serializedSizeInBytes();
            try (ByteArrayOutputStream bas = new ByteArrayOutputStream(size)) {
                DataOutput out = new DataOutputStream(bas);
                try {
                    rb.serialize(out);
                } catch (IOException e) {
                    throw new IllegalArgumentException("Failed to serialize given bitmap", e);
                }
                buffer.put(bas.toByteArray(), 5, size - 5);
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to serialize given bitmap", e);
            }
        }

        @Override
        public int serializedSizeInBytes() {
            return (int) rb.serializedSizeInBytes();
        }

        @Override
        public long serializedSizeInBytesAsLong() {
            return rb.serializedSizeInBytes();
        }

        @Override
        public int[] toIntArray() {
            long[] longs = toLongArray();
            int len = longs.length;
            int[] ints = new int[len];
            for (int i = 0; i < len; i++) {
                ints[i] = (int) longs[i];
            }
            return ints;
        }

        @Override
        public long[] toLongArray() {
            return rb.toArray();
        }
    }

    public static ClickHouseBitmap wrap(byte... values) {
        boolean isUnsigned = true;
        int len = values.length;
        int[] ints = new int[len];
        for (int i = 0; i < len; i++) {
            byte v = values[i];
            ints[i] = v;
            if (isUnsigned && v < 0) {
                isUnsigned = false;
            }
        }

        return wrap(RoaringBitmap.bitmapOf(ints), isUnsigned ? ClickHouseDataType.UInt8 : ClickHouseDataType.Int8);
    }

    public static ClickHouseBitmap wrap(short... values) {
        boolean isUnsigned = true;
        int len = values.length;
        int[] ints = new int[len];
        for (int i = 0; i < len; i++) {
            short v = values[i];
            ints[i] = v;
            if (isUnsigned && v < 0) {
                isUnsigned = false;
            }
        }

        return wrap(RoaringBitmap.bitmapOf(ints), isUnsigned ? ClickHouseDataType.UInt16 : ClickHouseDataType.Int16);
    }

    public static ClickHouseBitmap wrap(int... values) {
        boolean isUnsigned = true;
        int len = values.length;
        int[] ints = new int[len];
        for (int i = 0; i < len; i++) {
            int v = values[i];
            ints[i] = v;
            if (isUnsigned && v < 0) {
                isUnsigned = false;
            }
        }

        return wrap(RoaringBitmap.bitmapOf(ints), isUnsigned ? ClickHouseDataType.UInt32 : ClickHouseDataType.Int32);
    }

    public static ClickHouseBitmap wrap(long... values) {
        boolean isUnsigned = true;
        int len = values.length;
        long[] longs = new long[len];
        for (int i = 0; i < len; i++) {
            long v = values[i];
            longs[i] = v;
            if (isUnsigned && v < 0) {
                isUnsigned = false;
            }
        }

        return wrap(Roaring64NavigableMap.bitmapOf(longs),
                isUnsigned ? ClickHouseDataType.UInt64 : ClickHouseDataType.Int64);
    }

    public static ClickHouseBitmap wrap(Object bitmap, ClickHouseDataType innerType) {
        final ClickHouseBitmap b;
        if (bitmap instanceof RoaringBitmap) {
            b = new ClickHouseRoaringBitmap((RoaringBitmap) bitmap, innerType);
        } else if (bitmap instanceof MutableRoaringBitmap) {
            b = new ClickHouseMutableRoaringBitmap((MutableRoaringBitmap) bitmap, innerType);
        } else if (bitmap instanceof ImmutableRoaringBitmap) {
            b = new ClickHouseImmutableRoaringBitmap((ImmutableRoaringBitmap) bitmap, innerType);
        } else if (bitmap instanceof Roaring64Bitmap) {
            b = new ClickHouseRoaring64NavigableMap(
                    Roaring64NavigableMap.bitmapOf(((Roaring64Bitmap) bitmap).toArray()), innerType);
        } else if (bitmap instanceof Roaring64NavigableMap) {
            b = new ClickHouseRoaring64NavigableMap((Roaring64NavigableMap) bitmap, innerType);
        } else {
            throw new IllegalArgumentException("Only RoaringBitmap is supported but got: " + bitmap);
        }

        return b;
    }

    public static ClickHouseBitmap deserialize(DataInputStream in, ClickHouseDataType innerType) throws IOException {
        int byteLen = byteLength(innerType);
        int flag = in.readUnsignedByte();
        if (flag == 0) {
            byte cardinality = (byte) in.readUnsignedByte();
            byte[] bytes = new byte[2 + byteLen * cardinality];
            bytes[0] = (byte) flag;
            bytes[1] = cardinality;
            in.read(bytes, 2, bytes.length - 2);

            return ClickHouseBitmap.deserialize(bytes, innerType);
        } else if (byteLen <= 4) {
            int len = Utils.readVarInt(in);
            byte[] bytes = new byte[len];
            Utils.readFully(in, bytes);
            RoaringBitmap b = new RoaringBitmap();
            b.deserialize(flip(newBuffer(len).put(bytes)));
            return ClickHouseBitmap.wrap(b, innerType);
        } else {
            // why? when serializing Roaring64NavigableMap, the initial 5 bytes were removed
            // with 8 unknown bytes appended
            throw new UnsupportedOperationException(
                    "Deserializing Roaring64NavigableMap with cardinality larger than 32 is currently not supported.");
        }
    }

    public static ClickHouseBitmap deserialize(byte[] bytes, ClickHouseDataType innerType) throws IOException {
        // https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionGroupBitmapData.h#L100
        ClickHouseBitmap rb = ClickHouseBitmap.wrap();

        if (bytes == null || bytes.length == 0) {
            return rb;
        }

        int byteLen = byteLength(innerType);
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        if (buffer.order() != ByteOrder.LITTLE_ENDIAN) {
            buffer = buffer.slice().order(ByteOrder.LITTLE_ENDIAN);
        }
        buffer = (ByteBuffer) ((Buffer) buffer.put(bytes)).flip();

        if (buffer.get() == (byte) 0) { // small set
            int cardinality = buffer.get();
            if (byteLen == 1) {
                byte[] values = new byte[cardinality];
                for (int i = 0; i < cardinality; i++) {
                    values[i] = buffer.get();
                }
                rb = ClickHouseBitmap.wrap(values);
            } else if (byteLen == 2) {
                short[] values = new short[cardinality];
                for (int i = 0; i < cardinality; i++) {
                    values[i] = buffer.getShort();
                }
                rb = ClickHouseBitmap.wrap(values);
            } else if (byteLen == 4) {
                int[] values = new int[cardinality];
                for (int i = 0; i < cardinality; i++) {
                    values[i] = buffer.getInt();
                }
                rb = ClickHouseBitmap.wrap(values);
            } else {
                long[] values = new long[cardinality];
                for (int i = 0; i < cardinality; i++) {
                    values[i] = buffer.getLong();
                }
                rb = ClickHouseBitmap.wrap(values);
            }
        } else { // serialized bitmap
            int len = Utils.readVarInt(buffer);
            if (buffer.remaining() < len) {
                throw new IllegalStateException(
                        "Need " + len + " bytes to deserialize ClickHouseBitmap but only got " + buffer.remaining());
            }
            if (byteLen <= 4) {
                RoaringBitmap b = new RoaringBitmap();
                b.deserialize(buffer);
                rb = ClickHouseBitmap.wrap(b, innerType);
            } else {
                // why? when serializing Roaring64NavigableMap, the initial 5 bytes were removed
                // with 8 unknown bytes appended
                throw new UnsupportedOperationException(
                        "Deserializing Roaring64NavigableMap with cardinality larger than 32 is currently not supported.");
            }
        }

        return rb;
    }

    private static ByteBuffer newBuffer(int capacity) {
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        if (buffer.order() != ByteOrder.LITTLE_ENDIAN) {
            buffer = buffer.slice().order(ByteOrder.LITTLE_ENDIAN);
        }

        return buffer;
    }

    private static ByteBuffer flip(ByteBuffer buffer) {
        return (ByteBuffer) ((Buffer) buffer).flip();
    }

    private static int byteLength(ClickHouseDataType type) {
        int byteLen = 0;
        switch (Objects.requireNonNull(type)) {
        case Int8:
        case UInt8:
            byteLen = 1;
            break;
        case Int16:
        case UInt16:
            byteLen = 2;
            break;
        case Int32:
        case UInt32:
            byteLen = 4;
            break;
        case Int64:
        case UInt64:
            byteLen = 8;
            break;
        default:
            throw new IllegalArgumentException("Only native integer types are supported but we got: " + type.name());
        }

        return byteLen;
    }

    protected final ClickHouseDataType innerType;
    protected final int byteLen;
    protected final Object reference;

    protected ClickHouseBitmap(Object bitmap, ClickHouseDataType innerType) {
        this.innerType = innerType;
        this.byteLen = byteLength(innerType);
        this.reference = Objects.requireNonNull(bitmap);
    }

    public abstract int getCardinality();

    public long getLongCardinality() {
        return getCardinality();
    }

    public abstract void serialize(ByteBuffer buffer);

    public abstract int serializedSizeInBytes();

    public long serializedSizeInBytesAsLong() {
        return serializedSizeInBytes();
    }

    public abstract int[] toIntArray();

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseBitmap b = (ClickHouseBitmap) obj;
        return Objects.equals(innerType, b.innerType) && Objects.equals(byteLen, b.byteLen)
                && Objects.equals(reference, b.reference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(innerType, byteLen, reference);
    }

    public long[] toLongArray() {
        int[] ints = toIntArray();
        int len = ints.length;
        long[] longs = new long[len];
        for (int i = 0; i < len; i++) {
            longs[i] = ints[i];
        }
        return longs;
    }

    public ByteBuffer toByteBuffer() {
        ByteBuffer buf;

        int cardinality = getCardinality();
        if (cardinality <= 32) {
            buf = ByteBuffer.allocate(2 + byteLen * cardinality);
            if (buf.order() != ByteOrder.LITTLE_ENDIAN) {
                buf = buf.slice().order(ByteOrder.LITTLE_ENDIAN);
            }
            buf.put((byte) 0);
            buf.put((byte) cardinality);
            if (byteLen == 1) {
                for (int v : toIntArray()) {
                    buf.put((byte) v);
                }
            } else if (byteLen == 2) {
                for (int v : toIntArray()) {
                    buf.putShort((short) v);
                }
            } else if (byteLen == 4) {
                for (int v : toIntArray()) {
                    buf.putInt(v);
                }
            } else { // 64
                for (long v : toLongArray()) {
                    buf.putLong(v);
                }
            }
        } else if (byteLen <= 4) {
            int size = serializedSizeInBytes();
            int varIntSize = Utils.getVarIntSize(size);

            buf = ByteBuffer.allocate(1 + varIntSize + size);
            if (buf.order() != ByteOrder.LITTLE_ENDIAN) {
                buf = buf.slice().order(ByteOrder.LITTLE_ENDIAN);
            }
            buf.put((byte) 1);
            Utils.writeVarInt(size, buf);
            serialize(buf);
        } else { // 64
            // 1) exclude the leading 5 bytes - boolean flag + map size, see below:
            // https://github.com/RoaringBitmap/RoaringBitmap/blob/0.9.9/RoaringBitmap/src/main/java/org/roaringbitmap/longlong/Roaring64NavigableMap.java#L1107
            // 2) not sure what's the extra 8 bytes?
            long size = serializedSizeInBytesAsLong() - 5 + 8;
            int varIntSize = Utils.getVarLongSize(size);
            // TODO add serialize(DataOutput) to handle more
            int intSize = (int) size;
            buf = ByteBuffer.allocate(1 + varIntSize + intSize);
            if (buf.order() != ByteOrder.LITTLE_ENDIAN) {
                buf = buf.slice().order(ByteOrder.LITTLE_ENDIAN);
            }
            buf.put((byte) 1);
            Utils.writeVarInt(intSize, buf);
            buf.putLong(1L); // what's this?
            serialize(buf);
        }

        return (ByteBuffer) ((Buffer) buf).flip();
    }

    public byte[] toBytes() {
        ByteBuffer buffer = toByteBuffer();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    public String toBitmapBuildExpression() {
        StringBuilder sb = new StringBuilder();

        if (byteLen <= 4) {
            for (int v : toIntArray()) {
                sb.append(',').append("to").append(innerType.name()).append('(').append(v).append(')');
            }
        } else {
            for (long v : toLongArray()) {
                sb.append(',').append("to").append(innerType.name()).append('(').append(v).append(')');
            }
        }

        if (sb.length() > 0) {
            sb.deleteCharAt(0).insert(0, '[').append(']');
        } else {
            sb.append("cast([] as Array(").append(innerType.name()).append(')').append(')');
        }

        return sb.insert(0, "bitmapBuild(").append(')').toString();
    }

    public Object unwrap() {
        return this.reference;
    }
}
