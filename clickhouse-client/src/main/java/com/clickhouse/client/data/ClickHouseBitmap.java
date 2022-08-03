package com.clickhouse.client.data;

import java.io.*;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import org.roaringbitmap.BitmapDataProvider;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import com.clickhouse.client.ClickHouseDataType;

public abstract class ClickHouseBitmap {
    private static final int[] EMPTY_INT_ARRAY = new int[0];
    private static final long[] EMPTY_LONG_ARRAY = new long[0];
    private static final ClickHouseBitmap EMPTY_INT8_BITMAP = wrap(ImmutableRoaringBitmap.bitmapOf(EMPTY_INT_ARRAY),
            ClickHouseDataType.Int8);
    private static final ClickHouseBitmap EMPTY_UINT8_BITMAP = wrap(ImmutableRoaringBitmap.bitmapOf(EMPTY_INT_ARRAY),
            ClickHouseDataType.UInt8);
    private static final ClickHouseBitmap EMPTY_INT16_BITMAP = wrap(ImmutableRoaringBitmap.bitmapOf(EMPTY_INT_ARRAY),
            ClickHouseDataType.Int16);
    private static final ClickHouseBitmap EMPTY_UINT16_BITMAP = wrap(ImmutableRoaringBitmap.bitmapOf(EMPTY_INT_ARRAY),
            ClickHouseDataType.UInt16);
    private static final ClickHouseBitmap EMPTY_INT32_BITMAP = wrap(ImmutableRoaringBitmap.bitmapOf(EMPTY_INT_ARRAY),
            ClickHouseDataType.Int32);
    private static final ClickHouseBitmap EMPTY_UINT32_BITMAP = wrap(ImmutableRoaringBitmap.bitmapOf(EMPTY_INT_ARRAY),
            ClickHouseDataType.UInt32);

    static class ClickHouseRoaringBitmap extends ClickHouseBitmap {
        private final RoaringBitmap rb;

        protected ClickHouseRoaringBitmap(RoaringBitmap bitmap, ClickHouseDataType innerType) {
            super(bitmap, innerType);

            this.rb = Objects.requireNonNull(bitmap);
        }

        @Override
        public boolean isEmpty() {
            return rb.isEmpty();
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
        public boolean isEmpty() {
            return rb.isEmpty();
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
        public boolean isEmpty() {
            return rb.isEmpty();
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
        public boolean isEmpty() {
            return rb.isEmpty();
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
            // TODO use custom data output so that we can handle large byte array
            try (ByteArrayOutputStream bas = new ByteArrayOutputStream(size)) {
                DataOutput out = new DataOutputStream(bas);
                try {
                    // https://github.com/RoaringBitmap/RoaringBitmap/blob/0.9.9/RoaringBitmap/src/main/java/org/roaringbitmap/longlong/Roaring64NavigableMap.java#L1105
                    Field signedLongsField = Roaring64NavigableMap.class.getDeclaredField("signedLongs");
                    Field highToBitmapField = Roaring64NavigableMap.class.getDeclaredField("highToBitmap");
                    AccessibleObject.setAccessible(new Field[]{signedLongsField, highToBitmapField}, true);
                    boolean signedLongs = signedLongsField.getBoolean(rb);
                    NavigableMap<Integer, BitmapDataProvider> highToBitmap =
                            (NavigableMap<Integer, BitmapDataProvider>) highToBitmapField.get(rb);
                    out.writeBoolean(signedLongs);
                    out.writeInt(highToBitmap.size());
                    Iterator var2 = highToBitmap.entrySet().iterator();

                    while(var2.hasNext()) {
                        Map.Entry<Integer, BitmapDataProvider> entry = (Map.Entry)var2.next();
                        //here is the different between the implements of java and c
                        Integer v = entry.getKey();
                        out.write((byte)(v >>>  0));
                        out.write((byte)(v >>>  8));
                        out.write((byte)(v >>>  16));
                        out.write((byte)(v >>>  24));
                        ((BitmapDataProvider)entry.getValue()).serialize(out);
                    }
                } catch (IOException e) {
                    throw new IllegalArgumentException("Failed to serialize given bitmap", e);
                } catch (NoSuchFieldException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }

                byte[] bytes = bas.toByteArray();
                for (int i = 4; i > 0; i--) {
                    buffer.put(bytes[i]);
                }
                buffer.putInt(0);
                buffer.put(bytes, 5, size - 5);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to serialize given bitmap", e);
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

    public static ClickHouseBitmap empty() {
        return empty(null);
    }

    public static ClickHouseBitmap empty(ClickHouseDataType type) {
        if (type == null) {
            type = ClickHouseDataType.UInt32;
        }

        ClickHouseBitmap v;
        switch (type) {
            case Int8:
                v = ClickHouseBitmap.EMPTY_INT8_BITMAP;
                break;
            case UInt8:
                v = ClickHouseBitmap.EMPTY_UINT8_BITMAP;
                break;
            case Int16:
                v = ClickHouseBitmap.EMPTY_INT16_BITMAP;
                break;
            case UInt16:
                v = ClickHouseBitmap.EMPTY_UINT16_BITMAP;
                break;
            case Int32:
                v = ClickHouseBitmap.EMPTY_INT32_BITMAP;
                break;
            case UInt32:
                v = ClickHouseBitmap.EMPTY_UINT32_BITMAP;
                break;
            case Int64:
            case UInt64:
                v = wrap(Roaring64NavigableMap.bitmapOf(EMPTY_LONG_ARRAY), type);
                break;
            default:
                throw new IllegalArgumentException(
                        "Only native integer types are supported but we got: " + type.name());
        }
        return v;
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

    public static ClickHouseBitmap deserialize(InputStream in, ClickHouseDataType innerType) throws IOException {
        return deserialize(in instanceof DataInputStream ? (DataInputStream) in : new DataInputStream(in), innerType);
    }

    public static ClickHouseBitmap deserialize(DataInputStream in, ClickHouseDataType innerType) throws IOException {
        final ClickHouseBitmap rb;

        int byteLen = byteLength(innerType);
        int flag = in.readUnsignedByte();
        if (flag == 0) {
            byte cardinality = (byte) in.readUnsignedByte();
            byte[] bytes = new byte[2 + byteLen * cardinality];
            bytes[0] = (byte) flag;
            bytes[1] = cardinality;
            in.read(bytes, 2, bytes.length - 2);

            rb = ClickHouseBitmap.deserialize(bytes, innerType);
        } else {
            int len = BinaryStreamUtils.readVarInt(in);
            byte[] bytes = new byte[len];

            if (byteLen <= 4) {
                in.readFully(bytes);
                RoaringBitmap b = new RoaringBitmap();
                b.deserialize(flip(newBuffer(len).put(bytes)));
                rb = ClickHouseBitmap.wrap(b, innerType);
            } else {
                // TODO implement a wrapper of DataInput to get rid of byte array here
                bytes[0] = (byte) 0; // always unsigned
                // read map size in big-endian byte order
                for (int i = 4; i > 0; i--) {
                    bytes[i] = in.readByte();
                }
                if (in.readByte() != 0 || in.readByte() != 0 || in.readByte() != 0 || in.readByte() != 0) {
                    throw new IllegalStateException(
                            "Not able to deserialize ClickHouseBitmap for too many bitmaps(>" + 0xFFFFFFFFL + ")!");
                }
                // read the rest
                in.readFully(bytes, 5, len - 8);
                Roaring64NavigableMap b = deserializeFromCRoaring(new DataInputStream(new ByteArrayInputStream(bytes)));

                rb = ClickHouseBitmap.wrap(b, innerType);
            }
        }

        return rb;
    }

    public static ClickHouseBitmap deserialize(byte[] bytes, ClickHouseDataType innerType) throws IOException {
        // https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionGroupBitmapData.h#L100
        ClickHouseBitmap rb = ClickHouseBitmap.wrap();

        if (bytes == null || bytes.length == 0) {
            return rb;
        }

        int byteLen = byteLength(innerType);
        ByteBuffer buffer = newBuffer(bytes.length);
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
            int len = BinaryStreamUtils.readVarInt(buffer);
            if (buffer.remaining() < len) {
                throw new IllegalStateException(
                        "Need " + len + " bytes to deserialize ClickHouseBitmap but only got " + buffer.remaining());
            }
            if (byteLen <= 4) {
                RoaringBitmap b = new RoaringBitmap();
                b.deserialize(buffer);
                rb = ClickHouseBitmap.wrap(b, innerType);
            } else {
                // consume map size(long in little-endian byte order)
                byte[] bitmaps = new byte[4];
                buffer.get(bitmaps);
                if (buffer.get() != 0 || buffer.get() != 0 || buffer.get() != 0 || buffer.get() != 0) {
                    throw new IllegalStateException(
                            "Not able to deserialize ClickHouseBitmap for too many bitmaps(>" + 0xFFFFFFFFL + ")!");
                }
                // replace the last 5 bytes to flag(boolean for signed/unsigned) and map
                // size(integer)
                ((Buffer) buffer).position(buffer.position() - 5);
                // always unsigned due to limit of CRoaring
                buffer.put((byte) 0);
                // big-endian -> little-endian
                for (int i = 3; i >= 0; i--) {
                    buffer.put(bitmaps[i]);
                }

                ((Buffer) buffer).position(buffer.position() - 5);
                bitmaps = new byte[buffer.remaining()];
                buffer.get(bitmaps);
                Roaring64NavigableMap b = deserializeFromCRoaring(new DataInputStream(new ByteArrayInputStream(bitmaps)));
                rb = ClickHouseBitmap.wrap(b, innerType);
            }
        }

        return rb;
    }

    public static Roaring64NavigableMap deserializeFromCRoaring(DataInput in) throws IOException {
        Roaring64NavigableMap rb = new Roaring64NavigableMap();
        try {
            rb.clear();
            Field signedLongsField = Roaring64NavigableMap.class.getDeclaredField("signedLongs");
            Field highToBitmapField = Roaring64NavigableMap.class.getDeclaredField("highToBitmap");
            Method newRoaringBitmapMethod = Roaring64NavigableMap.class.getDeclaredMethod("newRoaringBitmap");
            Method resetPerfHelpersMethod = Roaring64NavigableMap.class.getDeclaredMethod("resetPerfHelpers");
            Class roaringIntPackingClass = Class.forName("org.roaringbitmap.longlong.RoaringIntPacking");
            Method unsignedComparatorMethod = roaringIntPackingClass.getDeclaredMethod("unsignedComparator");
            AccessibleObject.setAccessible(new AccessibleObject[]{
                    signedLongsField,unsignedComparatorMethod,
                    highToBitmapField,
                    newRoaringBitmapMethod, resetPerfHelpersMethod}, true);

            NavigableMap<Integer, BitmapDataProvider> highToBitmap = null;
            boolean signedLongs = in.readBoolean();

            signedLongsField.setBoolean(rb, signedLongs);

            int nbHighs = in.readInt();
            if (signedLongs) {
                highToBitmap = new TreeMap();
            } else {
                highToBitmap = new TreeMap((Comparator) unsignedComparatorMethod.invoke(null));
            }
            highToBitmapField.set(rb, highToBitmap);
            for (int i = 0; i < nbHighs; ++i) {
                byte[] bytes = new byte[4];
                in.readFully(bytes);
                if ((bytes[0] | bytes[1] | bytes[2] | bytes[3]) < 0)
                    throw new EOFException();
                int high = ((bytes[3] << 24) + (bytes[2] << 16) + (bytes[1] << 8) + (bytes[0] << 0));
                BitmapDataProvider provider = (BitmapDataProvider) newRoaringBitmapMethod.invoke(rb);

                if (provider instanceof RoaringBitmap) {
                    ((RoaringBitmap) provider).deserialize(in);
                } else {
                    if (!(provider instanceof MutableRoaringBitmap)) {
                        throw new UnsupportedEncodingException("Cannot deserialize a " + provider.getClass());
                    }

                    ((MutableRoaringBitmap) provider).deserialize(in);
                }

                highToBitmap.put(high, provider);
            }
            resetPerfHelpersMethod.invoke(rb);

        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
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
        int byteLen;
        switch (Objects.requireNonNull(type)) {
            case Int8:
            case UInt8:
            case Int16:
            case UInt16:
            case Int32:
            case UInt32:
            case Int64:
            case UInt64:
                byteLen = type.getByteLength();
                break;
            default:
                throw new IllegalArgumentException(
                        "Only native integer types are supported but we got: " + type.name());
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

    public abstract boolean isEmpty();

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

    /**
     * Serialize the bitmap into a flipped ByteBuffer.
     *
     * @return flipped byte buffer
     */
    public ByteBuffer toByteBuffer() {
        ByteBuffer buf;

        int cardinality = getCardinality();
        if (cardinality <= 32) {
            buf = newBuffer(2 + byteLen * cardinality);
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
            int varIntSize = BinaryStreamUtils.getVarIntSize(size);

            buf = newBuffer(1 + varIntSize + size);
            buf.put((byte) 1);
            BinaryStreamUtils.writeVarInt(buf, size);
            serialize(buf);
        } else { // 64
            // 1) deduct one to exclude the leading byte - boolean flag, see below:
            // https://github.com/RoaringBitmap/RoaringBitmap/blob/0.9.9/RoaringBitmap/src/main/java/org/roaringbitmap/longlong/Roaring64NavigableMap.java#L1107
            // 2) add 4 bytes because CRoaring uses long to store count of 32-bit bitmaps,
            // while Java uses int - see
            // https://github.com/RoaringBitmap/CRoaring/blob/v0.2.66/cpp/roaring64map.hh#L597
            long size = serializedSizeInBytesAsLong() - 1 + 4;
            int varIntSize = BinaryStreamUtils.getVarLongSize(size);
            // TODO add serialize(DataOutput) to handle more
            int intSize = (int) size;
            buf = newBuffer(1 + varIntSize + intSize);
            buf.put((byte) 1);
            BinaryStreamUtils.writeVarInt(buf, intSize);
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
