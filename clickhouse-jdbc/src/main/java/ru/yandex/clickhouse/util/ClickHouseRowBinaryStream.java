package ru.yandex.clickhouse.util;

import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class ClickHouseRowBinaryStream {

    private static final int U_INT8_MAX = (1 << 8) - 1;
    private static final int U_INT16_MAX = (1 << 16) - 1;
    private static final long U_INT32_MAX = (1L << 32) - 1;
    protected static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    private final DataOutputStream out;
    private final TimeZone timeZone;

    public ClickHouseRowBinaryStream(OutputStream outputStream, TimeZone timeZone, ClickHouseProperties properties) {
        this.out = new DataOutputStream(outputStream);
        if (properties.isUseServerTimeZoneForDates()) {
            this.timeZone = timeZone;
        } else {
            this.timeZone = TimeZone.getDefault();
        }
    }

    public void writeUnsignedLeb128(int value) throws IOException {
        Utils.checkArgument(value, 0);

        int remaining = value >>> 7;
        while (remaining != 0) {
            out.write((byte) ((value & 0x7f) | 0x80));
            value = remaining;
            remaining >>>= 7;
        }
        out.write((byte) (value & 0x7f));
    }

    /**
     * Dangerous. Can only be used for rare optimizations, for example when the string is written in parts
     * without prior concatenation. The size of the string in bytes must be passed through writeUnsignedLeb128.
     *
     * @param bytes byte array will be written into stream
     * @throws IOException in case if an I/O error occurs
     */
    public void writeBytes(byte[] bytes) throws IOException {
        out.write(bytes);
    }

    public void writeByteBuffer(ByteBuffer buffer) throws IOException {
        Channels.newChannel(out).write(buffer);
    }

    /**
     * Dangerous. Can only be used for rare optimizations, for example when the string is written in parts
     * without prior concatenation. The size of the string in bytes must be passed through writeUnsignedLeb128.
     *
     * @param b byte value
     * @throws IOException in case if an I/O error occurs
     */
    public void writeByte(byte b) throws IOException {
        out.write(b);
    }

    public void writeString(String string) throws IOException {
        byte[] bytes = Objects.requireNonNull(string).getBytes(StandardCharsets.UTF_8);
        writeUnsignedLeb128(bytes.length);
        out.write(bytes);
    }

    public void writeUInt8(boolean value) throws IOException {
        out.writeByte(value ? 1 : 0);
    }

    public void writeUInt8(int value) throws IOException {
        Utils.checkArgument(value, 0, U_INT8_MAX);
        byte unsigned = (byte) (value & 0xffL);
        out.writeByte(unsigned);
    }

    public void writeInt8(int value) throws IOException {
        Utils.checkArgument(value, Byte.MIN_VALUE, Byte.MAX_VALUE);
        out.writeByte(value);
    }

    public void writeInt8(byte value) throws IOException {
        out.writeByte(value);
    }

    public void writeInt16(int value) throws IOException {
        Utils.checkArgument(value, Short.MIN_VALUE, Short.MAX_VALUE);
        Utils.writeShort(out, value);
    }

    public void writeInt16(short value) throws IOException {
        Utils.writeShort(out, value);
    }

    public void writeUInt16(int value) throws IOException {
        Utils.checkArgument(value, 0, U_INT16_MAX);
        short unsigned = (short) (value & 0xffffL);
        Utils.writeShort(out, unsigned);
    }

    public void writeInt32(int value) throws IOException {
        Utils.writeInt(out, value);
    }

    public void writeUInt32(long value) throws IOException {
        Utils.checkArgument(value, 0, U_INT32_MAX);
        int unsigned = (int) (value & 0xffffffffL);
        Utils.writeInt(out, unsigned);
    }

    public void writeInt64(long value) throws IOException {
        Utils.writeLong(out, value);
    }

    public void writeUInt64(long value) throws IOException {
        Utils.writeLong(out, value);
    }

    public void writeUInt64(BigInteger value) throws IOException {
        Utils.checkArgument(value, BigInteger.ZERO);
        Utils.writeLong(out, value.longValue());
    }

    public void writeInt128(BigInteger value) throws IOException {
        Utils.writeBigInteger(out, value, 16);
    }

    public void writeUInt128(BigInteger value) throws IOException {
        Utils.checkArgument(value, BigInteger.ZERO);
        Utils.writeBigInteger(out, value, 16);
    }

    public void writeInt256(BigInteger value) throws IOException {
        Utils.writeBigInteger(out, value, 32);
    }

    public void writeUInt256(BigInteger value) throws IOException {
        Utils.checkArgument(value, BigInteger.ZERO);
        Utils.writeBigInteger(out, value, 32);
    }

    public void writeDateTime(Date date) throws IOException {
        Objects.requireNonNull(date);
        writeUInt32(TimeUnit.MILLISECONDS.toSeconds(date.getTime()));
    }

    public void writeDate(Date date) throws IOException {
        Objects.requireNonNull(date);
        long localMillis = date.getTime() + timeZone.getOffset(date.getTime());
        int daysSinceEpoch = (int) (localMillis / MILLIS_IN_DAY);
        writeUInt16(daysSinceEpoch);
    }

    public void writeFloat32(float value) throws IOException {
        Utils.writeInt(out, Float.floatToIntBits(value));
    }

    public void writeFloat64(double value) throws IOException {
        Utils.writeLong(out, Double.doubleToLongBits(value));
    }

    public void writeDecimal128(BigDecimal num, int scale) throws IOException {
        BigInteger bi = Utils.toBigInteger(num, scale);
        byte[] r = bi.toByteArray();
        for (int i = r.length; i > 0; i--) {
            out.write(r[i - 1]);
        }
        out.write(new byte[16 - r.length]);
    }

    public void writeDecimal256(BigDecimal num, int scale) throws IOException {
        BigInteger bi = Utils.toBigInteger(num, scale);
        byte[] r = bi.toByteArray();
        for (int i = r.length; i > 0; i--) {
            out.write(r[i - 1]);
        }
        out.write(new byte[32 - r.length]);
    }

    public void writeDecimal64(BigDecimal num, int scale) throws IOException {
        Utils.writeLong(out, Utils.toBigInteger(num, scale).longValue());
    }

    public void writeDecimal32(BigDecimal num, int scale) throws IOException {
        Utils.writeInt(out, Utils.toBigInteger(num, scale).intValue());
    }

    public void writeDateArray(Date[] dates) throws IOException {
        writeUnsignedLeb128(Objects.requireNonNull(dates).length);
        for (Date date : dates) {
            writeDate(date);
        }
    }

    public void writeDateTimeArray(Date[] dates) throws IOException {
        writeUnsignedLeb128(Objects.requireNonNull(dates).length);
        for (Date date : dates) {
            writeDateTime(date);
        }
    }

    public void writeStringArray(String[] strings) throws IOException {
        writeUnsignedLeb128(Objects.requireNonNull(strings).length);
        for (String el : strings) {
            writeString(el);
        }
    }

    public void writeInt8Array(byte[] bytes) throws IOException {
        writeUnsignedLeb128(Objects.requireNonNull(bytes).length);
        for (byte b : bytes) {
            writeInt8(b);
        }
    }

    public void writeInt8Array(int[] ints) throws IOException {
        writeUnsignedLeb128(Objects.requireNonNull(ints).length);
        for (int i : ints) {
            writeInt8(i);
        }
    }

    public void writeUInt8Array(int[] ints) throws IOException {
        writeUnsignedLeb128(Objects.requireNonNull(ints).length);
        for (int i : ints) {
            writeUInt8(i);
        }
    }

    public void writeInt16Array(short[] shorts) throws IOException {
        writeUnsignedLeb128(Objects.requireNonNull(shorts).length);
        for (short s : shorts) {
            writeInt16(s);
        }
    }

    public void writeUInt16Array(int[] ints) throws IOException {
        writeUnsignedLeb128(Objects.requireNonNull(ints).length);
        for (int i : ints) {
            writeUInt16(i);
        }
    }

    public void writeInt32Array(int[] ints) throws IOException {
        writeUnsignedLeb128(Objects.requireNonNull(ints).length);
        for (int i : ints) {
            writeInt32(i);
        }
    }

    public void writeUInt32Array(long[] longs) throws IOException {
        writeUnsignedLeb128(Objects.requireNonNull(longs).length);
        for (long l : longs) {
            writeUInt32(l);
        }
    }

    public void writeInt64Array(long[] longs) throws IOException {
        writeUnsignedLeb128(Objects.requireNonNull(longs).length);
        for (long l : longs) {
            writeInt64(l);
        }
    }

    public void writeUInt64Array(long[] longs) throws IOException {
        writeUnsignedLeb128(Objects.requireNonNull(longs).length);
        for (long l : longs) {
            writeUInt64(l);
        }
    }

    public void writeUInt64Array(BigInteger[] longs) throws IOException {
        writeUnsignedLeb128(Objects.requireNonNull(longs).length);
        for (BigInteger l : longs) {
            writeUInt64(l);
        }
    }

    public void writeFloat32Array(float[] floats) throws IOException {
        writeUnsignedLeb128(Objects.requireNonNull(floats).length);
        for (float f : floats) {
            writeFloat32(f);
        }
    }

    public void writeFloat64Array(double[] doubles) throws IOException {
        writeUnsignedLeb128(Objects.requireNonNull(doubles).length);
        for (double d : doubles) {
            writeFloat64(d);
        }
    }

    /**
     * Write a marker indicating if value is nullable or not.
     * <p>
     * E.g., to write Nullable(Int32):
     *
     * <pre>
     *     void writeNullableInt32(Integer value) {
     *         if (value == null) {
     *             markNextNullable(true);
     *         } else {
     *             markNextNullable(false);
     *             writeInt32(value);
     *         }
     *     }
     * </pre>
     *
     * @param isNullable if it's true, 1 will be written otherwise 0
     * @throws IOException in case if an I/O error occurs
     */
    public void markNextNullable(boolean isNullable) throws IOException {
        writeByte(isNullable ? (byte) 1 : (byte) 0);
    }

    public void writeUUID(UUID uuid) throws IOException {
        Objects.requireNonNull(uuid);
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]).order(ByteOrder.LITTLE_ENDIAN);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        byte[] array = bb.array();
        this.writeBytes(array);
    }

    public void writeUUIDArray(UUID[] uuids) throws IOException {
        writeUnsignedLeb128(Objects.requireNonNull(uuids).length);
        for (UUID uuid : uuids) {
            writeUUID(uuid);
        }
    }

    public void writeBitmap(ClickHouseBitmap rb) throws IOException {
        this.writeByteBuffer(Objects.requireNonNull(rb).toByteBuffer());
    }
}
