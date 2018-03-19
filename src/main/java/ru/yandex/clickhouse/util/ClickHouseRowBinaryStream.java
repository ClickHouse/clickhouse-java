package ru.yandex.clickhouse.util;

import com.google.common.base.Preconditions;
import com.google.common.io.LittleEndianDataOutputStream;
import com.google.common.primitives.UnsignedLong;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class ClickHouseRowBinaryStream {

    private static final int U_INT8_MAX = (1 << 8) - 1;
    private static final int U_INT16_MAX = (1 << 16) - 1;
    private static final long U_INT32_MAX = (1L << 32) - 1;

    private final LittleEndianDataOutputStream out;
    private final DateTimeZone dateTimeZone;
    private final LocalDate epochDate;

    public ClickHouseRowBinaryStream(OutputStream outputStream, TimeZone timeZone, ClickHouseProperties properties) {
        this.out = new LittleEndianDataOutputStream(outputStream);
        if (properties.isUseServerTimeZoneForDates()) {
            this.dateTimeZone = DateTimeZone.forTimeZone(timeZone);
        } else {
            this.dateTimeZone = DateTimeZone.getDefault();
        }
        this.epochDate = new LocalDate(0, dateTimeZone);
    }

    public void writeUnsignedLeb128(int value) throws IOException {
        Preconditions.checkArgument(value >= 0);
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
     * @param bytes
     * @throws IOException
     */
    public void writeBytes(byte[] bytes) throws IOException {
        out.write(bytes);
    }

    /**
     * Dangerous. Can only be used for rare optimizations, for example when the string is written in parts
     * without prior concatenation. The size of the string in bytes must be passed through writeUnsignedLeb128.
     * @param b
     * @throws IOException
     */
    public void writeByte(byte b) throws IOException {
        out.write(b);
    }

    public void writeString(String string) throws IOException {
        Preconditions.checkNotNull(string);
        byte[] bytes = string.getBytes("UTF-8");
        writeUnsignedLeb128(bytes.length);
        out.write(bytes);
    }

    private void validateInt(int value, int minValue, int maxValue, String dataType) {
        if (value < minValue || value > maxValue) {
            throw new IllegalStateException("Not a " + dataType + " value: " + value);
        }
    }

    public void writeUInt8(boolean value) throws IOException {
        out.writeByte(value ? 1 : 0);
    }

    public void writeUInt8(int value) throws IOException {
        validateInt(value, 0, U_INT8_MAX, "UInt8");
        byte unsigned = (byte) (value & 0xffL);
        out.writeByte(unsigned);
    }

    public void writeInt8(int value) throws IOException {
        validateInt(value, Byte.MIN_VALUE, Byte.MAX_VALUE, "Int8");
        out.writeByte(value);
    }

    public void writeInt8(byte value) throws IOException {
        out.writeByte(value);
    }

    public void writeInt16(int value) throws IOException {
        validateInt(value, Short.MIN_VALUE, Short.MAX_VALUE, "Int6");
        out.writeShort(value);
    }

    public void writeInt16(short value) throws IOException {
        out.writeShort(value);
    }

    public void writeUInt16(int value) throws IOException {
        validateInt(value, 0, U_INT16_MAX, "UInt16");
        short unsigned = (short) (value & 0xffffL);
        out.writeShort(unsigned);
    }

    public void writeInt32(int value) throws IOException {
        out.writeInt(value);
    }

    public void writeUInt32(long value) throws IOException {
        if (value < 0 || value > U_INT32_MAX) {
            throw new IllegalStateException("Not a UInt32 value: " + value);
        }
        int unsigned = (int) (value & 0xffffffffL);
        out.writeInt(unsigned);
    }

    public void writeInt64(long value) throws IOException {
        out.writeLong(value);
    }

    public void writeUInt64(long value) throws IOException {
        if (value < 0) {
            throw new IllegalStateException("Not a UInt64 value: " + value);
        }
        out.writeLong(value);
    }

    public void writeUInt64(UnsignedLong value) throws IOException {
        out.writeLong(value.longValue());
    }

    public void writeDateTime(Date date) throws IOException {
        Preconditions.checkNotNull(date);
        writeUInt32(TimeUnit.MILLISECONDS.toSeconds(date.getTime()));
    }

    public void writeDate(LocalDate date) throws IOException {
        Preconditions.checkNotNull(date);
        int daysSinceEpoch = Days.daysBetween(epochDate, date).getDays();
        writeUInt16(daysSinceEpoch);
    }

    public void writeDate(Date date) throws IOException {
        Preconditions.checkNotNull(date);
        LocalDate localDate = new LocalDate(date.getTime(), dateTimeZone);
        int daysSinceEpoch = Days.daysBetween(epochDate, localDate).getDays();
        writeUInt16(daysSinceEpoch);
    }

    public void writeFloat32(float value) throws IOException {
        out.writeFloat(value);
    }

    public void writeFloat64(double value) throws IOException {
        out.writeDouble(value);
    }


    public void writeDateArray(Date[] dates) throws IOException {
        Preconditions.checkNotNull(dates);
        writeUnsignedLeb128(dates.length);
        for (Date date: dates) {
            writeDate(date);
        }
    }

    public void writeDateTimeArray(Date[] dates) throws IOException {
        Preconditions.checkNotNull(dates);
        writeUnsignedLeb128(dates.length);
        for (Date date: dates) {
            writeDateTime(date);
        }
    }

    public void writeStringArray(String[] strings) throws IOException {
        Preconditions.checkNotNull(strings);
        writeUnsignedLeb128(strings.length);
        for (String el : strings) {
            writeString(el);
        }
    }

    public void writeInt8Array(byte[] bytes) throws IOException {
        Preconditions.checkNotNull(bytes);
        writeUnsignedLeb128(bytes.length);
        for (byte b: bytes) {
            writeInt8(b);
        }
    }
    public void writeInt8Array(int[] ints) throws IOException {
        Preconditions.checkNotNull(ints);
        writeUnsignedLeb128(ints.length);
        for (int i: ints) {
            writeInt8(i);
        }
    }

    public void writeUInt8Array(int[] ints) throws IOException {
        Preconditions.checkNotNull(ints);
        writeUnsignedLeb128(ints.length);
        for (int i: ints) {
            writeUInt8(i);
        }
    }

    public void writeInt16Array(short[] shorts) throws IOException {
        Preconditions.checkNotNull(shorts);
        writeUnsignedLeb128(shorts.length);
        for (short s: shorts) {
            writeInt16(s);
        }
    }

    public void writeUInt16Array(int[] ints) throws IOException {
        Preconditions.checkNotNull(ints);
        writeUnsignedLeb128(ints.length);
        for (int i: ints) {
            writeUInt16(i);
        }
    }

    public void writeInt32Array(int[] ints) throws IOException {
        Preconditions.checkNotNull(ints);
        writeUnsignedLeb128(ints.length);
        for (int i: ints) {
            writeInt32(i);
        }
    }

    public void writeUInt32Array(long[] longs) throws IOException {
        Preconditions.checkNotNull(longs);
        writeUnsignedLeb128(longs.length);
        for (long l: longs) {
            writeUInt32(l);
        }
    }

    public void writeInt64Array(long[] longs) throws IOException {
        Preconditions.checkNotNull(longs);
        writeUnsignedLeb128(longs.length);
        for (long l: longs) {
            writeInt64(l);
        }
    }

    public void writeUInt64Array(long[] longs) throws IOException {
        Preconditions.checkNotNull(longs);
        writeUnsignedLeb128(longs.length);
        for (long l: longs) {
            writeUInt64(l);
        }
    }

    public void writeFloat32Array(float[] floats) throws IOException {
        Preconditions.checkNotNull(floats);
        writeUnsignedLeb128(floats.length);
        for (float f: floats) {
            writeFloat32(f);
        }
    }

    public void writeFloat64Array(double[] doubles) throws IOException {
        Preconditions.checkNotNull(doubles);
        writeUnsignedLeb128(doubles.length);
        for (double d: doubles) {
            writeFloat64(d);
        }
    }
}
