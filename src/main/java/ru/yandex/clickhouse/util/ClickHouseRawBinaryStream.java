package ru.yandex.clickhouse.util;

import com.google.common.base.Preconditions;
import com.google.common.io.LittleEndianDataOutputStream;
import com.google.common.primitives.UnsignedLong;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.LocalDate;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class ClickHouseRawBinaryStream {
    private final LittleEndianDataOutputStream out;
    private final DateTimeZone dateTimeZone;

    private static final int U_INT8_MAX = (1 << 8) - 1;
    private static final int U_INT16_MAX = (1 << 16) - 1;
    private static final long U_INT32_MAX = (1L << 32) - 1;
    private static final DateTime EPOCH_DATE_TIME = new DateTime(0);
    private static final LocalDate EPOCH_DATE = new LocalDate(0);

    public ClickHouseRawBinaryStream(OutputStream outputStream, TimeZone timeZone) {
        this.out = new LittleEndianDataOutputStream(outputStream);
        this.dateTimeZone = DateTimeZone.forTimeZone(timeZone);
    }

    public void writeUnsignedLeb128(int value) throws IOException {
        int remaining = value >>> 7;
        while (remaining != 0) {
            out.write((byte) ((value & 0x7f) | 0x80));
            value = remaining;
            remaining >>>= 7;
        }
        out.write((byte) (value & 0x7f));
    }

    public void writeString(String string) throws IOException {
        Preconditions.checkNotNull(string);
        writeUnsignedLeb128(string.length());
        out.write(string.getBytes());
    }

    private void validateInt(int value, int minValue, int maxValue, String dataType) {
        if (value < minValue && value > maxValue) {
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
        if (value < 0 && value > U_INT32_MAX) {
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
        int daysSinceEpoch = Days.daysBetween(EPOCH_DATE, date).getDays();
        writeUInt16(daysSinceEpoch);
    }

    public void writeDate(Date date) throws IOException {
        Preconditions.checkNotNull(date);
        DateTime dateTime = new DateTime(date.getTime(), dateTimeZone);
        int daysSinceEpoch = Days.daysBetween(EPOCH_DATE_TIME, dateTime).getDays();
        writeUInt16(daysSinceEpoch);
    }

    public void writeFloat32(float value) throws IOException {
        out.writeFloat(value);
    }

    public void writeFloat64(double value) throws IOException {
        out.writeDouble(value);
    }


}
