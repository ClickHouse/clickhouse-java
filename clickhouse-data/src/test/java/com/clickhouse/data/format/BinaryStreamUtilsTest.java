package com.clickhouse.data.format;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.TimeZone;
import java.util.UUID;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.data.value.ClickHouseBitmap;
import com.clickhouse.data.value.WriterFunction;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BinaryStreamUtilsTest {
    @DataProvider(name = "timeZoneProvider")
    private Object[][] getTimeZones() {
        return new Object[][] { new String[] { "Asia/Chongqing" }, new String[] { "America/Los_Angeles" },
                new String[] { "Europe/Moscow" }, new String[] { "Etc/UTC" },
                new String[] { "Europe/Berlin" } };
    }

    public static byte[] generateBytes(int... ints) {
        byte[] bytes = new byte[ints.length];
        for (int i = 0; i < ints.length; i++) {
            bytes[i] = (byte) (0xFF & ints[i]);
        }
        return bytes;
    }

    public static ClickHouseInputStream generateInput(byte... values) {
        return ClickHouseInputStream.of(new ByteArrayInputStream(values));
    }

    public static ClickHouseInputStream generateInput(int... values) {
        return ClickHouseInputStream.of(new ByteArrayInputStream(generateBytes(values)));
    }

    public static byte[][] generateBytes(int byteLength, boolean unsigned) {
        if (byteLength < 1) {
            throw new IllegalArgumentException("byteLength must be greater than zero");
        }

        byte[][] bytes = new byte[byteLength + 1 + (unsigned ? 0 : 2)][byteLength];
        for (int i = 0; i <= byteLength; i++) {
            byte[] arr = new byte[byteLength];
            if (i == 0) {
                for (int j = 0; j < byteLength; j++) {
                    arr[j] = 0;
                }
                continue;
            }

            for (int j = 0; j < i; j++) {
                arr[j] = (byte) 0xFF;
            }

            for (int j = i; j < byteLength; j++) {
                arr[j] = 0;
            }

            bytes[i] = arr;
        }

        if (!unsigned) {
            // min/max values
            byte[] values = new byte[] { (byte) 0x80, (byte) 0x7F };
            for (int i = 0; i < values.length; i++) {
                byte[] arr = new byte[byteLength];
                for (int j = 0; j < byteLength - 1; j++) {
                    arr[j] = (byte) 0xFF;
                }
                arr[byteLength - 1] = values[i];
                bytes[bytes.length - 1 - i] = arr;
            }
        }

        return bytes;
    }

    public static byte[] getWrittenBytes(WriterFunction writter) throws IOException {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            writter.write(output);
            return output.toByteArray();
        }
    }

    public static int[] newBitmapValues(ClickHouseDataType t, int length) {
        int[] array = new int[length];
        int base = (t.getByteLength() - 1) * 0xFF;
        if (t.isSigned()) {
            for (int i = 0; i < length; i++) {
                if (i % 2 == 0) {
                    array[i / 2] = base + i;
                } else {
                    array[length - i / 2 - 1] = (base + i) * -1;
                }
            }
        } else {
            for (int i = 0; i < length; i++) {
                array[i] = base + i;
            }
        }

        return array;
    }

    public static long[] newBitmap64Values(ClickHouseDataType t, int length) {
        long[] array = new long[length];
        long base = 0xFFFFFFFFL;
        if (t.isSigned()) {
            for (int i = 0; i < length; i++) {
                if (i % 2 == 0) {
                    array[i / 2] = base + i;
                } else {
                    array[length - i / 2 - 1] = (base + i) * -1;
                }
            }
        } else {
            for (int i = 0; i < length; i++) {
                array[i] = base + i;
            }
        }

        return array;
    }

    @Test(groups = { "unit" })
    public void testReverse() {
        byte[] bytes = null;
        Assert.assertEquals(BinaryStreamUtils.reverse(bytes), bytes);
        Assert.assertEquals(BinaryStreamUtils.reverse(bytes = new byte[0]), bytes);
        Assert.assertEquals(BinaryStreamUtils.reverse(bytes = new byte[] { 1 }), bytes);
        Assert.assertEquals(BinaryStreamUtils.reverse(bytes = new byte[] { 1, 2 }), new byte[] { 2, 1 });
        Assert.assertEquals(BinaryStreamUtils.reverse(bytes = new byte[] { 1, 2, 3 }), new byte[] { 3, 2, 1 });
    }

    @Test(groups = { "unit" })
    public void testReadBoolean() throws IOException {
        Assert.assertEquals(BinaryStreamUtils.readBoolean(generateInput(1)), true);
        Assert.assertEquals(BinaryStreamUtils.readBoolean(generateInput(0)), false);

        Assert.assertThrows(IllegalArgumentException.class,
                () -> BinaryStreamUtils.readBoolean(generateInput(2)));
    }

    @Test(groups = { "unit" })
    public void testWriteBoolean() throws IOException {
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeBoolean(o, true)), generateBytes(1));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeBoolean(o, false)), generateBytes(0));
    }

    @Test(groups = { "unit" })
    public void testReadByte() throws IOException {
        for (int b = Byte.MIN_VALUE; b <= Byte.MAX_VALUE; b++) {
            Assert.assertEquals(generateInput(b).readByte(), b);
            Assert.assertEquals(BinaryStreamUtils.readInt8(generateInput(b)), b);
        }
    }

    @Test(groups = { "unit" })
    public void testWriteByte() throws IOException {
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt8(o, (byte) 0)), generateBytes(0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt8(o, (byte) 1)), generateBytes(1));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt8(o, (byte) -1)), generateBytes(-1));

        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt8(o, 0)), generateBytes(0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt8(o, 1)), generateBytes(1));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt8(o, -1)), generateBytes(-1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeInt8(o, Byte.MAX_VALUE + 1)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeInt8(o, Byte.MIN_VALUE - 1)));
    }

    @Test(groups = { "unit" })
    public void testWriteEmptyBitmap() throws IOException {
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeBitmap(o, ClickHouseBitmap.empty())),
                generateBytes(0, 0));

        for (ClickHouseDataType t : new ClickHouseDataType[] { ClickHouseDataType.Int8,
                ClickHouseDataType.UInt8,
                ClickHouseDataType.Int16, ClickHouseDataType.UInt16, ClickHouseDataType.Int32,
                ClickHouseDataType.UInt32, ClickHouseDataType.Int64, ClickHouseDataType.UInt64 }) {
            Assert.assertEquals(getWrittenBytes(
                    o -> BinaryStreamUtils.writeBitmap(o, ClickHouseBitmap.empty(t))),
                    generateBytes(0, 0));
        }
    }

    @Test(groups = { "unit" })
    public void testWriteBitmap32() throws IOException {
        // cardinality <= 32
        for (ClickHouseDataType t : new ClickHouseDataType[] { ClickHouseDataType.Int8,
                ClickHouseDataType.UInt8,
                ClickHouseDataType.Int16, ClickHouseDataType.UInt16, ClickHouseDataType.Int32,
                ClickHouseDataType.UInt32 }) {
            int[] values = newBitmapValues(t, 32);
            byte[] expected = getWrittenBytes(o -> {
                BinaryStreamUtils.writeInt8(o, 0);
                BinaryStreamUtils.writeInt8(o, 0x20);
                for (int v : values) {
                    int len = t.getByteLength();
                    if (len == 1) {
                        BinaryStreamUtils.writeInt8(o, (byte) v);
                    } else if (len == 2) {
                        BinaryStreamUtils.writeInt16(o, (short) v);
                    } else { // 4
                        BinaryStreamUtils.writeInt32(o, v);
                    }
                }
            });
            for (Object bitmap : new Object[] { RoaringBitmap.bitmapOf(values),
                    MutableRoaringBitmap.bitmapOf(values),
                    ImmutableRoaringBitmap.bitmapOf(values) }) {
                byte[] actual = getWrittenBytes(
                        o -> BinaryStreamUtils.writeBitmap(o,
                                ClickHouseBitmap.wrap(bitmap, t)));
                Assert.assertEquals(actual, expected);
            }
        }

        // cardinality > 32
        int i = 0;
        for (ClickHouseDataType t : new ClickHouseDataType[] { ClickHouseDataType.Int8,
                ClickHouseDataType.UInt8,
                ClickHouseDataType.Int16, ClickHouseDataType.UInt16, ClickHouseDataType.Int32,
                ClickHouseDataType.UInt32 }) {
            int[] values = newBitmapValues(t, 33);
            byte[][] expected = new byte[][] {
                    generateBytes(0x01, 0x5A, 0x3A, 0x30, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
                            0x00, 0x10, 0x00,
                            0xFF, 0xFF, 0x0F, 0x00, 0x18, 0x00, 0x00, 0x00, 0x3A, 0x00,
                            0x00, 0x00, 0x00, 0x00, 0x02,
                            0x00, 0x04, 0x00, 0x06, 0x00, 0x08, 0x00, 0x0A, 0x00, 0x0C,
                            0x00, 0x0E, 0x00, 0x10, 0x00,
                            0x12, 0x00, 0x14, 0x00, 0x16, 0x00, 0x18, 0x00, 0x1A, 0x00,
                            0x1C, 0x00, 0x1E, 0x00, 0x20,
                            0x00, 0xE1, 0xFF, 0xE3, 0xFF, 0xE5, 0xFF, 0xE7, 0xFF, 0xE9,
                            0xFF, 0xEB, 0xFF, 0xED, 0xFF,
                            0xEF, 0xFF, 0xF1, 0xFF, 0xF3, 0xFF, 0xF5, 0xFF, 0xF7, 0xFF,
                            0xF9, 0xFF, 0xFB, 0xFF, 0xFD,
                            0xFF, 0xFF, 0xFF),
                    generateBytes(0x01, 0x52, 0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
                            0x00, 0x20, 0x00,
                            0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00,
                            0x03, 0x00, 0x04, 0x00, 0x05,
                            0x00, 0x06, 0x00, 0x07, 0x00, 0x08, 0x00, 0x09, 0x00, 0x0A,
                            0x00, 0x0B, 0x00, 0x0C, 0x00,
                            0x0D, 0x00, 0x0E, 0x00, 0x0F, 0x00, 0x10, 0x00, 0x11, 0x00,
                            0x12, 0x00, 0x13, 0x00, 0x14,
                            0x00, 0x15, 0x00, 0x16, 0x00, 0x17, 0x00, 0x18, 0x00, 0x19,
                            0x00, 0x1A, 0x00, 0x1B, 0x00,
                            0x1C, 0x00, 0x1D, 0x00, 0x1E, 0x00, 0x1F, 0x00, 0x20, 0x00),
                    generateBytes(0x01, 0x5A, 0x3A, 0x30, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
                            0x00, 0x10, 0x00,
                            0xFF, 0xFF, 0x0F, 0x00, 0x18, 0x00, 0x00, 0x00, 0x3A, 0x00,
                            0x00, 0x00, 0xFF, 0x00, 0x01,
                            0x01, 0x03, 0x01, 0x05, 0x01, 0x07, 0x01, 0x09, 0x01, 0x0B,
                            0x01, 0x0D, 0x01, 0x0F, 0x01,
                            0x11, 0x01, 0x13, 0x01, 0x15, 0x01, 0x17, 0x01, 0x19, 0x01,
                            0x1B, 0x01, 0x1D, 0x01, 0x1F,
                            0x01, 0xE2, 0xFE, 0xE4, 0xFE, 0xE6, 0xFE, 0xE8, 0xFE, 0xEA,
                            0xFE, 0xEC, 0xFE, 0xEE, 0xFE,
                            0xF0, 0xFE, 0xF2, 0xFE, 0xF4, 0xFE, 0xF6, 0xFE, 0xF8, 0xFE,
                            0xFA, 0xFE, 0xFC, 0xFE, 0xFE,
                            0xFE, 0x00, 0xFF),
                    generateBytes(0x01, 0x52, 0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
                            0x00, 0x20, 0x00,
                            0x10, 0x00, 0x00, 0x00, 0xFF, 0x00, 0x00, 0x01, 0x01, 0x01,
                            0x02, 0x01, 0x03, 0x01, 0x04,
                            0x01, 0x05, 0x01, 0x06, 0x01, 0x07, 0x01, 0x08, 0x01, 0x09,
                            0x01, 0x0A, 0x01, 0x0B, 0x01,
                            0x0C, 0x01, 0x0D, 0x01, 0x0E, 0x01, 0x0F, 0x01, 0x10, 0x01,
                            0x11, 0x01, 0x12, 0x01, 0x13,
                            0x01, 0x14, 0x01, 0x15, 0x01, 0x16, 0x01, 0x17, 0x01, 0x18,
                            0x01, 0x19, 0x01, 0x1A, 0x01,
                            0x1B, 0x01, 0x1C, 0x01, 0x1D, 0x01, 0x1E, 0x01, 0x1F, 0x01),
                    generateBytes(0x01, 0x5A, 0x3A, 0x30, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
                            0x00, 0x10, 0x00,
                            0xFF, 0xFF, 0x0F, 0x00, 0x18, 0x00, 0x00, 0x00, 0x3A, 0x00,
                            0x00, 0x00, 0xFD, 0x02, 0xFF,
                            0x02, 0x01, 0x03, 0x03, 0x03, 0x05, 0x03, 0x07, 0x03, 0x09,
                            0x03, 0x0B, 0x03, 0x0D, 0x03,
                            0x0F, 0x03, 0x11, 0x03, 0x13, 0x03, 0x15, 0x03, 0x17, 0x03,
                            0x19, 0x03, 0x1B, 0x03, 0x1D,
                            0x03, 0xE4, 0xFC, 0xE6, 0xFC, 0xE8, 0xFC, 0xEA, 0xFC, 0xEC,
                            0xFC, 0xEE, 0xFC, 0xF0, 0xFC,
                            0xF2, 0xFC, 0xF4, 0xFC, 0xF6, 0xFC, 0xF8, 0xFC, 0xFA, 0xFC,
                            0xFC, 0xFC, 0xFE, 0xFC, 0x00,
                            0xFD, 0x02, 0xFD),
                    generateBytes(0x01, 0x52, 0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
                            0x00, 0x20, 0x00,
                            0x10, 0x00, 0x00, 0x00, 0xFD, 0x02, 0xFE, 0x02, 0xFF, 0x02,
                            0x00, 0x03, 0x01, 0x03, 0x02,
                            0x03, 0x03, 0x03, 0x04, 0x03, 0x05, 0x03, 0x06, 0x03, 0x07,
                            0x03, 0x08, 0x03, 0x09, 0x03,
                            0x0A, 0x03, 0x0B, 0x03, 0x0C, 0x03, 0x0D, 0x03, 0x0E, 0x03,
                            0x0F, 0x03, 0x10, 0x03, 0x11,
                            0x03, 0x12, 0x03, 0x13, 0x03, 0x14, 0x03, 0x15, 0x03, 0x16,
                            0x03, 0x17, 0x03, 0x18, 0x03,
                            0x19, 0x03, 0x1A, 0x03, 0x1B, 0x03, 0x1C, 0x03, 0x1D, 0x03) };
            for (Object bitmap : new Object[] { RoaringBitmap.bitmapOf(values),
                    MutableRoaringBitmap.bitmapOf(values),
                    ImmutableRoaringBitmap.bitmapOf(values) }) {
                byte[] actual = getWrittenBytes(
                        o -> BinaryStreamUtils.writeBitmap(o,
                                ClickHouseBitmap.wrap(bitmap, t)));
                Assert.assertEquals(actual, expected[i]);
            }
            i++;
        }
    }

    @Test(groups = { "unit" })
    public void testWriteBitmap64() throws IOException {
        // cardinality <= 32
        for (ClickHouseDataType t : new ClickHouseDataType[] { ClickHouseDataType.Int64,
                ClickHouseDataType.UInt64 }) {
            long[] values = newBitmap64Values(t, 32);
            byte[] expected = getWrittenBytes(o -> {
                BinaryStreamUtils.writeInt8(o, 0);
                BinaryStreamUtils.writeInt8(o, 0x20);
                for (long v : values) {
                    BinaryStreamUtils.writeInt64(o, v);
                }
            });
            for (Object bitmap : new Object[] { Roaring64Bitmap.bitmapOf(values),
                    Roaring64NavigableMap.bitmapOf(values) }) {
                byte[] actual = getWrittenBytes(
                        o -> BinaryStreamUtils.writeBitmap(o,
                                ClickHouseBitmap.wrap(bitmap, t)));
                Assert.assertEquals(actual, expected);
            }
        }

        // cardinality > 32
        // int i = 0;
        // for (ClickHouseDataType t : new ClickHouseDataType[] {
        // ClickHouseDataType.Int64, ClickHouseDataType.UInt64 }) {
        // long[] values = newBitmap64Values(t, 33);
        // byte[][] expected = new byte[][] {
        // generateBytes(0x01, 0x9A, 0x01, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // 0x00, 0x00, 0x00, 0x00,
        // 0x00, 0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x00, 0x00,
        // 0x10, 0x00,
        // 0x00, 0x00, 0xFF, 0xFF, 0x01, 0x00, 0x00, 0x00, 0x3A, 0x30, 0x00, 0x00, 0x01,
        // 0x00, 0x00,
        // 0x00, 0x00, 0x00, 0x0F, 0x00, 0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x00,
        // 0x05, 0x00,
        // 0x07, 0x00, 0x09, 0x00, 0x0B, 0x00, 0x0D, 0x00, 0x0F, 0x00, 0x11, 0x00, 0x13,
        // 0x00, 0x15,
        // 0x00, 0x17, 0x00, 0x19, 0x00, 0x1B, 0x00, 0x1D, 0x00, 0x1F, 0x00, 0xFE, 0xFF,
        // 0xFF, 0xFF,
        // 0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x0E, 0x00, 0x10,
        // 0x00, 0x00,
        // 0x00, 0xE2, 0xFF, 0xE4, 0xFF, 0xE6, 0xFF, 0xE8, 0xFF, 0xEA, 0xFF, 0xEC, 0xFF,
        // 0xEE, 0xFF,
        // 0xF0, 0xFF, 0xF2, 0xFF, 0xF4, 0xFF, 0xF6, 0xFF, 0xF8, 0xFF, 0xFA, 0xFF, 0xFC,
        // 0xFF, 0xFE,
        // 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        // 0x00, 0x00,
        // 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00),
        // generateBytes(0x01, 0x72, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // 0x00, 0x00, 0x00, 0x00,
        // 0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x00, 0x00, 0x10,
        // 0x00, 0x00,
        // 0x00, 0xFF, 0xFF, 0x01, 0x00, 0x00, 0x00, 0x3A, 0x30, 0x00, 0x00, 0x01, 0x00,
        // 0x00, 0x00,
        // 0x00, 0x00, 0x1F, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02,
        // 0x00, 0x03,
        // 0x00, 0x04, 0x00, 0x05, 0x00, 0x06, 0x00, 0x07, 0x00, 0x08, 0x00, 0x09, 0x00,
        // 0x0A, 0x00,
        // 0x0B, 0x00, 0x0C, 0x00, 0x0D, 0x00, 0x0E, 0x00, 0x0F, 0x00, 0x10, 0x00, 0x11,
        // 0x00, 0x12,
        // 0x00, 0x13, 0x00, 0x14, 0x00, 0x15, 0x00, 0x16, 0x00, 0x17, 0x00, 0x18, 0x00,
        // 0x19, 0x00,
        // 0x1A, 0x00, 0x1B, 0x00, 0x1C, 0x00, 0x1D, 0x00, 0x1E, 0x00, 0x1F, 0x00) };
        // for (Object bitmap : new Object[] { Roaring64NavigableMap.bitmapOf(values) })
        // {
        // byte[] actual = getWrittenBytes(
        // o -> BinaryStreamUtils.writeBitmap(o, ClickHouseBitmap.wrap(bitmap, t)));
        // Assert.assertEquals(actual, expected[i]);
        // }
        // i++;
        // }
    }

    @Test(groups = { "unit" })
    public void testReadUnsignedByte() throws IOException {
        for (int i = 0; i < 0xFF; i++) {
            Assert.assertEquals(generateInput(i).readUnsignedByte(), i);
            Assert.assertEquals(BinaryStreamUtils.readUnsignedInt8(generateInput(i)), (short) i);
        }
    }

    @Test(groups = { "unit" })
    public void testWriteUnsignedByte() throws IOException {
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt8(o, 0)), generateBytes(0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt8(o, 1)), generateBytes(1));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt8(o, 255)),
                generateBytes(-1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt8(o, 256)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt8(o, -1)));
    }

    @Test(groups = { "unit" })
    public void testReadNull() throws IOException {
        Assert.assertEquals(BinaryStreamUtils.readNull(generateInput(1)), true);
        Assert.assertEquals(BinaryStreamUtils.readNull(generateInput(0)), false);

        Assert.assertThrows(IllegalArgumentException.class, () -> BinaryStreamUtils.readNull(generateInput(2)));
    }

    @Test(groups = { "unit" })
    public void testWriteNull() throws IOException {
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeNull(o)), generateBytes(1));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeNonNull(o)), generateBytes(0));
    }

    @Test(groups = { "unit" })
    public void testReadInt16() throws IOException {
        Assert.assertEquals(BinaryStreamUtils.readInt16(generateInput(0, 0x80)), Short.MIN_VALUE);
        Assert.assertEquals(BinaryStreamUtils.readInt16(generateInput(0xFF, 0x7F)), Short.MAX_VALUE);
        Assert.assertEquals(BinaryStreamUtils.readInt16(generateInput(0xFF, 0xFF)), Short.parseShort("-1"));
        Assert.assertEquals(BinaryStreamUtils.readInt16(generateInput(0, 0)), Short.parseShort("0"));
    }

    @Test(groups = { "unit" })
    public void testWriteInt16() throws IOException {
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt16(o, (short) 0)),
                generateBytes(0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt16(o, (short) 1)),
                generateBytes(1, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt16(o, (short) -1)),
                generateBytes(0xFF, 0xFF));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt16(o, Short.MIN_VALUE)),
                generateBytes(0, 0x80));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt16(o, Short.MAX_VALUE)),
                generateBytes(0xFF, 0x7F));

        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt16(o, 0)), generateBytes(0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt16(o, 1)), generateBytes(1, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt16(o, -1)),
                generateBytes(0xFF, 0xFF));

        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeInt16(o, Short.MIN_VALUE - 1)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeInt16(o, Short.MAX_VALUE + 1)));
    }

    @Test(groups = { "unit" })
    public void testReadUnsignedInt16() throws IOException {
        Assert.assertEquals(BinaryStreamUtils.readUnsignedInt16(generateInput(0, 0)), 0);
        Assert.assertEquals(BinaryStreamUtils.readUnsignedInt16(generateInput(0xFF, 0)), 0xFF);
        Assert.assertEquals(BinaryStreamUtils.readUnsignedInt16(generateInput(0, 0xFF)), 0xFF00);
        Assert.assertEquals(BinaryStreamUtils.readUnsignedInt16(generateInput(0xFF, 0xFF)), 0xFFFF);
    }

    @Test(groups = { "unit" })
    public void testWriteUnsignedInt16() throws IOException {
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt16(o, 0)),
                generateBytes(0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt16(o, 1)),
                generateBytes(1, 0));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt16(o, -1)));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt16(o, 0xFF)),
                generateBytes(0xFF, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt16(o, 0xFFFF)),
                generateBytes(0xFF, 0xFF));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt16(o, 0xFFFF + 1)));
    }

    @Test(groups = { "unit" })
    public void testReadInt32() throws IOException {
        Assert.assertEquals(BinaryStreamUtils.readInt32(generateInput(0, 0, 0, 0x80)), Integer.MIN_VALUE);
        Assert.assertEquals(BinaryStreamUtils.readInt32(generateInput(0, 0, 0, 0)), 0);
        Assert.assertEquals(BinaryStreamUtils.readInt32(generateInput(0xFF, 0, 0, 0)), 0xFF);
        Assert.assertEquals(BinaryStreamUtils.readInt32(generateInput(0xFF, 0xFF, 0, 0)), 0xFFFF);
        Assert.assertEquals(BinaryStreamUtils.readInt32(generateInput(0xFF, 0xFF, 0xFF, 0)), 0xFFFFFF);
        Assert.assertEquals(BinaryStreamUtils.readInt32(generateInput(0xFF, 0xFF, 0xFF, 0x7F)),
                Integer.MAX_VALUE);
    }

    @Test(groups = { "unit" })
    public void testWriteInt32() throws IOException {
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt32(o, 0)),
                generateBytes(0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt32(o, 1)),
                generateBytes(1, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt32(o, 0xFF)),
                generateBytes(0xFF, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt32(o, 0xFFFF)),
                generateBytes(0xFF, 0xFF, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt32(o, 0xFFFFFF)),
                generateBytes(0xFF, 0xFF, 0xFF, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt32(o, 0xFFFFFFFF)),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt32(o, Integer.MIN_VALUE)),
                generateBytes(0, 0, 0, 0x80));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt32(o, Integer.MAX_VALUE)),
                generateBytes(0xFF, 0xFF, 0xFF, 0x7F));
    }

    @Test(groups = { "unit" })
    public void testReadUnsignedInt32() throws IOException {
        Assert.assertEquals(BinaryStreamUtils.readUnsignedInt32(generateInput(0, 0, 0, 0)), 0L);
        Assert.assertEquals(BinaryStreamUtils.readUnsignedInt32(generateInput(0xFF, 0, 0, 0)), 0xFFL);
        Assert.assertEquals(BinaryStreamUtils.readUnsignedInt32(generateInput(0xFF, 0xFF, 0, 0)), 0xFFFFL);
        Assert.assertEquals(BinaryStreamUtils.readUnsignedInt32(generateInput(0xFF, 0xFF, 0xFF, 0)), 0xFFFFFFL);
        Assert.assertEquals(BinaryStreamUtils.readUnsignedInt32(generateInput(0xFF, 0xFF, 0xFF, 0xFF)),
                0xFFFFFFFFL);
    }

    @Test(groups = { "unit" })
    public void testWriteUnsignedInt32() throws IOException {
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt32(o, 0L)),
                generateBytes(0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt32(o, 1L)),
                generateBytes(1, 0, 0, 0));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt32(o, -1L)));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt32(o, 0xFFL)),
                generateBytes(0xFF, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt32(o, 0xFFFFL)),
                generateBytes(0xFF, 0xFF, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt32(o, 0xFFFFFFL)),
                generateBytes(0xFF, 0xFF, 0xFF, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt32(o, 0xFFFFFFFFL)),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt32(o, 0xFFFFFFFFL + 1L)));
    }

    @Test(groups = { "unit" })
    public void testReadInt64() throws IOException {
        Assert.assertEquals(BinaryStreamUtils.readInt64(generateInput(0, 0, 0, 0, 0, 0, 0, 0x80)),
                Long.MIN_VALUE);
        Assert.assertEquals(BinaryStreamUtils.readInt64(generateInput(0, 0, 0, 0, 0, 0, 0, 0)), 0L);
        Assert.assertEquals(BinaryStreamUtils.readInt64(generateInput(0xFF, 0, 0, 0, 0, 0, 0, 0)), 0xFFL);
        Assert.assertEquals(BinaryStreamUtils.readInt64(generateInput(0xFF, 0xFF, 0, 0, 0, 0, 0, 0)), 0xFFFFL);
        Assert.assertEquals(BinaryStreamUtils.readInt64(generateInput(0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0)),
                0xFFFFFFL);
        Assert.assertEquals(BinaryStreamUtils.readInt64(generateInput(0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0)),
                0xFFFFFFFFL);
        Assert.assertEquals(BinaryStreamUtils.readInt64(generateInput(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0)),
                0xFFFFFFFFFFL);
        Assert.assertEquals(
                BinaryStreamUtils.readInt64(generateInput(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0)),
                0xFFFFFFFFFFFFL);
        Assert.assertEquals(
                BinaryStreamUtils.readInt64(generateInput(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0)),
                0xFFFFFFFFFFFFFFL);
        Assert.assertEquals(
                BinaryStreamUtils.readInt64(
                        generateInput(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)),
                -1L);
        Assert.assertEquals(
                BinaryStreamUtils.readInt64(
                        generateInput(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F)),
                Long.MAX_VALUE);
    }

    @Test(groups = { "unit" })
    public void testWriteInt64() throws IOException {
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt64(o, Long.MIN_VALUE)),
                generateBytes(0, 0, 0, 0, 0, 0, 0, 0x80));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt64(o, 0L)),
                generateBytes(0, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt64(o, 0xFFL)),
                generateBytes(0xFF, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt64(o, 0xFFFFL)),
                generateBytes(0xFF, 0xFF, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt64(o, 0xFFFFFFL)),
                generateBytes(0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt64(o, 0xFFFFFFFFL)),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt64(o, 0xFFFFFFFFFFL)),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt64(o, 0xFFFFFFFFFFFFL)),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt64(o, 0xFFFFFFFFFFFFFFL)),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt64(o, 0xFFFFFFFFFFFFFFFFL)),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt64(o, Long.MAX_VALUE)),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F));
    }

    @Test(groups = { "unit" })
    public void testReadUnsignedInt64() throws IOException {
        byte[] bytes = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };
        Assert.assertEquals(BinaryStreamUtils.readUnsignedInt64(generateInput(bytes)), BigInteger.ZERO);
        Assert.assertEquals(
                BinaryStreamUtils.readUnsignedInt64(
                        generateInput(bytes = generateBytes(0xFF, 0, 0, 0, 0, 0, 0, 0))),
                new BigInteger(BinaryStreamUtils.reverse(bytes)));

        Assert.assertEquals(
                BinaryStreamUtils.readUnsignedInt64(
                        generateInput(bytes = generateBytes(0xFF, 0xFF, 0, 0, 0, 0, 0, 0))),
                new BigInteger(BinaryStreamUtils.reverse(bytes)));
        Assert.assertEquals(
                BinaryStreamUtils
                        .readUnsignedInt64(generateInput(bytes = generateBytes(0xFF, 0xFF, 0xFF,
                                0, 0, 0, 0, 0))),
                new BigInteger(BinaryStreamUtils.reverse(bytes)));
        Assert.assertEquals(
                BinaryStreamUtils
                        .readUnsignedInt64(generateInput(bytes = generateBytes(0xFF, 0xFF, 0xFF,
                                0xFF, 0, 0, 0, 0))),
                new BigInteger(BinaryStreamUtils.reverse(bytes)));
        Assert.assertEquals(
                BinaryStreamUtils
                        .readUnsignedInt64(generateInput(bytes = generateBytes(0xFF, 0xFF, 0xFF,
                                0xFF, 0xFF, 0, 0, 0))),
                new BigInteger(BinaryStreamUtils.reverse(bytes)));
        Assert.assertEquals(
                BinaryStreamUtils.readUnsignedInt64(
                        generateInput(bytes = generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                0, 0))),
                new BigInteger(BinaryStreamUtils.reverse(bytes)));
        Assert.assertEquals(
                BinaryStreamUtils.readUnsignedInt64(
                        generateInput(bytes = generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                0xFF, 0))),
                new BigInteger(BinaryStreamUtils.reverse(bytes)));
        Assert.assertEquals(
                BinaryStreamUtils.readUnsignedInt64(
                        generateInput(bytes = generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                0xFF, 0xFF))),
                new BigInteger(1, BinaryStreamUtils.reverse(bytes)));

    }

    @Test(groups = { "unit" })
    public void testWriteUnsignedInt64() throws IOException {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(
                        o -> BinaryStreamUtils.writeUnsignedInt64(o, BigInteger.valueOf(-1L))));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt64(o, BigInteger.ZERO)),
                generateBytes(0, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt64(o, BigInteger.ONE)),
                generateBytes(1, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt64(o,
                        new BigInteger(1, generateBytes(0, 0, 0, 0, 0, 0, 0, 0xFF)))),
                generateBytes(0xFF, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt64(o,
                        new BigInteger(1, generateBytes(0, 0, 0, 0, 0, 0, 0xFF, 0xFF)))),
                generateBytes(0xFF, 0xFF, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt64(o,
                        new BigInteger(1, generateBytes(0, 0, 0, 0, 0, 0xFF, 0xFF, 0xFF)))),
                generateBytes(0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt64(o,
                        new BigInteger(1, generateBytes(0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF)))),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt64(o,
                        new BigInteger(1,
                                generateBytes(0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)))),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt64(o,
                        new BigInteger(1,
                                generateBytes(0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                        0xFF)))),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt64(o,
                        new BigInteger(1,
                                generateBytes(0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                        0xFF)))),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt64(o,
                        new BigInteger(1,
                                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                        0xFF)))),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt64(o,
                        new BigInteger(1,
                                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                        0xFF))
                                .add(BigInteger.ONE))));
    }

    @Test(groups = { "unit" })
    public void testReadInt128() throws IOException {
        byte[][] arr = generateBytes(Long.BYTES * 2, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] bytes = arr[i];
            Assert.assertEquals(BinaryStreamUtils.readInt128(generateInput(bytes)),
                    new BigInteger(BinaryStreamUtils.reverse(bytes)));
        }

        Assert.assertThrows(IOException.class, () -> BinaryStreamUtils.readInt128(generateInput(1)));
    }

    @Test(groups = { "unit" })
    public void testWriteInt128() throws IOException {
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt128(o, BigInteger.ZERO)),
                generateBytes(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt128(o, BigInteger.ONE)),
                generateBytes(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt128(o, BigInteger.valueOf(-1L))),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                        0xFF, 0xFF, 0xFF,
                        0xFF));

        byte[][] arr = generateBytes(Long.BYTES * 2, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] bytes = arr[i];
            Assert.assertEquals(
                    getWrittenBytes(o -> BinaryStreamUtils.writeInt128(o, new BigInteger(bytes))),
                    BinaryStreamUtils.reverse(bytes));
        }
    }

    @Test(groups = { "unit" })
    public void testReadUnsignedInt128() throws IOException {
        byte[][] arr = generateBytes(Long.BYTES * 2, true);
        for (int i = 0; i < arr.length; i++) {
            byte[] bytes = arr[i];
            Assert.assertEquals(BinaryStreamUtils.readUnsignedInt128(generateInput(bytes)),
                    new BigInteger(1, BinaryStreamUtils.reverse(bytes)));
        }

        Assert.assertThrows(IOException.class, () -> BinaryStreamUtils.readUnsignedInt128(generateInput(1)));
    }

    @Test(groups = { "unit" })
    public void testWriteUnsignedInt128() throws IOException {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt128(o,
                        BigInteger.valueOf(-1L))));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt128(o, BigInteger.ZERO)),
                generateBytes(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt128(o, BigInteger.ONE)),
                generateBytes(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));

        byte[][] arr = generateBytes(Long.BYTES * 2, true);
        for (int i = 0; i < arr.length; i++) {
            byte[] bytes = arr[i];
            Assert.assertEquals(
                    getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt128(o,
                            new BigInteger(1, bytes))),
                    BinaryStreamUtils.reverse(bytes));
        }
    }

    @Test(groups = { "unit" })
    public void testReadInt256() throws IOException {
        byte[][] arr = generateBytes(Long.BYTES * 4, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] bytes = arr[i];
            Assert.assertEquals(BinaryStreamUtils.readInt256(generateInput(bytes)),
                    new BigInteger(BinaryStreamUtils.reverse(bytes)));
        }

        Assert.assertThrows(IOException.class, () -> BinaryStreamUtils.readInt256(generateInput(1)));
    }

    @Test(groups = { "unit" })
    public void testWriteInt256() throws IOException {
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt256(o, BigInteger.ZERO)),
                generateBytes(0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt256(o, BigInteger.ONE)),
                generateBytes(1, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeInt256(o, BigInteger.valueOf(-1L))),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                        0xFF, 0xFF, 0xFF,
                        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                        0xFF, 0xFF, 0xFF, 0xFF,
                        0xFF));

        byte[][] arr = generateBytes(Long.BYTES * 4, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] bytes = arr[i];
            Assert.assertEquals(
                    getWrittenBytes(o -> BinaryStreamUtils.writeInt256(o, new BigInteger(bytes))),
                    BinaryStreamUtils.reverse(bytes));
        }
    }

    @Test(groups = { "unit" })
    public void testReadUnsignedInt256() throws IOException {
        byte[][] arr = generateBytes(Long.BYTES * 4, true);
        for (int i = 0; i < arr.length; i++) {
            byte[] bytes = arr[i];
            Assert.assertEquals(BinaryStreamUtils.readUnsignedInt256(generateInput(bytes)),
                    new BigInteger(1, BinaryStreamUtils.reverse(bytes)));
        }

        Assert.assertThrows(IOException.class, () -> BinaryStreamUtils.readUnsignedInt256(generateInput(1)));
    }

    @Test(groups = { "unit" })
    public void testWriteUnsignedInt256() throws IOException {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt256(o,
                        BigInteger.valueOf(-1L))));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt256(o, BigInteger.ZERO)),
                generateBytes(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0,
                        0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt256(o, BigInteger.ONE)),
                generateBytes(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0,
                        0, 0));

        byte[][] arr = generateBytes(Long.BYTES * 4, true);
        for (int i = 0; i < arr.length; i++) {
            byte[] bytes = arr[i];
            Assert.assertEquals(
                    getWrittenBytes(o -> BinaryStreamUtils.writeUnsignedInt256(o,
                            new BigInteger(1, bytes))),
                    BinaryStreamUtils.reverse(bytes));
        }
    }

    @Test(groups = { "unit" })
    public void testReadDate() throws IOException {
        Assert.assertEquals(BinaryStreamUtils.readDate(generateInput(-1, 0), null),
                LocalDate.ofEpochDay(255));
        Assert.assertEquals(BinaryStreamUtils.readDate(generateInput(0, 0x80), null),
                LocalDate.ofEpochDay(0x8000));
        Assert.assertEquals(BinaryStreamUtils.readDate(generateInput(0, 0), null),
                LocalDate.ofEpochDay(0));
        Assert.assertEquals(BinaryStreamUtils.readDate(generateInput(1, 0), null),
                LocalDate.ofEpochDay(1));
        Assert.assertEquals(BinaryStreamUtils.readDate(generateInput(0xFF, 0x7F), null),
                LocalDate.ofEpochDay(Short.MAX_VALUE));
        Assert.assertEquals(BinaryStreamUtils.readDate(generateInput(0xFF, 0xFF), null),
                LocalDate.ofEpochDay(0xFFFF));

        Assert.assertEquals(BinaryStreamUtils.readDate(generateInput(0x9E, 0x49), null),
                LocalDate.of(2021, 8, 7));
    }

    @Test(groups = { "unit" })
    public void testWriteDate() throws IOException {
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDate(o, LocalDate.ofEpochDay(255), null)),
                generateBytes(-1, 0));
        Assert.assertEquals(getWrittenBytes(
                o -> BinaryStreamUtils.writeDate(o, LocalDate.ofEpochDay(0x8000), null)),
                generateBytes(0, 0x80));
        Assert.assertEquals(getWrittenBytes(
                o -> BinaryStreamUtils.writeDate(o, LocalDate.ofEpochDay(0), null)),
                generateBytes(0, 0));
        Assert.assertEquals(getWrittenBytes(
                o -> BinaryStreamUtils.writeDate(o, LocalDate.ofEpochDay(1), null)),
                generateBytes(1, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDate(o,
                        LocalDate.ofEpochDay(Short.MAX_VALUE), null)),
                generateBytes(0xFF, 0x7F));
        Assert.assertEquals(getWrittenBytes(
                o -> BinaryStreamUtils.writeDate(o, LocalDate.ofEpochDay(0xFFFF), null)),
                generateBytes(0xFF, 0xFF));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDate(o, LocalDate.of(2021, 8, 7), null)),
                generateBytes(0x9E, 0x49));
    }

    @Test(groups = { "unit" })
    public void testReadDate32() throws IOException {
        Assert.assertEquals(BinaryStreamUtils.readDate32(generateInput(0xFF, 0xFF, 0xFF, 0xFF), null),
                LocalDate.ofEpochDay(-1));
        Assert.assertEquals(BinaryStreamUtils.readDate32(generateInput(0, 0, 0, 0), null),
                LocalDate.ofEpochDay(0));
        Assert.assertEquals(BinaryStreamUtils.readDate32(generateInput(1, 0, 0, 0), null),
                LocalDate.ofEpochDay(1));
        Assert.assertEquals(BinaryStreamUtils.readDate32(generateInput(0x17, 0x61, 0, 0), null),
                LocalDate.of(2038, 1, 19));

        Assert.assertEquals(BinaryStreamUtils.readDate32(generateInput(0x21, 0x9C, 0xFF, 0xFF), null),
                LocalDate.of(1900, 1, 1));
        Assert.assertEquals(BinaryStreamUtils.readDate32(generateInput(0xD1, 0xD6, 1, 0), null),
                LocalDate.of(2299, 12, 31));
    }

    @Test(dataProvider = "timeZoneProvider", groups = { "unit" })
    public void testReadDate32WithTimeZone(String timeZoneId) throws IOException {
        TimeZone tz = TimeZone.getTimeZone(timeZoneId);
        Assert.assertEquals(BinaryStreamUtils.readDate32(generateInput(0xFF, 0xFF, 0xFF, 0xFF), tz),
                LocalDate.ofEpochDay(-1).atStartOfDay(ClickHouseValues.SYS_ZONE)
                        .withZoneSameInstant(tz.toZoneId())
                        .toLocalDate());
        Assert.assertEquals(BinaryStreamUtils.readDate32(generateInput(0, 0, 0, 0), tz),
                LocalDate.ofEpochDay(0).atStartOfDay(ClickHouseValues.SYS_ZONE)
                        .withZoneSameInstant(tz.toZoneId())
                        .toLocalDate());
        Assert.assertEquals(BinaryStreamUtils.readDate32(generateInput(1, 0, 0, 0), tz),
                LocalDate.ofEpochDay(1).atStartOfDay(ClickHouseValues.SYS_ZONE)
                        .withZoneSameInstant(tz.toZoneId())
                        .toLocalDate());
        Assert.assertEquals(BinaryStreamUtils.readDate32(generateInput(0x17, 0x61, 0, 0), tz),
                LocalDate.of(2038, 1, 19).atStartOfDay(ClickHouseValues.SYS_ZONE)
                        .withZoneSameInstant(tz.toZoneId())
                        .toLocalDate());

        Assert.assertEquals(BinaryStreamUtils.readDate32(generateInput(0x21, 0x9C, 0xFF, 0xFF), tz),
                LocalDate.of(1900, 1, 1).atStartOfDay(ClickHouseValues.SYS_ZONE)
                        .withZoneSameInstant(tz.toZoneId())
                        .toLocalDate());
        Assert.assertEquals(BinaryStreamUtils.readDate32(generateInput(0xD1, 0xD6, 1, 0), tz),
                LocalDate.of(2299, 12, 31).atStartOfDay(ClickHouseValues.SYS_ZONE)
                        .withZoneSameInstant(tz.toZoneId())
                        .toLocalDate());
    }

    @Test(groups = { "unit" })
    public void testWriteDate32() throws IOException {
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.ofEpochDay(-1), null)),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.ofEpochDay(0), null)),
                generateBytes(0, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.ofEpochDay(1), null)),
                generateBytes(1, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.of(2038, 1, 19), null)),
                generateBytes(0x17, 0x61, 0, 0));

        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.of(1900, 1, 1), null)),
                generateBytes(0x21, 0x9C, 0xFF, 0xFF));
        Assert.assertEquals(getWrittenBytes(
                o -> BinaryStreamUtils.writeDate32(o, LocalDate.of(2299, 12, 31), null)),
                generateBytes(0xD1, 0xD6, 1, 0));

        Assert.assertThrows(IllegalArgumentException.class, () -> getWrittenBytes(
                o -> BinaryStreamUtils.writeDate32(o,
                        LocalDate.of(1900, 1, 1).minus(1L, ChronoUnit.DAYS), null)));
        Assert.assertThrows(IllegalArgumentException.class, () -> getWrittenBytes(
                o -> BinaryStreamUtils.writeDate32(o,
                        LocalDate.of(2299, 12, 31).plus(1L, ChronoUnit.DAYS), null)));
    }

    @Test(dataProvider = "timeZoneProvider", groups = { "unit" })
    public void testWriteDate32WithTimeZone(String timeZoneId) throws IOException {
        TimeZone tz = TimeZone.getTimeZone(timeZoneId);
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.ofEpochDay(-1), tz)),
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.ofEpochDay(-1)
                        .atStartOfDay(tz.toZoneId())
                        .withZoneSameInstant(ClickHouseValues.SYS_ZONE).toLocalDate())));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.ofEpochDay(0), tz)),
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.ofEpochDay(0)
                        .atStartOfDay(tz.toZoneId())
                        .withZoneSameInstant(ClickHouseValues.SYS_ZONE).toLocalDate())));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.ofEpochDay(1), tz)),
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.ofEpochDay(1)
                        .atStartOfDay(tz.toZoneId())
                        .withZoneSameInstant(ClickHouseValues.SYS_ZONE).toLocalDate())));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.of(2038, 1, 19), tz)),
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.of(2038, 1, 19)
                        .atStartOfDay(tz.toZoneId())
                        .withZoneSameInstant(ClickHouseValues.SYS_ZONE).toLocalDate())));

        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.of(1900, 1, 2), tz)),
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.of(1900, 1, 2)
                        .atStartOfDay(tz.toZoneId())
                        .withZoneSameInstant(ClickHouseValues.SYS_ZONE).toLocalDate())));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.of(2299, 12, 31), tz)),
                getWrittenBytes(o -> BinaryStreamUtils.writeDate32(o, LocalDate.of(2299, 12, 31)
                        .atStartOfDay(tz.toZoneId())
                        .withZoneSameInstant(ClickHouseValues.SYS_ZONE).toLocalDate())));

        Assert.assertThrows(IllegalArgumentException.class, () -> getWrittenBytes(
                o -> BinaryStreamUtils.writeDate32(o,
                        LocalDate.of(1900, 1, 1).minus(2L, ChronoUnit.DAYS), tz)));
        Assert.assertThrows(IllegalArgumentException.class, () -> getWrittenBytes(
                o -> BinaryStreamUtils.writeDate32(o,
                        LocalDate.of(2299, 12, 31).plus(2L, ChronoUnit.DAYS), tz)));
    }

    @Test(groups = { "unit" })
    public void testReadDateTime32() throws IOException {
        Assert.assertEquals(BinaryStreamUtils.readDateTime32(generateInput(-1, 0, 0, 0), null),
                LocalDateTime.ofEpochSecond(255, 0, ZoneOffset.UTC));
        Assert.assertEquals(BinaryStreamUtils.readDateTime32(generateInput(0, 0, 0, 0x80), null), LocalDateTime
                .ofEpochSecond(new BigInteger(1, generateBytes(0x80, 0, 0, 0)).longValue(), 0,
                        ZoneOffset.UTC));
        Assert.assertEquals(BinaryStreamUtils.readDateTime32(generateInput(0, 0, 0, 0), null),
                LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC));
        Assert.assertEquals(BinaryStreamUtils.readDateTime32(generateInput(1, 0, 0, 0), null),
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC));
        Assert.assertEquals(BinaryStreamUtils.readDateTime32(generateInput(0xFF, 0xFF, 0xFF, 0x7F), null),
                LocalDateTime.ofEpochSecond(Integer.MAX_VALUE, 0, ZoneOffset.UTC));

        Assert.assertEquals(BinaryStreamUtils.readDateTime32(generateInput(0x2D, 0x9A, 0x0E, 0x61), null),
                LocalDateTime.of(2021, 8, 7, 14, 35, 25));
    }

    @Test(dataProvider = "timeZoneProvider", groups = { "unit" })
    public void testReadDateTime32WithTimeZone(String timeZoneId) throws IOException {
        TimeZone tz = TimeZone.getTimeZone(timeZoneId);
        Assert.assertEquals(BinaryStreamUtils.readDateTime32(generateInput(-1, 0, 0, 0), tz),
                LocalDateTime.ofInstant(Instant.ofEpochSecond(255L), tz.toZoneId()));
        Assert.assertEquals(BinaryStreamUtils.readDateTime32(generateInput(0, 0, 0, 0x80), tz),
                LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(new BigInteger(1, generateBytes(0x80, 0, 0, 0))
                                .longValue()),
                        tz.toZoneId()));
        Assert.assertEquals(BinaryStreamUtils.readDateTime32(generateInput(0, 0, 0, 0), tz),
                LocalDateTime.ofInstant(Instant.ofEpochSecond(0L), tz.toZoneId()));
        Assert.assertEquals(BinaryStreamUtils.readDateTime32(generateInput(1, 0, 0, 0), tz),
                LocalDateTime.ofInstant(Instant.ofEpochSecond(1L), tz.toZoneId()));
        Assert.assertEquals(BinaryStreamUtils.readDateTime32(generateInput(0xFF, 0xFF, 0xFF, 0x7F), tz),
                LocalDateTime.ofInstant(Instant.ofEpochSecond(Integer.MAX_VALUE), tz.toZoneId()));

        Assert.assertEquals(BinaryStreamUtils.readDateTime32(generateInput(0x2D, 0x9A, 0x0E, 0x61), tz),
                LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(
                                new BigInteger(1, generateBytes(0x61, 0x0E, 0x9A, 0x2D))
                                        .longValue()),
                        tz.toZoneId()));
    }

    @Test(groups = { "unit" })
    public void testWriteDateTime32() throws IOException {
        Assert.assertEquals(getWrittenBytes(
                o -> BinaryStreamUtils.writeDateTime32(o,
                        LocalDateTime.ofEpochSecond(255, 0, ZoneOffset.UTC), null)),
                generateBytes(-1, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime32(o, LocalDateTime.ofEpochSecond(
                        new BigInteger(1, generateBytes(0x80, 0, 0, 0)).longValue(), 0,
                        ZoneOffset.UTC), null)),
                generateBytes(0, 0, 0, 0x80));
        Assert.assertEquals(getWrittenBytes(
                o -> BinaryStreamUtils.writeDateTime32(o,
                        LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC), null)),
                generateBytes(0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(
                o -> BinaryStreamUtils.writeDateTime32(o,
                        LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), null)),
                generateBytes(1, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime32(o,
                        LocalDateTime.ofEpochSecond(Integer.MAX_VALUE, 0, ZoneOffset.UTC),
                        null)),
                generateBytes(0xFF, 0xFF, 0xFF, 0x7F));

        Assert.assertEquals(
                getWrittenBytes(
                        o -> BinaryStreamUtils.writeDateTime32(o,
                                LocalDateTime.of(2021, 8, 7, 14, 35, 25), null)),
                generateBytes(0x2D, 0x9A, 0x0E, 0x61));

        Assert.assertThrows(IllegalArgumentException.class, () -> getWrittenBytes(
                o -> BinaryStreamUtils.writeDateTime32(o,
                        LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC), null)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeDateTime32(o,
                        LocalDateTime.ofEpochSecond(BinaryStreamUtils.DATETIME_MAX + 1, 0,
                                ZoneOffset.UTC),
                        null)));
    }

    @Test(dataProvider = "timeZoneProvider", groups = { "unit" })
    public void testWriteDateTime32WithTimeZone(String timeZoneId) throws IOException {
        TimeZone tz = TimeZone.getTimeZone(timeZoneId);
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime32(o,
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(255L), tz.toZoneId()),
                        tz)),
                generateBytes(-1, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeDateTime32(o,
                LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(new BigInteger(1, generateBytes(0x80, 0, 0, 0))
                                .longValue()),
                        tz.toZoneId()),
                tz)), generateBytes(0, 0, 0, 0x80));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime32(o,
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(0L), tz.toZoneId()), tz)),
                generateBytes(0, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime32(o,
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(1L), tz.toZoneId()), tz)),
                generateBytes(1, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime32(o,
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(Integer.MAX_VALUE),
                                tz.toZoneId()),
                        tz)),
                generateBytes(0xFF, 0xFF, 0xFF, 0x7F));

        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeDateTime32(o,
                LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(
                                new BigInteger(1, generateBytes(0x61, 0x0E, 0x9A, 0x2D))
                                        .longValue()),
                        tz.toZoneId()),
                tz)), generateBytes(0x2D, 0x9A, 0x0E, 0x61));

        Assert.assertThrows(IllegalArgumentException.class, () -> getWrittenBytes(o -> BinaryStreamUtils
                .writeDateTime32(o, LocalDateTime.ofInstant(Instant.ofEpochSecond(-1L), tz.toZoneId()),
                        tz)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeDateTime32(o, LocalDateTime
                        .ofInstant(Instant.ofEpochSecond(BinaryStreamUtils.DATETIME_MAX + 1),
                                tz.toZoneId()),
                        tz)));
    }

    @Test(groups = { "unit" })
    public void testReadDateTime64() throws IOException {
        Assert.assertEquals(BinaryStreamUtils.readDateTime64(generateInput(-1, 0, 0, 0, 0, 0, 0, 0), 0, null),
                LocalDateTime.ofEpochSecond(255, 0, ZoneOffset.UTC));
        Assert.assertEquals(BinaryStreamUtils
                .readDateTime64(generateInput(0xF6, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF), 1, null),
                LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC));
        Assert.assertEquals(BinaryStreamUtils
                .readDateTime64(generateInput(0xF5, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF), 1, null),
                LocalDateTime.ofEpochSecond(-2L, 900000000, ZoneOffset.UTC));
        // Actually query "select toDateTime64(-1.000000001::Decimal64(9), 9)" returns:
        // 1969-12-31 23:59:59.000000001
        // see https://github.com/ClickHouse/ClickHouse/issues/29386
        Assert.assertEquals(BinaryStreamUtils
                .readDateTime64(generateInput(0xFF, 0x35, 0x65, 0xC4, 0xFF, 0xFF, 0xFF, 0xFF), 9, null),
                LocalDateTime.ofEpochSecond(-2L, 999999999, ZoneOffset.UTC));
        Assert.assertEquals(BinaryStreamUtils.readDateTime64(generateInput(0, 0, 0, 0, 0, 0, 0, 0), 0, null),
                LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC));
        Assert.assertEquals(BinaryStreamUtils.readDateTime64(generateInput(1, 0, 0, 0, 0, 0, 0, 0), 0, null),
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC));

        Assert.assertEquals(BinaryStreamUtils.readDateTime64(generateInput(0x0A, 0, 0, 0, 0, 0, 0, 0), 1, null),
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC));
        Assert.assertEquals(BinaryStreamUtils.readDateTime64(generateInput(1, 0, 0, 0, 0, 0, 0, 0), 9, null),
                LocalDateTime.ofEpochSecond(0L, 1, ZoneOffset.UTC));
    }

    @Test(dataProvider = "timeZoneProvider", groups = { "unit" })
    public void testReadDateTime64WithTimeZone(String timeZoneId) throws IOException {
        TimeZone tz = TimeZone.getTimeZone(timeZoneId);
        Assert.assertEquals(BinaryStreamUtils.readDateTime64(generateInput(-1, 0, 0, 0, 0, 0, 0, 0), 0, tz),
                LocalDateTime.ofInstant(Instant.ofEpochSecond(255L), tz.toZoneId()));
        Assert.assertEquals(
                BinaryStreamUtils.readDateTime64(
                        generateInput(0xF6, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF), 1, tz),
                LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC)
                        .atZoneSameInstant(tz.toZoneId()).toLocalDateTime());
        Assert.assertEquals(
                BinaryStreamUtils.readDateTime64(
                        generateInput(0xF5, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF), 1, tz),
                LocalDateTime.ofEpochSecond(-2L, 900000000, ZoneOffset.UTC).atOffset(ZoneOffset.UTC)
                        .atZoneSameInstant(tz.toZoneId()).toLocalDateTime());
        // Actually query "select toDateTime64(-1.000000001::Decimal64(9), 9)" returns:
        // 1969-12-31 23:59:59.000000001
        // see https://github.com/ClickHouse/ClickHouse/issues/29386
        Assert.assertEquals(
                BinaryStreamUtils.readDateTime64(
                        generateInput(0xFF, 0x35, 0x65, 0xC4, 0xFF, 0xFF, 0xFF, 0xFF), 9, tz),
                LocalDateTime.ofEpochSecond(-2L, 999999999, ZoneOffset.UTC).atOffset(ZoneOffset.UTC)
                        .atZoneSameInstant(tz.toZoneId()).toLocalDateTime());
        Assert.assertEquals(BinaryStreamUtils.readDateTime64(generateInput(0, 0, 0, 0, 0, 0, 0, 0), 0, tz),
                LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC)
                        .atZoneSameInstant(tz.toZoneId()).toLocalDateTime());
        Assert.assertEquals(BinaryStreamUtils.readDateTime64(generateInput(1, 0, 0, 0, 0, 0, 0, 0), 0, tz),
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC)
                        .atZoneSameInstant(tz.toZoneId()).toLocalDateTime());

        Assert.assertEquals(BinaryStreamUtils.readDateTime64(generateInput(0x0A, 0, 0, 0, 0, 0, 0, 0), 1, tz),
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC)
                        .atZoneSameInstant(tz.toZoneId()).toLocalDateTime());
        Assert.assertEquals(BinaryStreamUtils.readDateTime64(generateInput(1, 0, 0, 0, 0, 0, 0, 0), 9, tz),
                LocalDateTime.ofEpochSecond(0L, 1, ZoneOffset.UTC).atOffset(ZoneOffset.UTC)
                        .atZoneSameInstant(tz.toZoneId()).toLocalDateTime());
    }

    @Test(groups = { "unit" })
    public void testWriteDateTime64() throws IOException {
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.ofEpochSecond(255L, 0, ZoneOffset.UTC), 0, null)),
                generateBytes(-1, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC), 1, null)),
                generateBytes(0xF6, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.ofEpochSecond(-2L, 900000000, ZoneOffset.UTC), 1, null)),
                generateBytes(0xF5, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.ofEpochSecond(-2L, 999999999, ZoneOffset.UTC), 9, null)),
                generateBytes(0xFF, 0x35, 0x65, 0xC4, 0xFF, 0xFF, 0xFF, 0xFF));
        Assert.assertEquals(getWrittenBytes(
                o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC), 0, null)),
                generateBytes(0, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(
                o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), 0, null)),
                generateBytes(1, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(
                o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), 1, null)),
                generateBytes(0x0A, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(
                o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.ofEpochSecond(0L, 1, ZoneOffset.UTC), 9, null)),
                generateBytes(1, 0, 0, 0, 0, 0, 0, 0));

        Assert.assertThrows(IllegalArgumentException.class, () -> getWrittenBytes(o -> BinaryStreamUtils
                .writeDateTime64(o, LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC), -1, null)));
        Assert.assertThrows(IllegalArgumentException.class, () -> getWrittenBytes(o -> BinaryStreamUtils
                .writeDateTime64(o, LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC), 10, null)));

        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.of(LocalDate.of(1900, 1, 1).minus(1L, ChronoUnit.DAYS),
                                LocalTime.MAX),
                        null)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.of(LocalDate.of(2299, 12, 31).plus(1L, ChronoUnit.DAYS),
                                LocalTime.MIN),
                        null)));
    }

    @Test(dataProvider = "timeZoneProvider", groups = { "unit" })
    public void testWriteDateTime64WithTimeZone(String timeZoneId) throws IOException {
        TimeZone tz = TimeZone.getTimeZone(timeZoneId);
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(255L), tz.toZoneId()), 0,
                        tz)),
                generateBytes(-1, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(-1L), tz.toZoneId()), 1,
                        tz)),
                generateBytes(0xF6, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(-2L, 900000000),
                                tz.toZoneId()),
                        1, tz)),
                generateBytes(0xF5, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(-2L, 999999999),
                                tz.toZoneId()),
                        9, tz)),
                generateBytes(0xFF, 0x35, 0x65, 0xC4, 0xFF, 0xFF, 0xFF, 0xFF));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(0L, 0), tz.toZoneId()), 0,
                        tz)),
                generateBytes(0, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(1L, 0), tz.toZoneId()), 0,
                        tz)),
                generateBytes(1, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(1L, 0), tz.toZoneId()), 1,
                        tz)),
                generateBytes(0x0A, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDateTime64(o,
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(0L, 1), tz.toZoneId()), 9,
                        tz)),
                generateBytes(1, 0, 0, 0, 0, 0, 0, 0));

        Assert.assertThrows(IllegalArgumentException.class, () -> getWrittenBytes(o -> BinaryStreamUtils
                .writeDateTime64(o,
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(0L, 0), tz.toZoneId()),
                        -1, tz)));
        Assert.assertThrows(IllegalArgumentException.class, () -> getWrittenBytes(o -> BinaryStreamUtils
                .writeDateTime64(o,
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(0L, 0), tz.toZoneId()),
                        10, tz)));

        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(
                        o -> BinaryStreamUtils.writeDateTime64(o,
                                LocalDateTime.of(
                                        LocalDate.of(1900, 1, 1).minus(1L,
                                                ChronoUnit.DAYS),
                                        LocalTime.MAX)
                                        .atOffset(ZoneOffset.UTC)
                                        .atZoneSameInstant(tz.toZoneId())
                                        .toLocalDateTime(),
                                tz)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(
                        o -> BinaryStreamUtils.writeDateTime64(o,
                                LocalDateTime.of(
                                        LocalDate.of(2299, 12, 31).plus(1L,
                                                ChronoUnit.DAYS),
                                        LocalTime.MIN)
                                        .atOffset(ZoneOffset.UTC)
                                        .atZoneSameInstant(tz.toZoneId())
                                        .toLocalDateTime(),
                                tz)));
    }

    @Test(groups = { "unit" })
    public void testReadDecimal32() throws IOException {
        byte[][] arr = generateBytes(Integer.BYTES, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] b = arr[i];
            for (int j = 0; j < 10; j++) {
                byte[] bytes = new byte[b.length];
                System.arraycopy(b, 0, bytes, 0, b.length);
                Assert.assertEquals(BinaryStreamUtils.readDecimal32(generateInput(bytes), j),
                        new BigDecimal(new BigInteger(BinaryStreamUtils.reverse(bytes)), j));
            }
        }

        Assert.assertEquals(BinaryStreamUtils.readDecimal32(generateInput(0xFF, 0xFF, 0xFF, 0xFF), 0),
                new BigDecimal("-1"));
        Assert.assertEquals(BinaryStreamUtils.readDecimal32(generateInput(0, 0xCA, 0x9A, 0x3B), 9),
                new BigDecimal("1.000000000"));
        Assert.assertEquals(BinaryStreamUtils.readDecimal32(generateInput(0, 0x36, 0x65, 0xC4), 9),
                new BigDecimal("-1.000000000"));
    }

    @Test(groups = { "unit" })
    public void testWriteDecimal32() throws IOException {
        byte[][] arr = generateBytes(Integer.BYTES, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] b = arr[i];
            for (int j = 0; j < 10; j++) {
                byte[] bytes = new byte[b.length];
                System.arraycopy(b, 0, bytes, 0, b.length);
                final int scale = j;
                BigDecimal d = new BigDecimal(new BigInteger(BinaryStreamUtils.reverse(bytes)), scale);
                if (d.toString().replaceAll("[-.]", "").replaceAll("^0+", "").length() > 9) {
                    Assert.assertThrows(IllegalArgumentException.class,
                            () -> getWrittenBytes(o -> BinaryStreamUtils.writeDecimal32(o,
                                    d, scale)));
                } else {
                    Assert.assertEquals(getWrittenBytes(
                            o -> BinaryStreamUtils.writeDecimal32(o, d, scale)), b);
                }
            }
        }

        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeDecimal32(o, new BigDecimal("-1"), 0)),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDecimal32(o, new BigDecimal("0.9999999999"),
                        9)),
                generateBytes(0xFF, 0xC9, 0x9A, 0x3B));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDecimal32(o,
                        new BigDecimal("-0.9999999999"), 9)),
                generateBytes(0x01, 0x36, 0x65, 0xC4));

        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeDecimal32(o, BigDecimal.ZERO, -1)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeDecimal32(o, BigDecimal.ZERO, 10)));
    }

    @Test(groups = { "unit" })
    public void testReadDecimal64() throws IOException {
        byte[][] arr = generateBytes(Long.BYTES, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] b = arr[i];
            for (int j = 0; j < 19; j++) {
                byte[] bytes = new byte[b.length];
                System.arraycopy(b, 0, bytes, 0, b.length);
                Assert.assertEquals(BinaryStreamUtils.readDecimal64(generateInput(bytes), j),
                        new BigDecimal(new BigInteger(BinaryStreamUtils.reverse(bytes)), j));

            }
        }

        Assert.assertEquals(
                BinaryStreamUtils.readDecimal64(
                        generateInput(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF), 0),
                new BigDecimal("-1"));
        Assert.assertEquals(
                BinaryStreamUtils.readDecimal64(generateInput(0, 0, 0x64, 0xA7, 0xB3, 0xB6, 0xE0, 0x0D),
                        18),
                new BigDecimal("1.000000000000000000"));
        Assert.assertEquals(
                BinaryStreamUtils.readDecimal64(generateInput(0, 0, 0x9C, 0x58, 0x4C, 0x49, 0x1F, 0xF2),
                        18),
                new BigDecimal("-1.000000000000000000"));
    }

    @Test(groups = { "unit" })
    public void testWriteDecimal64() throws IOException {
        byte[][] arr = generateBytes(Long.BYTES, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] b = arr[i];
            for (int j = 0; j < 19; j++) {
                byte[] bytes = new byte[b.length];
                System.arraycopy(b, 0, bytes, 0, b.length);
                final int scale = j;
                BigDecimal d = new BigDecimal(new BigInteger(BinaryStreamUtils.reverse(bytes)), scale);
                if (d.toString().replaceAll("[-.]", "").replaceAll("^0+", "").length() > 18) {
                    Assert.assertThrows(IllegalArgumentException.class,
                            () -> getWrittenBytes(o -> BinaryStreamUtils.writeDecimal64(o,
                                    d, scale)));
                } else {
                    Assert.assertEquals(getWrittenBytes(
                            o -> BinaryStreamUtils.writeDecimal64(o, d, scale)), b);
                }
            }
        }

        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeDecimal64(o, new BigDecimal("-1"), 0)),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDecimal64(o,
                        new BigDecimal("0.99999999999999999999"), 18)),
                generateBytes(0xFF, 0xFF, 0x63, 0xA7, 0xB3, 0xB6, 0xE0, 0x0D));
        Assert.assertEquals(
                getWrittenBytes(
                        o -> BinaryStreamUtils.writeDecimal64(o,
                                new BigDecimal("-0.99999999999999999999"), 18)),
                generateBytes(1, 0, 0x9C, 0x58, 0x4C, 0x49, 0x1F, 0xF2));

        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeDecimal64(o, BigDecimal.ZERO, -1)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeDecimal64(o, BigDecimal.ZERO, 19)));
    }

    @Test(groups = { "unit" })
    public void testReadDecimal128() throws IOException {
        byte[][] arr = generateBytes(Long.BYTES * 2, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] b = arr[i];
            for (int j = 0; j < 39; j++) {
                byte[] bytes = new byte[b.length];
                System.arraycopy(b, 0, bytes, 0, b.length);
                Assert.assertEquals(BinaryStreamUtils.readDecimal128(generateInput(bytes), j),
                        new BigDecimal(new BigInteger(BinaryStreamUtils.reverse(bytes)), j));

            }
        }

        Assert.assertEquals(
                BinaryStreamUtils.readDecimal128(generateInput(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF), 0),
                new BigDecimal("-1"));
        Assert.assertEquals(BinaryStreamUtils.readDecimal128(
                generateInput(0, 0, 0, 0, 0x40, 0x22, 0x8A, 0x09, 0x7A, 0xC4, 0x86, 0x5A, 0xA8, 0x4C,
                        0x3B, 0x4B),
                38),
                new BigDecimal("1.00000000000000000000000000000000000000"));
        Assert.assertEquals(
                BinaryStreamUtils.readDecimal128(
                        generateInput(0, 0, 0, 0, 0xC0, 0xDD, 0x75, 0xF6, 0x85, 0x3B, 0x79,
                                0xA5, 0x57, 0xB3, 0xC4, 0xB4),
                        38),
                new BigDecimal("-1.00000000000000000000000000000000000000"));
    }

    @Test(groups = { "unit" })
    public void testWriteDecimal128() throws IOException {
        byte[][] arr = generateBytes(Long.BYTES * 2, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] b = arr[i];
            for (int j = 0; j < 39; j++) {
                byte[] bytes = new byte[b.length];
                System.arraycopy(b, 0, bytes, 0, b.length);
                final int scale = j;
                BigDecimal d = new BigDecimal(new BigInteger(BinaryStreamUtils.reverse(bytes)), scale);
                if (d.toString().replaceAll("[-.]", "").replaceAll("^0+", "").length() > 38) {
                    Assert.assertThrows(IllegalArgumentException.class,
                            () -> getWrittenBytes(o -> BinaryStreamUtils.writeDecimal128(o,
                                    d, scale)));
                } else {
                    Assert.assertEquals(getWrittenBytes(
                            o -> BinaryStreamUtils.writeDecimal128(o, d, scale)), b);
                }
            }
        }

        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeDecimal128(o, new BigDecimal("-1"), 0)),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                        0xFF, 0xFF, 0xFF,
                        0xFF));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDecimal128(o,
                        new BigDecimal("0.999999999999999999999999999999999999999999"), 38)),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0x3F, 0x22, 0x8A, 0x09, 0x7A, 0xC4, 0x86, 0x5A,
                        0xA8, 0x4C, 0x3B,
                        0x4B));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDecimal128(o,
                        new BigDecimal("-0.999999999999999999999999999999999999999999"), 38)),
                generateBytes(1, 0, 0, 0, 0xC0, 0xDD, 0x75, 0xF6, 0x85, 0x3B, 0x79, 0xA5, 0x57, 0xB3,
                        0xC4, 0xB4));

        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeDecimal128(o, BigDecimal.ZERO, -1)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeDecimal128(o, BigDecimal.ZERO, 39)));
    }

    @Test(groups = { "unit" })
    public void testReadDecimal256() throws IOException {
        byte[][] arr = generateBytes(Long.BYTES * 4, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] b = arr[i];
            for (int j = 0; j < 77; j++) {
                byte[] bytes = new byte[b.length];
                System.arraycopy(b, 0, bytes, 0, b.length);
                Assert.assertEquals(BinaryStreamUtils.readDecimal256(generateInput(bytes), j),
                        new BigDecimal(new BigInteger(BinaryStreamUtils.reverse(bytes)), j));

            }
        }

        Assert.assertEquals(
                BinaryStreamUtils.readDecimal256(generateInput(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                        0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF), 0),
                new BigDecimal("-1"));
        Assert.assertEquals(
                BinaryStreamUtils.readDecimal256(
                        generateInput(0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10, 0x95, 0x71, 0xF1, 0xA5,
                                0x75, 0x77, 0x79, 0x29,
                                0x65, 0xE8, 0xAB, 0xB4, 0x64, 0x07, 0xB5, 0x15, 0x99,
                                0x11, 0xA7, 0xCC, 0x1B, 0x16),
                        76),
                new BigDecimal("1.0000000000000000000000000000000000000000000000000000000000000000000000000000"));
        Assert.assertEquals(
                BinaryStreamUtils.readDecimal256(
                        generateInput(0, 0, 0, 0, 0, 0, 0, 0, 0, 0xF0, 0x6A, 0x8E, 0x0E, 0x5A,
                                0x8A, 0x88, 0x86, 0xD6,
                                0x9A, 0x17, 0x54, 0x4B, 0x9B, 0xF8, 0x4A, 0xEA, 0x66,
                                0xEE, 0x58, 0x33, 0xE4, 0xE9),
                        76),
                new BigDecimal("-1.0000000000000000000000000000000000000000000000000000000000000000000000000000"));
    }

    @Test(groups = { "unit" })
    public void testWriteDecimal256() throws IOException {
        byte[][] arr = generateBytes(Long.BYTES * 4, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] b = arr[i];
            for (int j = 0; j < 77; j++) {
                byte[] bytes = new byte[b.length];
                System.arraycopy(b, 0, bytes, 0, b.length);
                final int scale = j;
                BigDecimal d = new BigDecimal(new BigInteger(BinaryStreamUtils.reverse(bytes)), scale);
                if (d.toString().replaceAll("[-.]", "").replaceAll("^0+", "").length() > 76) {
                    Assert.assertThrows(IllegalArgumentException.class,
                            () -> getWrittenBytes(o -> BinaryStreamUtils.writeDecimal256(o,
                                    d, scale)));
                } else {
                    Assert.assertEquals(getWrittenBytes(
                            o -> BinaryStreamUtils.writeDecimal256(o, d, scale)), b);
                }
            }
        }

        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeDecimal256(o, new BigDecimal("-1"), 0)),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                        0xFF, 0xFF, 0xFF,
                        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                        0xFF, 0xFF, 0xFF, 0xFF,
                        0xFF));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDecimal256(o,
                        new BigDecimal(
                                "0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),
                        76)),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F, 0x95, 0x71,
                        0xF1, 0xA5, 0x75,
                        0x77, 0x79, 0x29, 0x65, 0xE8, 0xAB, 0xB4, 0x64, 0x07, 0xB5, 0x15, 0x99,
                        0x11, 0xA7, 0xCC, 0x1B,
                        0x16));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeDecimal256(o,
                        new BigDecimal(
                                "-0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),
                        76)),
                generateBytes(1, 0, 0, 0, 0, 0, 0, 0, 0, 0xF0, 0x6A, 0x8E, 0x0E, 0x5A, 0x8A, 0x88, 0x86,
                        0xD6, 0x9A,
                        0x17, 0x54, 0x4B, 0x9B, 0xF8, 0x4A, 0xEA, 0x66, 0xEE, 0x58, 0x33, 0xE4,
                        0xE9));

        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeDecimal256(o, BigDecimal.ZERO, -1)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeDecimal256(o, BigDecimal.ZERO, 77)));
    }

    @Test(groups = { "unit" })
    public void testReadFloat32() throws IOException {
        byte[][] arr = generateBytes(Integer.BYTES, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] bytes = arr[i];
            Assert.assertEquals(BinaryStreamUtils.readFloat32(generateInput(bytes)),
                    Float.intBitsToFloat(
                            new BigInteger(BinaryStreamUtils.reverse(bytes)).intValue()));
        }

        // INF and NaN
        Assert.assertEquals(BinaryStreamUtils.readFloat32(generateInput(0, 0, 0x80, 0xFF)),
                Float.NEGATIVE_INFINITY);
        Assert.assertEquals(BinaryStreamUtils.readFloat32(generateInput(0, 0, 0x80, 0x7F)),
                Float.POSITIVE_INFINITY);
        Assert.assertEquals(BinaryStreamUtils.readFloat32(generateInput(0, 0, 0xC0, 0x7F)), Float.NaN);
    }

    @Test(groups = { "unit" })
    public void testWriteFloat32() throws IOException {
        byte[][] arr = generateBytes(Integer.BYTES, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] bytes = arr[i];
            float v = Float.intBitsToFloat(new BigInteger(bytes).intValue());
            if (Float.isNaN(v)) {
                continue;
            }
            Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeFloat32(o, v)),
                    BinaryStreamUtils.reverse(bytes));
        }

        // INF and NaN
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeFloat32(o, Float.NEGATIVE_INFINITY)),
                generateBytes(0, 0, 0x80, 0xFF));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeFloat32(o, Float.POSITIVE_INFINITY)),
                generateBytes(0, 0, 0x80, 0x7F));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeFloat32(o, Float.NaN)),
                generateBytes(0, 0, 0xC0, 0x7F));
    }

    @Test(groups = { "unit" })
    public void testReadFloat64() throws IOException {
        byte[][] arr = generateBytes(Long.BYTES, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] bytes = arr[i];
            Assert.assertEquals(BinaryStreamUtils.readFloat64(generateInput(bytes)),
                    Double.longBitsToDouble(
                            new BigInteger(BinaryStreamUtils.reverse(bytes)).longValue()));
        }

        // INF and NaN
        Assert.assertEquals(BinaryStreamUtils.readFloat64(generateInput(0, 0, 0, 0, 0, 0, 0xF0, 0xFF)),
                Double.NEGATIVE_INFINITY);
        Assert.assertEquals(BinaryStreamUtils.readFloat64(generateInput(0, 0, 0, 0, 0, 0, 0xF0, 0x7F)),
                Double.POSITIVE_INFINITY);
        Assert.assertEquals(BinaryStreamUtils.readFloat64(generateInput(0, 0, 0, 0, 0, 0, 0xF8, 0x7F)),
                Double.NaN);
    }

    @Test(groups = { "unit" })
    public void testWriteFloat64() throws IOException {
        byte[][] arr = generateBytes(Long.BYTES, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] bytes = arr[i];
            double v = Double.longBitsToDouble(new BigInteger(bytes).longValue());
            if (Double.isNaN(v)) {
                continue;
            }
            Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeFloat64(o, v)),
                    BinaryStreamUtils.reverse(bytes));
        }

        // INF and NaN
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeFloat64(o, Float.NEGATIVE_INFINITY)),
                generateBytes(0, 0, 0, 0, 0, 0, 0xF0, 0xFF));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeFloat64(o, Float.POSITIVE_INFINITY)),
                generateBytes(0, 0, 0, 0, 0, 0, 0xF0, 0x7F));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeFloat64(o, Float.NaN)),
                generateBytes(0, 0, 0, 0, 0, 0, 0xF8, 0x7F));
    }

    @Test(groups = { "unit" })
    public void testReadGeoPoint() throws IOException {
        Assert.assertEquals(
                BinaryStreamUtils.readGeoPoint(
                        generateInput(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)),
                new double[] { 0D, 0D });
        Assert.assertEquals(
                BinaryStreamUtils
                        .readGeoPoint(generateInput(0, 0, 0, 0, 0, 0, 0xF0, 0xBF, 0, 0, 0, 0, 0,
                                0, 0xF0, 0xBF)),
                new double[] { -1D, -1D });
        Assert.assertEquals(
                BinaryStreamUtils
                        .readGeoPoint(generateInput(0, 0, 0, 0, 0, 0, 0xF0, 0x3F, 0, 0, 0, 0, 0,
                                0, 0xF0, 0x3F)),
                new double[] { 1D, 1D });
    }

    @Test(groups = { "unit" })
    public void testWriteGeoPoint() throws IOException {
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeGeoPoint(o, 0D, 0D)),
                generateBytes(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeGeoPoint(o, -1D, -1D)),
                generateBytes(0, 0, 0, 0, 0, 0, 0xF0, 0xBF, 0, 0, 0, 0, 0, 0, 0xF0, 0xBF));
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeGeoPoint(o, 1D, 1D)),
                generateBytes(0, 0, 0, 0, 0, 0, 0xF0, 0x3F, 0, 0, 0, 0, 0, 0, 0xF0, 0x3F));

        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeGeoPoint(o, new double[0])));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeGeoPoint(o, new double[1])));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> getWrittenBytes(o -> BinaryStreamUtils.writeGeoPoint(o, new double[3])));
    }

    @Test(groups = { "unit" })
    public void testReadGeoRing() throws IOException {
        Assert.assertEquals(BinaryStreamUtils.readGeoRing(generateInput(0)), new double[0][]);
        double[][] ring = BinaryStreamUtils
                .readGeoRing(generateInput(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(ring.length, 1);
        Assert.assertEquals(ring[0], new double[] { 0D, 0D });

        ring = BinaryStreamUtils
                .readGeoRing(generateInput(2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0xF0, 0xBF, 0, 0, 0, 0, 0, 0, 0xF0, 0xBF));
        Assert.assertEquals(ring.length, 2);
        Assert.assertEquals(ring[0], new double[] { 0D, 0D });
        Assert.assertEquals(ring[1], new double[] { -1D, -1D });

        ring = BinaryStreamUtils.readGeoRing(
                generateInput(3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xF0,
                        0xBF, 0, 0, 0,
                        0, 0, 0, 0xF0, 0xBF, 0, 0, 0, 0, 0, 0, 0xF0, 0x3F, 0, 0, 0, 0, 0, 0,
                        0xF0, 0x3F));
        Assert.assertEquals(ring.length, 3);
        Assert.assertEquals(ring[0], new double[] { 0D, 0D });
        Assert.assertEquals(ring[1], new double[] { -1D, -1D });
        Assert.assertEquals(ring[2], new double[] { 1D, 1D });
    }

    @Test(groups = { "unit" })
    public void testWriteGeoRing() throws IOException {
        Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeGeoRing(o, new double[0][])),
                generateBytes(0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeGeoRing(o,
                        new double[][] { new double[] { 0D, 0D } })),
                generateBytes(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeGeoRing(o,
                        new double[][] { new double[] { 0D, 0D }, new double[] { -1D, -1D } })),
                generateBytes(2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xF0,
                        0xBF, 0, 0, 0,
                        0, 0, 0, 0xF0, 0xBF));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeGeoRing(o,
                        new double[][] { new double[] { 0D, 0D }, new double[] { -1D, -1D },
                                new double[] { 1D, 1D } })),
                generateBytes(3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xF0,
                        0xBF, 0, 0, 0,
                        0, 0, 0, 0xF0, 0xBF, 0, 0, 0, 0, 0, 0, 0xF0, 0x3F, 0, 0, 0, 0, 0, 0,
                        0xF0, 0x3F));
    }

    @Test(groups = { "unit" })
    public void testReadGeoPolygon() throws IOException {
        Assert.assertEquals(BinaryStreamUtils.readGeoPolygon(generateInput(0)), new double[0][][]);
    }

    @Test(groups = { "unit" })
    public void testReadGeoMultiPolygon() throws IOException {
        Assert.assertEquals(BinaryStreamUtils.readGeoPolygon(generateInput(0)), new double[0][][][]);
    }

    @Test(groups = { "unit" })
    public void testReadInet4Address() throws IOException {
        Assert.assertEquals(BinaryStreamUtils.readInet4Address(generateInput(0, 0, 0, 0)).getHostAddress(),
                "0.0.0.0");
        Assert.assertEquals(BinaryStreamUtils.readInet4Address(generateInput(1, 0, 0, 0)).getHostAddress(),
                "0.0.0.1");
        Assert.assertEquals(
                BinaryStreamUtils.readInet4Address(generateInput(0xFF, 0xFF, 0xFF, 0xFF))
                        .getHostAddress(),
                "255.255.255.255");
        Assert.assertEquals(
                BinaryStreamUtils.readInet4Address(generateInput(1, 5, 0xA8, 0xC0)).getHostAddress(),
                "192.168.5.1");
    }

    @Test(groups = { "unit" })
    public void testWriteInet4Address() throws IOException {
        Assert.assertEquals(
                getWrittenBytes(
                        o -> BinaryStreamUtils.writeInet4Address(o,
                                (Inet4Address) InetAddress.getByName("0.0.0.0"))),
                generateBytes(0, 0, 0, 0));
        Assert.assertEquals(
                getWrittenBytes(
                        o -> BinaryStreamUtils.writeInet4Address(o,
                                (Inet4Address) InetAddress.getByName("0.0.0.1"))),
                generateBytes(1, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(
                o -> BinaryStreamUtils.writeInet4Address(o,
                        (Inet4Address) InetAddress.getByName("255.255.255.255"))),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF));
        Assert.assertEquals(getWrittenBytes(
                o -> BinaryStreamUtils.writeInet4Address(o,
                        (Inet4Address) InetAddress.getByName("192.168.5.1"))),
                generateBytes(1, 5, 0xA8, 0xC0));
    }

    @Test(groups = { "unit" })
    public void testReadInet6Address() throws IOException {
        Assert.assertEquals(BinaryStreamUtils
                .readInet6Address(generateInput(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
                .getHostAddress(),
                "0:0:0:0:0:0:0:0");
        Assert.assertEquals(BinaryStreamUtils
                .readInet6Address(generateInput(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1))
                .getHostAddress(),
                "0:0:0:0:0:0:0:1");
        Assert.assertEquals(
                BinaryStreamUtils
                        .readInet6Address(generateInput(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                0xFF, 0xFF, 0xFF,
                                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF))
                        .getHostAddress(),
                "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff");
        Assert.assertEquals(
                BinaryStreamUtils
                        .readInet6Address(generateInput(0x12, 0x34, 0x56, 0x78, 0x90, 0xAB,
                                0xCD, 0xEF, 0x43,
                                0x21, 0x87, 0x65, 0xBA, 0x09, 0xFE, 0xDC))
                        .getHostAddress(),
                "1234:5678:90ab:cdef:4321:8765:ba09:fedc");
        Assert.assertEquals(BinaryStreamUtils
                .readInet6Address(generateInput(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0xC0, 0xA8, 5,
                        1))
                .getHostAddress(), "0:0:0:0:0:ffff:c0a8:501");
    }

    @Test(groups = { "unit" })
    public void testWriteInet6Address() throws IOException {
        Assert.assertEquals(getWrittenBytes(
                o -> BinaryStreamUtils.writeInet6Address(o,
                        (Inet6Address) InetAddress.getByName("0:0:0:0:0:0:0:0"))),
                generateBytes(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        Assert.assertEquals(getWrittenBytes(
                o -> BinaryStreamUtils.writeInet6Address(o,
                        (Inet6Address) InetAddress.getByName("0:0:0:0:0:0:0:1"))),
                generateBytes(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeInet6Address(o,
                        (Inet6Address) InetAddress
                                .getByName("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"))),
                generateBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                        0xFF, 0xFF, 0xFF,
                        0xFF));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeInet6Address(o,
                        (Inet6Address) InetAddress
                                .getByName("1234:5678:90ab:cdef:4321:8765:ba09:fedc"))),
                generateBytes(0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF, 0x43, 0x21, 0x87, 0x65,
                        0xBA, 0x09, 0xFE,
                        0xDC));
        Assert.assertEquals(
                getWrittenBytes(o -> BinaryStreamUtils.writeInet6Address(o,
                        Inet6Address.getByAddress(null,
                                generateBytes(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF,
                                        0xC0, 0xA8, 5, 1),
                                null))),
                generateBytes(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0xC0, 0xA8, 5, 1));
    }

    @Test(groups = { "unit" })
    public void testReadString() throws IOException {
        Assert.assertEquals(new String(new byte[0]), "");
        Assert.assertEquals(new String(new byte[0], StandardCharsets.UTF_8), "");
        Assert.assertEquals(new String(new byte[0], StandardCharsets.UTF_16), "");
    }

    @Test(groups = { "unit" })
    public void testReadUuid() throws IOException {
        byte[][] arr = generateBytes(Long.BYTES * 2, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] bytes = arr[i];
            UUID uuid = BinaryStreamUtils.readUuid(generateInput(bytes));
            Assert.assertEquals(uuid.getMostSignificantBits(),
                    BinaryStreamUtils
                            .readInt64(ClickHouseInputStream.of(new ByteArrayInputStream(
                                    bytes, 0, Long.BYTES))));
            Assert.assertEquals(uuid.getLeastSignificantBits(),
                    BinaryStreamUtils.readInt64(
                            ClickHouseInputStream.of(new ByteArrayInputStream(bytes,
                                    Long.BYTES, Long.BYTES))));
        }
    }

    @Test(groups = { "unit" })
    public void testWriteUuid() throws IOException {
        byte[][] arr = generateBytes(Long.BYTES * 2, false);
        for (int i = 0; i < arr.length; i++) {
            byte[] bytes = arr[i];
            UUID uuid = BinaryStreamUtils.readUuid(generateInput(bytes));
            Assert.assertEquals(getWrittenBytes(o -> BinaryStreamUtils.writeUuid(o, uuid)), bytes);
        }
    }

    @Test(groups = { "unit" })
    public void testVarInt() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryStreamUtils.writeVarInt(out, 0);
        Assert.assertEquals(out.toByteArray(), new byte[] { (byte) 0 });
        Assert.assertEquals(BinaryStreamUtils.readVarInt(generateInput(0)), 0);

        out = new ByteArrayOutputStream();
        BinaryStreamUtils.writeVarInt(out, 1);
        Assert.assertEquals(out.toByteArray(), new byte[] { (byte) 1 });
        Assert.assertEquals(BinaryStreamUtils.readVarInt(generateInput(1)), 1);

        out = new ByteArrayOutputStream();
        BinaryStreamUtils.writeVarInt(out, 128);
        Assert.assertEquals(out.toByteArray(), new byte[] { (byte) -128, (byte) 1 });
        Assert.assertEquals(BinaryStreamUtils.readVarInt(generateInput(-128, 1)), 128);
    }
}
