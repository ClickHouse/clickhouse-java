package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.format.BinaryStreamUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.TimeZone;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BinaryStreamReaderTests {

    private ZoneId tzLAX;
    private ZoneId tzBER;

    @BeforeClass
    void beforeClass() {
        tzLAX = ZoneId.of("America/Los_Angeles");
        tzBER = ZoneId.of("Europe/Berlin");
    }

    @Test
    public void testCachedByteAllocator() {
        BinaryStreamReader.CachingByteBufferAllocator allocator = new BinaryStreamReader.CachingByteBufferAllocator();

        for (int i = 0; i < 6; i++) {
            int size = (int) Math.pow(2, i);
            byte[] firstAllocation = allocator.allocate(size);
            byte[] nextAllocation = allocator.allocate(size);
            Assert.assertTrue(firstAllocation == nextAllocation, "Should be the same buffer for size " + size);
        }

        for (int i = 6; i < 16; i++) {
            int size = (int) Math.pow(2, i);
            byte[] firstAllocation = allocator.allocate(size);
            byte[] nextAllocation = allocator.allocate(size);
            Assert.assertNotSame(firstAllocation, nextAllocation);
        }
    }

    @Test(dataProvider = "dateTestData")
    void readDateZonedDateTimeNoTimeZone(ZonedDateTime zdt, ZoneId writeTZ, ZoneId readTZ,
        ZonedDateTime expectedZDT) throws IOException
    {
        /*
         * Date is number of days since 1970-01-01 (unsigned)
         * ... The date value is stored without the time zone.
         */

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializerUtils.writeDate(baos, zdt, writeTZ);
        byte[] bytes = baos.toByteArray();
        Assert.assertEquals(
            BinaryStreamReader.readDate(
                new ByteArrayInputStream(bytes),
                bytes,
                TimeZone.getTimeZone(readTZ)),
            expectedZDT);
    }

    @Test(dataProvider = "dateTestData")
    void readDateOffsetDateTimeNoTimeZone(ZonedDateTime zdt, ZoneId writeTZ, ZoneId readTZ,
        ZonedDateTime expectedZDT) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializerUtils.writeDate(baos, zdt.toOffsetDateTime(), writeTZ);
        byte[] bytes = baos.toByteArray();
        Assert.assertEquals(
            BinaryStreamReader.readDate(
                new ByteArrayInputStream(bytes),
                bytes,
                TimeZone.getTimeZone(readTZ)).toOffsetDateTime(),
            expectedZDT.toOffsetDateTime());
    }

    @DataProvider(name = "dateTestData")
    private Object[][] provideDateTestData() {
        ZonedDateTime zdtLAX = ZonedDateTime.of(
            2025, 7, 20, 22, 23, 1, 232323232, tzLAX);
        ZonedDateTime zdtBER = zdtLAX.withZoneSameInstant(tzBER);
        return new Object[][] {
            // no conversion at all
            { zdtLAX, tzLAX, tzLAX, zdtLAX.truncatedTo(ChronoUnit.DAYS) },

            // write using Berlin local date -> next day
            { zdtLAX, tzBER, tzBER, zdtLAX.plusDays(1L).withZoneSameLocal(tzBER)
                .truncatedTo(ChronoUnit.DAYS) },

            // read using different time zone: local date same as original
            { zdtLAX, tzLAX, tzBER, zdtLAX.withZoneSameLocal(tzBER)
                .truncatedTo(ChronoUnit.DAYS) },

            // write using different time zone: local date same as original
            { zdtBER, tzLAX, tzBER, zdtLAX.withZoneSameLocal(tzBER)
                .truncatedTo(ChronoUnit.DAYS) }
        };

    }

    @Test(dataProvider = "dateTimeTestData")
    void readDateTime32ZonedDateTime(ZonedDateTime zdt, ZoneId writeTZ, ZoneId readTZ,
        ZonedDateTime expectedZDT) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializerUtils.writeDateTime32(baos, zdt, writeTZ);
        byte[] bytes = baos.toByteArray();
        Assert.assertEquals(
            BinaryStreamReader.readDateTime32(
                new ByteArrayInputStream(bytes),
                bytes,
                TimeZone.getTimeZone(readTZ)),
            expectedZDT.truncatedTo(ChronoUnit.SECONDS));
    }

    @Test(dataProvider = "dateTimeTestData")
    void readDateTime32OffsetDateTime(ZonedDateTime zdt, ZoneId writeTZ, ZoneId readTZ,
        ZonedDateTime expectedZDT) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializerUtils.writeDateTime32(baos, zdt.toOffsetDateTime(), writeTZ);
        byte[] bytes = baos.toByteArray();
        Assert.assertEquals(
            BinaryStreamReader.readDateTime32(
                new ByteArrayInputStream(bytes),
                bytes,
                TimeZone.getTimeZone(readTZ)).toOffsetDateTime(),
            expectedZDT.toOffsetDateTime().truncatedTo(ChronoUnit.SECONDS));
    }

    @Test(dataProvider = "dateTimeTestData")
    void readDateTime32Instant(ZonedDateTime zdt, ZoneId writeTZ, ZoneId readTZ,
        ZonedDateTime expectedZDT) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializerUtils.writeDateTime32(baos, zdt.toInstant(), writeTZ);
        byte[] bytes = baos.toByteArray();
        Assert.assertEquals(
            BinaryStreamReader.readDateTime32(
                new ByteArrayInputStream(bytes),
                bytes,
                TimeZone.getTimeZone(readTZ)),
            expectedZDT.truncatedTo(ChronoUnit.SECONDS));
    }

    @Test(dataProvider = "dateTimeTestData")
    void readDateTime64Instant(ZonedDateTime zdt, ZoneId writeTZ, ZoneId readTZ,
        ZonedDateTime expectedZDT) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializerUtils.writeDateTime64(baos, zdt.toInstant(), 9, writeTZ);
        byte[] bytes = baos.toByteArray();
        Assert.assertEquals(
            BinaryStreamReader.readDateTime64(
                new ByteArrayInputStream(bytes),
                bytes,
                9,
                TimeZone.getTimeZone(readTZ)),
            expectedZDT);
    }

    @DataProvider(name = "dateTimeTestData")
    private Object[][] provideDateTimeTestData() {
        ZonedDateTime zdtLAX = ZonedDateTime.of(
            2025, 7, 20, 22, 23, 1, 232323232, tzLAX);
        ZonedDateTime zdtBER = zdtLAX.withZoneSameInstant(tzBER);
        return new Object[][] {
            { zdtLAX, tzLAX, tzLAX, zdtLAX },
            { zdtLAX, tzBER, tzLAX, zdtLAX },
            { zdtLAX, tzLAX, tzBER, zdtBER },
            { zdtBER, tzLAX, tzBER, zdtBER }
        };
    }

    @Test
    public void testArrayValue() throws Exception {
        BinaryStreamReader.ArrayValue array = new BinaryStreamReader.ArrayValue(int.class, 10);

        for (int i = 0; i < array.length(); i++) {
            array.set(i, i);
        }

        int[] array1 = (int[]) array.getArray();
        Object[] array2 = array.getArrayOfObjects();
        Assert.assertEquals(array1.length, array2.length);
    }

    @Test
    public void testReadNullVariantReturnsNull() throws Exception {
        ClickHouseColumn column = ClickHouseColumn.of("v", "Variant(Int32, String)");
        BinaryStreamReader reader = new BinaryStreamReader(
                new ByteArrayInputStream(new byte[]{(byte) 0xFF}),
                TimeZone.getTimeZone("UTC"),
                null,
                new BinaryStreamReader.CachingByteBufferAllocator(),
                false,
                null,
                false);

        Assert.assertNull(reader.readValue(column));
    }

    @Test
    public void testNullableArrayValueUsesBoxedComponentType() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryStreamUtils.writeVarInt(baos, 2);
        BinaryStreamUtils.writeNonNull(baos);
        BinaryStreamUtils.writeFloat64(baos, 1.0);
        BinaryStreamUtils.writeNonNull(baos);
        BinaryStreamUtils.writeFloat64(baos, 2.0);

        BinaryStreamReader reader = new BinaryStreamReader(
                new ByteArrayInputStream(baos.toByteArray()),
                TimeZone.getTimeZone("UTC"),
                null,
                new BinaryStreamReader.CachingByteBufferAllocator(),
                false,
                null,
                false);

        BinaryStreamReader.ArrayValue array = (BinaryStreamReader.ArrayValue) reader.readValue(
                ClickHouseColumn.of("v", "Array(Nullable(Float64))"));

        Assert.assertEquals(array.getArray().getClass().getComponentType(), Double.class);
    }

    @Test
    public void testNullableUnsignedArrayUsesWidenedType() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryStreamUtils.writeVarInt(baos, 2);
        BinaryStreamUtils.writeNonNull(baos);
        BinaryStreamUtils.writeUnsignedInt8(baos, 10);
        BinaryStreamUtils.writeNonNull(baos);
        BinaryStreamUtils.writeUnsignedInt8(baos, 20);

        BinaryStreamReader reader = new BinaryStreamReader(
                new ByteArrayInputStream(baos.toByteArray()),
                TimeZone.getTimeZone("UTC"),
                null,
                new BinaryStreamReader.CachingByteBufferAllocator(),
                false,
                null,
                false);

        BinaryStreamReader.ArrayValue array = (BinaryStreamReader.ArrayValue) reader.readValue(
                ClickHouseColumn.of("v", "Array(Nullable(UInt8))"));

        Assert.assertEquals(array.getArray().getClass().getComponentType(), Short.class);
    }

    @Test
    public void testNullableEnumArrayUsesEnumValueType() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryStreamUtils.writeVarInt(baos, 2);
        BinaryStreamUtils.writeNonNull(baos);
        baos.write(1); // enum ordinal for 'a'
        BinaryStreamUtils.writeNonNull(baos);
        baos.write(2); // enum ordinal for 'b'

        BinaryStreamReader reader = new BinaryStreamReader(
                new ByteArrayInputStream(baos.toByteArray()),
                TimeZone.getTimeZone("UTC"),
                null,
                new BinaryStreamReader.CachingByteBufferAllocator(),
                false,
                null,
                false);

        BinaryStreamReader.ArrayValue array = (BinaryStreamReader.ArrayValue) reader.readValue(
                ClickHouseColumn.of("v", "Array(Nullable(Enum8('a'=1,'b'=2)))"));

        Assert.assertEquals(array.getArray().getClass().getComponentType(),
                BinaryStreamReader.EnumValue.class);
    }

    @Test
    public void testEmptyArrayTypes() throws Exception {
        assertEmptyArrayComponentType("Array(UInt8)", short.class);
        assertEmptyArrayComponentType("Array(Nullable(UInt8))", Short.class);
        assertEmptyArrayComponentType("Array(String)", String.class);
        assertEmptyArrayComponentType("Array(Nullable(String))", String.class);
        assertEmptyArrayComponentType("Array(Enum8('a'=1))", BinaryStreamReader.EnumValue.class);
        assertEmptyArrayComponentType("Array(Nullable(Enum8('a'=1)))", BinaryStreamReader.EnumValue.class);
        assertEmptyArrayComponentType("Array(Variant(Int32, String))", Object.class);
        assertEmptyArrayComponentType("Array(Array(String))", BinaryStreamReader.ArrayValue.class);
    }

    private void assertEmptyArrayComponentType(String columnType, Class<?> expectedComponentType) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryStreamUtils.writeVarInt(baos, 0); 

        BinaryStreamReader reader = new BinaryStreamReader(
                new ByteArrayInputStream(baos.toByteArray()),
                TimeZone.getTimeZone("UTC"),
                null,
                new BinaryStreamReader.CachingByteBufferAllocator(),
                false,
                null,
                false);

        BinaryStreamReader.ArrayValue array = (BinaryStreamReader.ArrayValue) reader.readValue(
                ClickHouseColumn.of("v", columnType));

        Assert.assertEquals(array.getArray().getClass().getComponentType(), expectedComponentType, "Failed for " + columnType);
    }

    // Native-format QBit column payloads captured from ClickHouse 26.5.1 (the block header stripped,
    // leaving the Tuple(FixedString) bit-plane bytes readQBitNative consumes) paired with the vector the
    // server encoded. Pins the decode against the server's real layout as a unit test, because the QBit
    // integration tests skip on the coverage build's older server.
    @DataProvider(name = "qbitNativeGoldenBytes")
    public static Object[][] qbitNativeGoldenBytes() {
        return new Object[][] {
                {"Float32 dim 3", "QBit(Float32, 3)", 1,
                        "0206010101010101010404000000000000000000000000000000000000000000",
                        false, new Object[] {new float[] {1f, -2f, 3.5f}}},
                {"Float32 dim 8 (full single-byte plane)", "QBit(Float32, 8)", 1,
                        "02fe010101010181796454000000000000000000000000000000000000000000",
                        false, new Object[] {new float[] {1f, -2f, 3.5f, 4f, 5f, 6f, 7f, 8f}}},
                {"Float32 dim 10 (two-byte plane, partial last byte)", "QBit(Float32, 10)", 1,
                        "02aa03fe000100010001000100010381007900640254010000000000000000000000000000000000000000000000000000000000000000000000000000000000",
                        false, new Object[] {new float[] {1f, -2f, 3.5f, -4f, 5f, -6f, 7f, -8f, 9f, -10f}}},
                {"Float32 dim 1", "QBit(Float32, 1)", 1,
                        "0001000000000100000001000100010000000000000000000000000000000000",
                        false, new Object[] {new float[] {42.5f}}},
                {"Float64 dim 3 (64 bit planes)", "QBit(Float64, 3)", 1,
                        "02060101010101010101010104040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
                        true, new Object[] {new double[] {1d, -2d, 3.5d}}},
                {"BFloat16 dim 8 (16 bit planes)", "QBit(BFloat16, 8)", 1,
                        "00fe01010101e1995500000000000000",
                        false, new Object[] {new float[] {1f, 2f, 4f, 8f, 16f, 32f, 64f, 128f}}},
                {"Float32 dim 3, three rows (column-major slicing)", "QBit(Float32, 3)", 3,
                        "000000060707010000010000010000010000010000010006010701040401000201000004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
                        false, new Object[] {
                                new float[] {1f, 2f, 3f}, new float[] {4f, 5f, 6f}, new float[] {7f, 8f, 9f}}},
        };
    }

    @Test(dataProvider = "qbitNativeGoldenBytes")
    public void testReadQBitNativeDecodesGoldenBytes(String label, String columnType, int nRows,
            String columnDataHex, boolean isDouble, Object[] expectedRows) throws Exception {
        List<Object> rows = qbitReader(fromHex(columnDataHex))
                .readQBitNative(ClickHouseColumn.of("vec", columnType), nRows);

        Assert.assertEquals(rows.size(), nRows, label);
        for (int r = 0; r < nRows; r++) {
            Object vector = ((BinaryStreamReader.ArrayValue) rows.get(r)).getArray();
            if (isDouble) {
                Assert.assertEquals((double[]) vector, (double[]) expectedRows[r], label + " row " + r);
            } else {
                Assert.assertEquals((float[]) vector, (float[]) expectedRows[r], label + " row " + r);
            }
        }
    }

    @Test
    public void testReadQBitNativeDecodesSpecialFloatValues() throws Exception {
        // NaN, +Inf, -Inf and -0.0 must survive the bit-plane transpose bit-for-bit.
        float[] vector = (float[]) ((BinaryStreamReader.ArrayValue) qbitReader(fromHex(
                "0c07070707070707070100000000000000000000000000000000000000000000"))
                .readQBitNative(ClickHouseColumn.of("vec", "QBit(Float32, 4)"), 1).get(0)).getArray();

        float[] expected = {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, -0.0f};
        Assert.assertEquals(vector.length, expected.length);
        for (int j = 0; j < expected.length; j++) {
            Assert.assertEquals(Float.floatToRawIntBits(vector[j]), Float.floatToRawIntBits(expected[j]),
                    "element " + j);
        }
    }

    @Test
    public void testReadQBitNativeRejectsUnsupportedElementType() {
        // readQBitNative reconstructs only float element types; a non-float element is rejected by its own
        // guard (the Native reader also filters these earlier), which fires before any read.
        ClickHouseColumn column = ClickHouseColumn.of("vec", "QBit(Int8, 3)");
        ClientException ex = Assert.expectThrows(ClientException.class,
                () -> qbitReader(new byte[0]).readQBitNative(column, 1));
        Assert.assertTrue(ex.getMessage().contains("Float32"),
                "Expected an unsupported-element-type message, got: " + ex.getMessage());
    }

    @Test
    public void testReadQBitNativeRejectsIntOverflowPlaneSize() {
        // nRows * ceil(dimension/8) per bit plane must not overflow a 32-bit int; the reader rejects it
        // before allocating or reading, so an empty stream is sufficient (200000 -> 25000 bytes/plane,
        // 90000 rows -> 2_250_000_000 bytes > Integer.MAX_VALUE).
        ClickHouseColumn column = ClickHouseColumn.of("vec", "QBit(Float32, 200000)");
        ClientException ex = Assert.expectThrows(ClientException.class,
                () -> qbitReader(new byte[0]).readQBitNative(column, 90000));
        Assert.assertTrue(ex.getMessage().contains("too large"),
                "Expected an overflow rejection message, got: " + ex.getMessage());
    }

    private static BinaryStreamReader qbitReader(byte[] columnData) {
        return new BinaryStreamReader(new ByteArrayInputStream(columnData), TimeZone.getTimeZone("UTC"),
                null, new BinaryStreamReader.CachingByteBufferAllocator(), false, null, false);
    }

    private static byte[] fromHex(String hex) {
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return out;
    }
}
