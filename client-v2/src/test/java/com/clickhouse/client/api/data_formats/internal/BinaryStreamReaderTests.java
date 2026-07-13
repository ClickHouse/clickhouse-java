package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.format.BinaryStreamUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
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
                null);

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
                null);

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
                null);

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
                null);

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
                null);

        BinaryStreamReader.ArrayValue array = (BinaryStreamReader.ArrayValue) reader.readValue(
                ClickHouseColumn.of("v", columnType));

        Assert.assertEquals(array.getArray().getClass().getComponentType(), expectedComponentType, "Failed for " + columnType);
    }
}
