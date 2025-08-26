package com.clickhouse.client.api.data_formats.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
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
}
