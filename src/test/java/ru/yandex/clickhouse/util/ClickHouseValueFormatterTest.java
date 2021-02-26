package ru.yandex.clickhouse.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.TimeZone;
import java.util.UUID;

import org.testng.annotations.Test;

import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;
import ru.yandex.clickhouse.response.parser.ClickHouseValueParser;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;

public class ClickHouseValueFormatterTest {

    @Test
    public void testFormatBytesUUID() {
        UUID uuid = UUID.randomUUID();
        byte[] bytes = ByteBuffer.allocate(16)
            .putLong(uuid.getMostSignificantBits())
            .putLong(uuid.getLeastSignificantBits())
            .array();
        String formattedBytes = ClickHouseValueFormatter.formatBytes(bytes);
        byte[] reparsedBytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            reparsedBytes[i] =  (byte)
                ((Character.digit(formattedBytes.charAt(i * 4 + 2), 16) << 4)
                + Character.digit(formattedBytes.charAt(i * 4 + 3), 16));
        }
        assertEquals(reparsedBytes, bytes);
    }

    @Test
    public void testFormatBytesHelloWorld() throws Exception {
        byte[] bytes = "HELLO WORLD".getBytes("UTF-8");
        String formattedBytes = ClickHouseValueFormatter.formatBytes(bytes);
        byte[] reparsedBytes = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            reparsedBytes[i] =  (byte)
                ((Character.digit(formattedBytes.charAt(i * 4 + 2), 16) << 4)
                + Character.digit(formattedBytes.charAt(i * 4 + 3), 16));
        }
        assertEquals(
            new String(reparsedBytes, Charset.forName("UTF-8")),
            "HELLO WORLD");
    }

    @Test
    public void testFormatBytesNull() {
        assertNull(ClickHouseValueFormatter.formatBytes(null));
    }

    @Test
    public void testFormatBytesEmpty() {
        assertEquals(ClickHouseValueFormatter.formatBytes(new byte[0]), "");
    }

    @Test
    public void testFormatLocalTime() {
        LocalTime l = LocalTime.parse("13:37:42.023");
        assertEquals(ClickHouseValueFormatter.formatLocalTime(l), "13:37:42");
    }

    @Test
    public void testFormatOffsetTime() {
        OffsetTime o = OffsetTime.of(
            LocalTime.parse("13:37:42.023"),
            ZoneOffset.ofHoursMinutes(1, 7));
        assertEquals(
            ClickHouseValueFormatter.formatOffsetTime(o),
            "13:37:42.023+01:07");
    }

    @Test
    public void testFormatLocalDate() {
        LocalDate l = LocalDate.of(2020, 1, 7);
        assertEquals(ClickHouseValueFormatter.formatLocalDate(l), "2020-01-07");
    }

    @Test
    public void testFormatLocalDateTime() {
        LocalDateTime l = LocalDateTime.of(2020, 1, 7, 13, 37, 42, 107);
        assertEquals(ClickHouseValueFormatter.formatLocalDateTime(l), "2020-01-07 13:37:42");
    }

    @Test
    public void testFormatOffsetDateTime() {
        OffsetDateTime o = OffsetDateTime.of(
            LocalDateTime.of(2020, 1, 7, 13, 37, 42, 107),
            ZoneOffset.ofHoursMinutes(2, 30));
        assertEquals(
            ClickHouseValueFormatter.formatOffsetDateTime(o, TimeZone.getTimeZone("Europe/Moscow")),
            "2020-01-07 14:07:42");
    }

    @Test
    public void testFormatZonedDateTime() {
        ZonedDateTime z = ZonedDateTime.of(
            LocalDateTime.of(2020, 1, 7, 13, 37, 42, 107),
            ZoneId.of("America/Los_Angeles"));
        assertEquals(
            ClickHouseValueFormatter.formatZonedDateTime(z, TimeZone.getTimeZone("Europe/Moscow")),
            "2020-01-08 00:37:42");
    }

    @Test
    public void testFormatObject() throws Exception {
        TimeZone tzUTC = TimeZone.getTimeZone("UTC");
        assertEquals(ClickHouseValueFormatter.formatObject(Byte.valueOf("42"), tzUTC, tzUTC), "42");
        assertEquals(ClickHouseValueFormatter.formatObject("foo", tzUTC, tzUTC), "foo");
        assertEquals(ClickHouseValueFormatter.formatObject("f'oo\to", tzUTC, tzUTC), "f\\'oo\\to");
        assertEquals(ClickHouseValueFormatter.formatObject(BigDecimal.valueOf(42.23), tzUTC, tzUTC), "42.23");
        assertEquals(ClickHouseValueFormatter.formatObject(BigInteger.valueOf(1337), tzUTC, tzUTC), "1337");
        assertEquals(ClickHouseValueFormatter.formatObject(Short.valueOf("-23"), tzUTC, tzUTC), "-23");
        assertEquals(ClickHouseValueFormatter.formatObject(Integer.valueOf(1337), tzUTC, tzUTC), "1337");
        assertEquals(ClickHouseValueFormatter.formatObject(Long.valueOf(-23L), tzUTC, tzUTC), "-23");
        assertEquals(ClickHouseValueFormatter.formatObject(Float.valueOf(4.2f), tzUTC, tzUTC), "4.2");
        assertEquals(ClickHouseValueFormatter.formatObject(Double.valueOf(23.42), tzUTC, tzUTC), "23.42");
        byte[] bytes = "HELLO WORLD".getBytes("UTF-8");
        assertEquals(ClickHouseValueFormatter.formatObject(bytes, tzUTC, tzUTC),
            "\\x48\\x45\\x4C\\x4C\\x4F\\x20\\x57\\x4F\\x52\\x4C\\x44");
        Date d = new Date(1557136800000L);
        assertEquals(ClickHouseValueFormatter.formatObject(d, tzUTC, tzUTC), "2019-05-06");
        Time t = new Time(1557136800000L);
        assertEquals(ClickHouseValueFormatter.formatObject(t, tzUTC, tzUTC), "10:00:00");
        Timestamp ts = new Timestamp(1557136800000L);
        assertEquals(ClickHouseValueFormatter.formatObject(ts, tzUTC, tzUTC), "2019-05-06 10:00:00");
        assertEquals(ClickHouseValueFormatter.formatObject(Boolean.TRUE, tzUTC, tzUTC), "1");
        UUID u = UUID.randomUUID();
        assertEquals(ClickHouseValueFormatter.formatObject(u, tzUTC, tzUTC), u.toString());
        int[] ints = new int[] { 23, 42 };
        assertEquals(ClickHouseValueFormatter.formatObject(ints, tzUTC, tzUTC), "[23,42]");
        assertEquals(ClickHouseValueFormatter.formatObject(new Timestamp[] {ts, ts}, tzUTC, tzUTC),
            "['2019-05-06 10:00:00','2019-05-06 10:00:00']");
        assertEquals(
            ClickHouseValueFormatter.formatObject(LocalTime.parse("13:37:42.023"), tzUTC, tzUTC),
            "13:37:42");
        OffsetTime offTime = OffsetTime.of(
            LocalTime.parse("13:37:42.023"),
            ZoneOffset.ofHoursMinutes(1, 7));
        assertEquals(
            ClickHouseValueFormatter.formatObject(offTime, tzUTC, TimeZone.getTimeZone("Europe/Moscow")),
            "13:37:42.023+01:07");
        assertEquals(
            ClickHouseValueFormatter.formatObject(offTime, tzUTC, tzUTC),
            "13:37:42.023+01:07");
        assertEquals(
            ClickHouseValueFormatter.formatObject(
                OffsetTime.of(
                    LocalTime.parse("13:37:42.023"),
                    ZoneOffset.ofHoursMinutes(1, 7)),
                tzUTC,
                TimeZone.getTimeZone("Europe/Moscow")),
            "13:37:42.023+01:07");
        assertEquals(
            ClickHouseValueFormatter.formatObject(LocalDate.of(2020, 1, 7), tzUTC, tzUTC),
            "2020-01-07");
        assertEquals(
            ClickHouseValueFormatter.formatObject(
                LocalDateTime.of(2020, 1, 7, 13, 37, 42, 23),
                tzUTC,
                tzUTC),
            "2020-01-07 13:37:42");
        OffsetDateTime o = OffsetDateTime.of(
            LocalDateTime.of(2020, 1, 7, 13, 37, 42, 107),
            ZoneOffset.ofHoursMinutes(2, 30));
        assertEquals(
            ClickHouseValueFormatter.formatObject(o, tzUTC, TimeZone.getTimeZone("Europe/Moscow")),
            "2020-01-07 14:07:42");
        assertEquals(
            ClickHouseValueFormatter.formatObject(o, tzUTC, tzUTC),
            "2020-01-07 11:07:42");
        ZonedDateTime z = ZonedDateTime.of(
            LocalDateTime.of(2020, 1, 7, 13, 37, 42, 107),
            ZoneId.of("America/Los_Angeles"));
        assertEquals(
            ClickHouseValueFormatter.formatObject(z, tzUTC, TimeZone.getTimeZone("Europe/Moscow")),
            "2020-01-08 00:37:42");
        assertEquals(
            ClickHouseValueFormatter.formatObject(z, tzUTC, tzUTC),
            "2020-01-07 21:37:42");
    }

    @Test
    public void testRoundTripLocalDate() throws Exception {
        LocalDate l0 = LocalDate.of(1957, 10, 4);
        LocalDate l1 = (LocalDate) ClickHouseValueParser.getParser(LocalDate.class).parse(
            ByteFragment.fromString(ClickHouseValueFormatter.formatLocalDate(l0)),
            ClickHouseColumnInfo.parse("Date", "col"), null);
        assertEquals(l1, l0);
        l1 = (LocalDate) ClickHouseValueParser.getParser(LocalDate.class).parse(
            ByteFragment.fromString(ClickHouseValueFormatter.formatLocalDate(l0)),
            ClickHouseColumnInfo.parse("String", "col"), null);
        assertEquals(l1, l0);
    }

    @Test
    public void testRoundTripLocalTime() throws Exception {
        LocalTime l0 = LocalTime.of(13, 37, 42);
        LocalTime l1 = (LocalTime) ClickHouseValueParser.getParser(LocalTime.class).parse(
            ByteFragment.fromString(ClickHouseValueFormatter.formatLocalTime(l0)),
            ClickHouseColumnInfo.parse("String", "col"), null);
        assertEquals(l1, l0);
    }

    @Test
    public void testRoundTripLocalDateTime() throws Exception {
        LocalDateTime l0 = LocalDateTime.of(1957, 10, 4, 13, 37, 42);
        LocalDateTime l1 = (LocalDateTime) ClickHouseValueParser.getParser(LocalDateTime.class)
            .parse(ByteFragment.fromString(ClickHouseValueFormatter.formatLocalDateTime(l0)),
            ClickHouseColumnInfo.parse("DateTime", "col"), null);
        assertEquals(l1, l0);
        l1 = (LocalDateTime) ClickHouseValueParser.getParser(LocalDateTime.class)
            .parse(ByteFragment.fromString(ClickHouseValueFormatter.formatLocalDateTime(l0)),
            ClickHouseColumnInfo.parse("String", "col"), null);
        assertEquals(l1, l0);
    }

    @Test
    public void testRoundTripOffsetTime() throws Exception {
        ZoneOffset offset = ZoneId.of("Asia/Vladivostok")
            .getRules().getOffset(Instant.now());
        OffsetTime ot0 = OffsetTime.of(LocalTime.of(13, 37, 42), offset);
        OffsetTime ot1 = (OffsetTime) ClickHouseValueParser.getParser(OffsetTime.class)
            .parse(ByteFragment.fromString(ClickHouseValueFormatter.formatOffsetTime(ot0)),
            ClickHouseColumnInfo.parse("String", "col"), null);
        assertEquals(ot1, ot0);
    }

    @Test
    public void testRoundTripOffsetDateTime() throws Exception {
        TimeZone tzBerlin = TimeZone.getTimeZone("Europe/Berlin");
        TimeZone tzLosAngeles = TimeZone.getTimeZone("America/Los_Angeles");
        TimeZone tzVladivostok = TimeZone.getTimeZone("Asia/Vladivostok");
        LocalDateTime ldt = LocalDateTime.of(1957, 10, 4, 13, 37, 42);
        OffsetDateTime odt0 = OffsetDateTime.of(
            ldt,
            tzVladivostok.toZoneId().getRules().getOffset(ldt));

        OffsetDateTime odt1 = (OffsetDateTime) ClickHouseValueParser.getParser(OffsetDateTime.class)
            .parse(ByteFragment.fromString(ClickHouseValueFormatter.formatOffsetDateTime(odt0, tzLosAngeles)),
            ClickHouseColumnInfo.parse("String", "col"), tzBerlin);
        assertEquals(odt1, OffsetDateTime.of(
            LocalDateTime.of(1957, 10, 3, 19, 37, 42),
            tzBerlin.toZoneId().getRules().getOffset(ldt)));

        odt1 = (OffsetDateTime) ClickHouseValueParser.getParser(OffsetDateTime.class)
            .parse(ByteFragment.fromString(ClickHouseValueFormatter.formatOffsetDateTime(odt0, tzLosAngeles)),
            ClickHouseColumnInfo.parse("DateTime", "col"), tzBerlin);
        assertEquals(odt1, OffsetDateTime.of(
            LocalDateTime.of(1957, 10, 3, 19, 37, 42),
            tzBerlin.toZoneId().getRules().getOffset(ldt)));

        odt1 = (OffsetDateTime) ClickHouseValueParser.getParser(OffsetDateTime.class)
            .parse(ByteFragment.fromString(ClickHouseValueFormatter.formatOffsetDateTime(odt0, tzLosAngeles)),
            ClickHouseColumnInfo.parse("DateTime", "col"), tzLosAngeles);
        assertEquals(odt1, OffsetDateTime.of(
            LocalDateTime.of(1957, 10, 3, 19, 37, 42),
            tzLosAngeles.toZoneId().getRules().getOffset(ldt)));
    }

    @Test
    public void testRoundTripZonedDateTime() throws Exception {
        TimeZone tzBerlin = TimeZone.getTimeZone("Europe/Berlin");
        TimeZone tzLosAngeles = TimeZone.getTimeZone("America/Los_Angeles");
        TimeZone tzVladivostok = TimeZone.getTimeZone("Asia/Vladivostok");
        LocalDateTime ldt = LocalDateTime.of(1957, 10, 4, 13, 37, 42);
        ZonedDateTime odt0 = ZonedDateTime.of(
            ldt,
            tzVladivostok.toZoneId());

        ZonedDateTime odt1 = (ZonedDateTime) ClickHouseValueParser.getParser(ZonedDateTime.class)
            .parse(ByteFragment.fromString(ClickHouseValueFormatter.formatZonedDateTime(odt0, tzLosAngeles)),
            ClickHouseColumnInfo.parse("String", "col"), tzBerlin);
        assertEquals(odt1, ZonedDateTime.of(
            LocalDateTime.of(1957, 10, 3, 19, 37, 42),
            tzBerlin.toZoneId()));

        odt1 = (ZonedDateTime) ClickHouseValueParser.getParser(ZonedDateTime.class)
            .parse(ByteFragment.fromString(ClickHouseValueFormatter.formatZonedDateTime(odt0, tzLosAngeles)),
            ClickHouseColumnInfo.parse("DateTime", "col"), tzBerlin);
        assertEquals(odt1, ZonedDateTime.of(
            LocalDateTime.of(1957, 10, 3, 19, 37, 42),
            tzBerlin.toZoneId()));

        odt1 = (ZonedDateTime) ClickHouseValueParser.getParser(ZonedDateTime.class)
            .parse(ByteFragment.fromString(ClickHouseValueFormatter.formatZonedDateTime(odt0, tzLosAngeles)),
            ClickHouseColumnInfo.parse("DateTime", "col"), tzLosAngeles);
        assertEquals(odt1, ZonedDateTime.of(
            LocalDateTime.of(1957, 10, 3, 19, 37, 42),
            tzLosAngeles.toZoneId()));
    }

    @Test
    public void testRoundTripSQLTimestamp() throws Exception {
        TimeZone tzLosAngeles = TimeZone.getTimeZone("America/Los_Angeles");
        TimeZone tzBerlin = TimeZone.getTimeZone("Europe/Berlin");
        Timestamp t0 = new Timestamp(1497474018000L);
        Timestamp t1 = (Timestamp) ClickHouseValueParser.getParser(Timestamp.class)
            .parse(ByteFragment.fromString(ClickHouseValueFormatter.formatTimestamp(t0, tzLosAngeles)),
                ClickHouseColumnInfo.parse("String", "col"), tzLosAngeles);
        assertEquals(t1, t0);

        t1 = (Timestamp) ClickHouseValueParser.getParser(Timestamp.class)
            .parse(ByteFragment.fromString(ClickHouseValueFormatter.formatTimestamp(t0, tzLosAngeles)),
                ClickHouseColumnInfo.parse("DateTime", "col"), tzLosAngeles);
        assertEquals(t1, t0);

        t1 = (Timestamp) ClickHouseValueParser.getParser(Timestamp.class)
            .parse(ByteFragment.fromString(ClickHouseValueFormatter.formatTimestamp(t0, tzLosAngeles)),
                ClickHouseColumnInfo.parse("String", "col"), tzBerlin);
        Timestamp t2 = new Timestamp(
                ZonedDateTime.of(LocalDateTime.of(2017,6, 14, 14, 0, 18),
                tzBerlin.toZoneId())
            .toInstant()
            .toEpochMilli());
        assertNotEquals(t1, t0);
        assertEquals(t1, t2);
    }

    @Test
    public void testRoundTripSQLTime() throws Exception {
        TimeZone tzLosAngeles = TimeZone.getTimeZone("America/Los_Angeles");
        TimeZone tzBerlin = TimeZone.getTimeZone("Europe/Berlin");
        Time t0 = Time.valueOf(LocalTime.of(13, 37, 42));
        Time t1 = (Time) ClickHouseValueParser.getParser(Time.class)
            .parse(ByteFragment.fromString(ClickHouseValueFormatter.formatTime(t0, tzLosAngeles)),
                ClickHouseColumnInfo.parse("String", "col"), tzLosAngeles);
        assertEquals(t1, t0);

        t1 = (Time) ClickHouseValueParser.getParser(Time.class)
            .parse(ByteFragment.fromString(ClickHouseValueFormatter.formatTime(t0, tzLosAngeles)),
                ClickHouseColumnInfo.parse("String", "col"), tzBerlin);
        assertEquals(
            Instant.ofEpochMilli(t1.getTime()),
            Instant.ofEpochMilli(t0.getTime()).minus(Duration.ofHours(9)));
    }

    @Test
    public void testRoundTripSQLDate() throws Exception {
        TimeZone tzLosAngeles = TimeZone.getTimeZone("America/Los_Angeles");
        TimeZone tzBerlin = TimeZone.getTimeZone("Europe/Berlin");

        /*
         * This is a case with intended discrepancy:
         *
         * Input:  2017-06-14T14:00:18-07:00
         * Output: 2017-06-14T00:00:00-07:00
         *
         * See Javadoc for java.sql.Date:
         *
         * the millisecond values wrapped by a java.sql.Date instance must be "normalized"
         * by setting the hours, minutes, seconds, and milliseconds to zero in the particular
         * time zone with which the instance is associated.
         */
        Date t0 = new Date(1497474018000L);
        Date t1 = (Date) ClickHouseValueParser.getParser(Date.class)
            .parse(ByteFragment.fromString(ClickHouseValueFormatter.formatDate(t0, tzLosAngeles)),
                ClickHouseColumnInfo.parse("String", "col"), tzLosAngeles);
        assertNotEquals(t1, t0);
        assertEquals(
            t1,
            new Date(OffsetDateTime.parse("2017-06-14T00:00:00-07:00")
                .toInstant()
                .toEpochMilli()));

        // now try parsing with a different time zone. The values should be "normalized"
        // for the particular time zone.

        Date t2 = (Date) ClickHouseValueParser.getParser(Date.class)
            .parse(ByteFragment.fromString(ClickHouseValueFormatter.formatDate(t0, tzLosAngeles)),
                ClickHouseColumnInfo.parse("String", "col"), tzBerlin);
        assertNotEquals(t2, t0);
        assertEquals(
            t2,
            new Date(OffsetDateTime.parse("2017-06-14T00:00:00+02:00")
                .toInstant()
                .toEpochMilli()));
    }

}
