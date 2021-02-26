package ru.yandex.clickhouse.response.parser;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.TimeZone;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class ClickHouseLocalDateTimeParserTest {

    private TimeZone tzLosAngeles;
    private TimeZone tzBerlin;
    private ClickHouseLocalDateTimeParser parser;

    @BeforeClass
    public void setUp() {
        tzLosAngeles = TimeZone.getTimeZone("America/Los_Angeles");
        tzBerlin = TimeZone.getTimeZone("Europe/Berlin");
        parser = ClickHouseLocalDateTimeParser.getInstance();
    }

    @Test
    public void testParseLocalDateTimeNull() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("Date", "col");
        try {
            parser.parse(
                null, columnInfo, tzBerlin);
            fail();
        } catch (NullPointerException npe) {
            // should be handled before calling the parser
        }
    }

    @Test
    public void testParseLocalDateTimeDate() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("Date", "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12"), columnInfo, null),
            LocalDate.of(2020, 1, 12).atStartOfDay());
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12"), columnInfo, tzLosAngeles),
            LocalDate.of(2020, 1, 12).atStartOfDay());
        // local stays local
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12"), columnInfo, tzBerlin),
            LocalDate.of(2020, 1, 12).atStartOfDay());
        assertNull(
            parser.parse(
                ByteFragment.fromString("0000-00-00"), columnInfo, tzLosAngeles));
    }

    @Test
    public void testParseLocalDateTimeDateTimeNullable() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("Nullable(DateTime)", "col");
        assertNull(
            parser.parse(
                ByteFragment.fromString("0000-00-00 00:00:00"), columnInfo, tzLosAngeles));
    }

    @Test
    public void testParseLocalDateTimeDateTime() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("DateTime", "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12 01:02:03"), columnInfo, null),
            LocalDateTime.of(2020, 1, 12, 1, 2, 3));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12 22:23:24"), columnInfo, tzLosAngeles),
            LocalDateTime.of(2020, 1, 12, 22, 23, 24));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12 22:23:24"), columnInfo, tzBerlin),
            LocalDateTime.of(2020, 1, 12, 22, 23, 24));
        assertNull(
            parser.parse(
                ByteFragment.fromString("0000-00-00 00:00:00"), columnInfo, null));
    }

    /*
     * No automatic conversion into any time zone, simply local date time
     */
    @Test
    public void testParseLocalDateTimeDateTimeTZColumn() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("DateTime(Europe/Berlin)", "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12 01:02:03"), columnInfo, null),
            LocalDateTime.of(2020, 1, 12, 1, 2, 3));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12 22:23:24"), columnInfo, tzLosAngeles),
            LocalDateTime.of(2020, 1, 12, 22, 23, 24));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12 22:23:24"), columnInfo, tzBerlin),
            LocalDateTime.of(2020, 1, 12, 22, 23, 24));
        assertNull(
            parser.parse(
                ByteFragment.fromString("0000-00-00 00:00:00"), columnInfo, null));
    }

    @Test(
        dataProvider = ClickHouseTimeParserTestDataProvider.OTHER_DATA_TYPES,
        dataProviderClass = ClickHouseTimeParserTestDataProvider.class)
    public void testParseLocalDateTimeNumber(ClickHouseDataType dataType) throws Exception {
        // Instant in LA time zone
        Instant instant = LocalDateTime.of(2020, 1, 12, 22, 23, 24)
            .atZone(tzLosAngeles.toZoneId())
            .toInstant();
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse(dataType.name(), "col");

        // same time zone: no problem
        assertEquals(
            parser.parse(
                ByteFragment.fromString(
                    String.valueOf(instant.getEpochSecond())),
                columnInfo,
                tzLosAngeles),
            LocalDateTime.of(2020, 1, 12, 22, 23, 24));

        // different time zone: different date
        assertEquals(
            parser.parse(
                ByteFragment.fromString(
                    String.valueOf(instant.getEpochSecond())),
                columnInfo,
                tzBerlin),
            LocalDateTime.of(2020, 1, 13, 7, 23, 24));

        try {
            parser.parse(
                ByteFragment.fromString(
                    String.valueOf(instant.getEpochSecond())),
                columnInfo,
                null);
            fail();
        } catch (ClickHouseException che) {
            // time zone for parsing required
        }

        try {
            parser.parse(
                ByteFragment.fromString("ABC"), columnInfo, null);
            fail();
        } catch (ClickHouseException che) {
            // expected
        }

        try {
            parser.parse(
                ByteFragment.fromString("3.14159265359"), columnInfo, null);
            fail();
        } catch (ClickHouseException che) {
            // expected
        }

        assertNull(parser.parse(
            ByteFragment.fromString(String.valueOf(0)), columnInfo, tzBerlin));
    }

    @Test
    public void testParseLocalDateTimeNumberNegative() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse(
                ClickHouseDataType.Int64.name(), "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString(String.valueOf(-386384400)), columnInfo, tzBerlin),
            LocalDate.of(1957, 10, 4).atStartOfDay());
    }

    @Test
    public void testParseLocalDateTimeOtherLikeDate() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse(
                ClickHouseDataType.Unknown.name(),
                "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-13"), columnInfo, null),
            LocalDate.of(2020, 1, 13).atStartOfDay());
        try {
            parser.parse(
                ByteFragment.fromString("2020-1-13"), columnInfo, null);
            fail();
        } catch (ClickHouseException che) {
            // illegal format
        }
        try {
            parser.parse(
                ByteFragment.fromString("2020-01-42"), columnInfo, null);
            fail();
        } catch (ClickHouseException che) {
            // illegal format
        }
    }

    @Test
    public void testParseLocalDateTimeOtherLikeDateTime() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse(
                ClickHouseDataType.Unknown.name(),
                "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-13 22:23:24"), columnInfo, null),
            LocalDateTime.of(LocalDate.of(2020, 1, 13), LocalTime.of(22, 23, 24)));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-13T22:23:24"), columnInfo, null),
            LocalDateTime.of(LocalDate.of(2020, 1, 13), LocalTime.of(22, 23, 24)));
        try {
            parser.parse(
                ByteFragment.fromString("2020-1-13 22:23:24"), columnInfo, null);
            fail();
        } catch (ClickHouseException che) {
            // illegal format
        }
        try {
            parser.parse(
                ByteFragment.fromString("2020-01-42 22:23:24"), columnInfo, null);
            fail();
        } catch (ClickHouseException che) {
            // illegal format
        }
    }

}
