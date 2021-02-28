package ru.yandex.clickhouse.response.parser;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.TimeZone;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class ClickHouseLocalDateParserTest {

    private TimeZone tzLosAngeles;
    private TimeZone tzBerlin;
    private ClickHouseLocalDateParser parser;

    @BeforeClass
    public void setUp() {
        tzLosAngeles = TimeZone.getTimeZone("America/Los_Angeles");
        tzBerlin = TimeZone.getTimeZone("Europe/Berlin");
        parser = ClickHouseLocalDateParser.getInstance();
    }

    @Test
    public void testParseLocalDateNull() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("Date", "col");
        try {
            parser.parse(
                null, columnInfo, tzBerlin);
        } catch (NullPointerException npe) {
            // should be handled before calling the parser
        }
    }

    @Test
    public void testParseLocalDateDate() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("Date", "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12"), columnInfo, null),
            LocalDate.of(2020, 1, 12));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12"), columnInfo, tzLosAngeles),
            LocalDate.of(2020, 1, 12));
        // local stays local
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12"), columnInfo, tzBerlin),
            LocalDate.of(2020, 1, 12));
        assertNull(
            parser.parse(
                ByteFragment.fromString("0000-00-00"), columnInfo, tzLosAngeles));
    }

    @Test
    public void testParseLocalDateDateNullable() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("Nullable(Date)", "col");
        assertNull(
            parser.parse(
                ByteFragment.fromString("0000-00-00"), columnInfo, tzLosAngeles));
    }

    @Test
    public void testParseLocalDateDateTime() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("DateTime", "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12 01:02:03"), columnInfo, null),
            LocalDate.of(2020, 1, 12));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12 22:23:24"), columnInfo, tzLosAngeles),
            LocalDate.of(2020, 1, 12));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12 22:23:24"), columnInfo, tzBerlin),
            LocalDate.of(2020, 1, 12));
        assertNull(
            parser.parse(
                ByteFragment.fromString("0000-00-00 00:00:00"), columnInfo, null));
    }

    @Test
    public void testParseLocalDateDateTimeTZColumn() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("DateTime(Europe/Berlin)", "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12 01:02:03"), columnInfo, null),
            LocalDate.of(2020, 1, 12));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12 22:23:24"), columnInfo, tzLosAngeles),
            LocalDate.of(2020, 1, 12));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-12 22:23:24"), columnInfo, tzBerlin),
            LocalDate.of(2020, 1, 12));
        assertNull(
            parser.parse(
                ByteFragment.fromString("0000-00-00 00:00:00"), columnInfo, null));
    }

    @Test(
        dataProvider = ClickHouseTimeParserTestDataProvider.OTHER_DATA_TYPES,
        dataProviderClass = ClickHouseTimeParserTestDataProvider.class)
    public void testParseLocalDateNumber(ClickHouseDataType dataType) throws Exception {

        // Instant in LA time zone
        Instant instant = LocalDateTime.of(2020, 1, 12, 22, 23, 24)
            .atZone(tzLosAngeles.toZoneId())
            .toInstant();
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse(dataType.name(), "col");

        // same time zone: no problem
        assertEquals(
            parser.parse(
                ByteFragment.fromString(String.valueOf(instant.getEpochSecond())),
                columnInfo,
                tzLosAngeles),
            LocalDate.of(2020, 1, 12));

        // different time zone: different date
        assertEquals(
            parser.parse(
                ByteFragment.fromString(String.valueOf(instant.getEpochSecond())),
                columnInfo,
                tzBerlin),
            LocalDate.of(2020, 1, 13));

        try {
            parser.parse(
                ByteFragment.fromString("ABC"), columnInfo, null);
            fail();
        } catch (SQLException sqle) {
            // expected
        }

        try {
            parser.parse(
                ByteFragment.fromString("3.14159265359"), columnInfo, null);
            fail();
        } catch (SQLException sqle) {
            // expected
        }

        assertNull(
            parser.parse(
                ByteFragment.fromString(String.valueOf(0)), columnInfo, tzBerlin));
    }

    @Test
    public void testParseLocalDateNumberNegative() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse(
                ClickHouseDataType.Int64.name(), "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString(String.valueOf(-386384400)),
                    columnInfo, tzBerlin),
            LocalDate.of(1957, 10, 4));
    }

    @Test
    public void testParseLocalDateOtherLikeDate() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse(
                ClickHouseDataType.Unknown.name(),
                "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-13"), columnInfo, null),
            LocalDate.of(2020, 1, 13));
        try {
            parser.parse(
                ByteFragment.fromString("2020-1-13"), columnInfo, null);
            fail();
        } catch (SQLException sqle) {
            // expected
        }
        try {
            parser.parse(
                ByteFragment.fromString("2020-01-42"), columnInfo, null);
            fail();
        } catch (SQLException sqle) {
            // expected
        }
    }

    @Test
    public void testParseLocalDateOtherLikeDateTime() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse(
                ClickHouseDataType.Unknown.name(),
                "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-13 22:23:24"), columnInfo, null),
            LocalDate.of(2020, 1, 13));
        try {
            parser.parse(
                ByteFragment.fromString("2020-1-13 22:23:24"), columnInfo, null);
            fail();
        } catch (SQLException sqle) {
            // expected
        }
        try {
            parser.parse(
                ByteFragment.fromString("2020-01-42 22:23:24"), columnInfo, null);
            fail();
        } catch (SQLException sqle) {
            // expected
        }
    }

}
