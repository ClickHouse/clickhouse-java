package ru.yandex.clickhouse.response.parser;

import java.sql.Time;
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

public class ClickHouseSQLTimeParserTest {

    private TimeZone tzBerlin;
    private TimeZone tzLosAngeles;
    private ClickHouseSQLTimeParser parser;

    @BeforeClass
    public void setUp() {
        tzBerlin = TimeZone.getTimeZone("Europe/Berlin");
        tzLosAngeles = TimeZone.getTimeZone("America/Los_Angeles");
        parser = ClickHouseSQLTimeParser.getInstance();
    }

    @Test
    public void testParseTimeTimeDate() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("Date", "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17"), columnInfo, tzBerlin),
            createExpectedTime(LocalTime.MIDNIGHT, tzBerlin));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17"), columnInfo, tzLosAngeles),
            createExpectedTime(LocalTime.MIDNIGHT, tzLosAngeles));
    }

    @Test
    public void testParseOffsetTimeDateTime() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("DateTime", "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17 22:23:24"), columnInfo, tzBerlin),
            createExpectedTime(LocalTime.of(22, 23, 24), tzBerlin));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17 22:23:24"), columnInfo, tzLosAngeles),
            createExpectedTime(LocalTime.of(22, 23, 24), tzLosAngeles));
    }

    @Test
    public void testParseOffsetTimeDateTimeColumnTimeZone() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("DateTime(Asia/Vladivostok)", "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17 22:23:24"), columnInfo, tzBerlin),
            createExpectedTime(LocalTime.of(13, 23, 24), tzBerlin));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17 22:23:24"), columnInfo, tzLosAngeles),
            createExpectedTime(LocalTime.of(04, 23, 24), tzLosAngeles));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17 22:23:24"), columnInfo,
                TimeZone.getTimeZone("Asia/Vladivostok")),
            createExpectedTime(LocalTime.of(22, 23, 24), TimeZone.getTimeZone("Asia/Vladivostok")));
    }

    @Test(
        dataProvider = ClickHouseTimeParserTestDataProvider.OTHER_DATA_TYPES,
        dataProviderClass = ClickHouseTimeParserTestDataProvider.class)
    public void testParseOffsetTimeNumber(ClickHouseDataType dataType) throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse(dataType.name(), "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("222324"), columnInfo, null),
            createExpectedTime(LocalTime.of(22, 23, 24), TimeZone.getDefault()));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2223"), columnInfo, null),
            createExpectedTime(LocalTime.of(22, 23), TimeZone.getDefault()));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("22"), columnInfo, null),
            createExpectedTime(LocalTime.of(22, 0), TimeZone.getDefault()));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("5"), columnInfo, null),
            createExpectedTime(LocalTime.of(5, 0), TimeZone.getDefault()));
        assertNull(
            parser.parse(
                ByteFragment.fromString("0"), columnInfo, null));

        try {
            parser.parse(
                ByteFragment.fromString("-42"), columnInfo, null);
            fail();
        } catch (ClickHouseException che) {
            // does not make sense
        }
        try {
            parser.parse(
                ByteFragment.fromString("42"), columnInfo, null);
            fail();
        } catch (ClickHouseException che) {
            // does not make sense
        }
    }

    @Test
    public void testParseOffsetTimeString() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse(ClickHouseDataType.String.name(), "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("22:23:24"), columnInfo, null),
            createExpectedTime(LocalTime.of(22, 23, 24), TimeZone.getDefault()));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("22:23:24.123"), columnInfo, null),
            createExpectedTime(LocalTime.of(22, 23, 24, 123 * 1000 * 1000), TimeZone.getDefault()));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("22:23"), columnInfo, null),
            createExpectedTime(LocalTime.of(22, 23), TimeZone.getDefault()));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("22"), columnInfo, null),
            createExpectedTime(LocalTime.of(22, 0), TimeZone.getDefault()));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("0022"), columnInfo, null),
            createExpectedTime(LocalTime.of(0, 22), TimeZone.getDefault()));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("002223"), columnInfo, null),
            createExpectedTime(LocalTime.of(0, 22, 23), TimeZone.getDefault()));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("00:22:23"), columnInfo, null),
            createExpectedTime(LocalTime.of(0, 22, 23), TimeZone.getDefault()));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("000000"), columnInfo, null),
            createExpectedTime(LocalTime.MIDNIGHT, TimeZone.getDefault()));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("00:00:00"), columnInfo, null),
            createExpectedTime(LocalTime.MIDNIGHT, TimeZone.getDefault()));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("00:00"), columnInfo, null),
            createExpectedTime(LocalTime.MIDNIGHT, TimeZone.getDefault()));
    }

    private static Time createExpectedTime(LocalTime expected, TimeZone timeZone) {
        return new Time(
            LocalDateTime.of(
                LocalDate.ofEpochDay(0),
                expected)
            .atZone(timeZone.toZoneId())
            .toInstant()
            .toEpochMilli());
    }

}

