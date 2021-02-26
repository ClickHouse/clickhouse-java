package ru.yandex.clickhouse.response.parser;

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

public class ClickHouseLocalTimeParserTest {

    private TimeZone tzBerlin;
    private TimeZone tzLosAngeles;
    private ClickHouseLocalTimeParser parser;

    @BeforeClass
    public void setUp() {
        tzBerlin = TimeZone.getTimeZone("Europe/Berlin");
        tzLosAngeles = TimeZone.getTimeZone("America/Los_Angeles");
        parser = ClickHouseLocalTimeParser.getInstance();
    }

    @Test
    public void testParseLocalTimeDate() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("Date", "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17"), columnInfo, tzBerlin),
            LocalTime.MIDNIGHT);
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17"), columnInfo, tzLosAngeles),
            LocalTime.MIDNIGHT);
    }

    @Test
    public void testParseLocalTimeDateTime() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("DateTime", "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17 22:23:24"), columnInfo, tzBerlin),
            LocalTime.of(22, 23, 24));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17 22:23:24"), columnInfo, tzLosAngeles),
            LocalTime.of(22, 23, 24));
    }

    /*
     * No automatic conversion into any time zone, simply local  time
     */
    @Test
    public void testParseLocalTimeDateTimeColumnTimeZone() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("DateTime(Asia/Vladivostok)", "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17 22:23:24"), columnInfo, tzBerlin),
            LocalTime.of(22, 23, 24));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17 22:23:24"), columnInfo, tzLosAngeles),
            LocalTime.of(22, 23, 24));
    }

    @Test(
        dataProvider = ClickHouseTimeParserTestDataProvider.OTHER_DATA_TYPES,
        dataProviderClass = ClickHouseTimeParserTestDataProvider.class)
    public void testParseLocalTimeNumber(ClickHouseDataType dataType) throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse(dataType.name(), "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("222324"), columnInfo, null),
            LocalTime.of(22, 23, 24));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2223"), columnInfo, null),
            LocalTime.of(22, 23));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("22"), columnInfo, null),
            LocalTime.of(22, 0));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("5"), columnInfo, null),
            LocalTime.of(5, 0));
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
    public void testParseLocalTimeString() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse(ClickHouseDataType.String.name(), "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("22:23:24"), columnInfo, null),
            LocalTime.of(22, 23, 24));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("22:23:24.123"), columnInfo, null),
            LocalTime.of(22, 23, 24, 123 * 1000 * 1000));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("22:23"), columnInfo, null),
            LocalTime.of(22, 23));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("22"), columnInfo, null),
            LocalTime.of(22, 0));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("0022"), columnInfo, null),
            LocalTime.of(0, 22));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("002223"), columnInfo, null),
            LocalTime.of(0, 22, 23));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("00:22:23"), columnInfo, null),
            LocalTime.of(0, 22, 23));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("000000"), columnInfo, null),
            LocalTime.MIDNIGHT);
        assertEquals(
            parser.parse(
                ByteFragment.fromString("00:00:00"), columnInfo, null),
            LocalTime.MIDNIGHT);
        assertEquals(
            parser.parse(
                ByteFragment.fromString("00:00"), columnInfo, null),
            LocalTime.MIDNIGHT);
        assertEquals(
            parser.parse(
                ByteFragment.fromString("13:37:42"), columnInfo, null),
            LocalTime.of(13, 37, 42));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("13:37:42.107"), columnInfo, null),
            LocalTime.of(13, 37, 42, 107000000));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("13:37:42"), columnInfo, tzBerlin),
            LocalTime.of(13, 37, 42));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("13:37:42"), columnInfo, tzLosAngeles),
            LocalTime.of(13, 37, 42));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("13:37:42+02:00"), columnInfo, null),
            LocalTime.of(13, 37, 42));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("13:37:42Z"), columnInfo, null),
            LocalTime.of(13, 37, 42));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("13:37:42+02:00"), columnInfo, tzLosAngeles),
            LocalTime.of(13, 37, 42));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("13:37:42+02:00"), columnInfo, tzBerlin),
            LocalTime.of(13, 37, 42));
    }

}
