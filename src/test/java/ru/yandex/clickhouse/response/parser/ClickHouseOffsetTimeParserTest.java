package ru.yandex.clickhouse.response.parser;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
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

public class ClickHouseOffsetTimeParserTest {

    private TimeZone tzBerlin;
    private TimeZone tzLosAngeles;
    private ZoneOffset offsetBerlin;
    private ZoneOffset offsetLosAngeles;
    private ZoneOffset offsetJVM;
    private ClickHouseOffsetTimeParser parser;

    @BeforeClass
    public void setUp() {
        tzBerlin = TimeZone.getTimeZone("Europe/Berlin");
        offsetBerlin = tzBerlin.toZoneId().getRules().getOffset(
            LocalDateTime.of(LocalDate.of(2020,01,17), LocalTime.MIDNIGHT));
        tzLosAngeles = TimeZone.getTimeZone("America/Los_Angeles");
        offsetLosAngeles = tzLosAngeles.toZoneId().getRules().getOffset(
            LocalDateTime.of(LocalDate.of(2020,01,17), LocalTime.MIDNIGHT));
        offsetJVM = ZoneId.systemDefault().getRules().getOffset(Instant.now());
        parser = ClickHouseOffsetTimeParser.getInstance();
    }

    @Test
    public void testParseOffsetTimeDate() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("Date", "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17"), columnInfo, tzBerlin),
            LocalTime.MIDNIGHT.atOffset(offsetBerlin));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17"), columnInfo, tzLosAngeles),
            LocalTime.MIDNIGHT.atOffset(offsetLosAngeles));
    }

    @Test
    public void testParseOffsetTimeDateTime() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("DateTime", "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17 22:23:24"), columnInfo, tzBerlin),
            LocalTime.of(22, 23, 24).atOffset(offsetBerlin));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17 22:23:24"), columnInfo, tzLosAngeles),
            LocalTime.of(22, 23, 24).atOffset(offsetLosAngeles));
    }

    @Test
    public void testParseOffsetTimeRegular() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("String", "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("13:37:42.023+07:00"), columnInfo, null),
            OffsetTime.of(
                LocalTime.of(13, 37, 42, 23 * 1000000),
                ZoneOffset.ofHours(7)));
    }

    @Test
    public void testParseOffsetTimeDateTimeColumnTimeZone() throws Exception {
        ZoneOffset offsetVladivostok = ZoneId.of("Asia/Vladivostok").getRules().getOffset(
            LocalDateTime.of(LocalDate.of(2020,01,17), LocalTime.MIDNIGHT));
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("DateTime(Asia/Vladivostok)", "col");
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17 22:23:24"), columnInfo, tzBerlin),
            LocalTime.of(22, 23, 24).atOffset(offsetVladivostok));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2020-01-17 22:23:24"), columnInfo, tzLosAngeles),
            LocalTime.of(22, 23, 24).atOffset(offsetVladivostok));
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
            LocalTime.of(22, 23, 24).atOffset(offsetJVM));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("2223"), columnInfo, null),
            LocalTime.of(22, 23).atOffset(offsetJVM));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("22"), columnInfo, null),
            LocalTime.of(22, 0).atOffset(offsetJVM));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("5"), columnInfo, null),
            LocalTime.of(5, 0).atOffset(offsetJVM));
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
            LocalTime.of(22, 23, 24).atOffset(offsetJVM));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("22:23:24.123"), columnInfo, null),
            LocalTime.of(22, 23, 24, 123 * 1000 * 1000).atOffset(offsetJVM));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("22:23"), columnInfo, null),
            LocalTime.of(22, 23).atOffset(offsetJVM));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("22"), columnInfo, null),
            LocalTime.of(22, 0).atOffset(offsetJVM));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("0022"), columnInfo, null),
            LocalTime.of(0, 22).atOffset(offsetJVM));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("002223"), columnInfo, null),
            LocalTime.of(0, 22, 23).atOffset(offsetJVM));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("00:22:23"), columnInfo, null),
            LocalTime.of(0, 22, 23).atOffset(offsetJVM));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("000000"), columnInfo, null),
            LocalTime.MIDNIGHT.atOffset(offsetJVM));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("00:00:00"), columnInfo, null),
            LocalTime.MIDNIGHT.atOffset(offsetJVM));
        assertEquals(
            parser.parse(
                ByteFragment.fromString("00:00"), columnInfo, null),
            LocalTime.MIDNIGHT.atOffset(offsetJVM));
    }

}
