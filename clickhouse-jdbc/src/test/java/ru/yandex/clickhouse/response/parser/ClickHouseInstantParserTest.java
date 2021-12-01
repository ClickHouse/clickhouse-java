package ru.yandex.clickhouse.response.parser;

import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.TimeZone;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

import static org.testng.Assert.assertEquals;

public class ClickHouseInstantParserTest {

    private TimeZone tzLosAngeles;
    private TimeZone tzBerlin;
    private ClickHouseInstantParser parser;

    @BeforeClass(groups = "unit")
    public void setUp() {
        parser = ClickHouseInstantParser.getInstance();
        tzLosAngeles = TimeZone.getTimeZone("America/Los_Angeles");
        tzBerlin = TimeZone.getTimeZone("Europe/Berlin");
    }

    @Test(groups = "unit")
    public void testParseInstantDateTime() throws Exception {
        Instant inst = parser.parse(
            ByteFragment.fromString("2020-01-20 22:23:24"),
            ClickHouseColumnInfo.parse("DateTime", "col", tzBerlin), tzBerlin);
        assertEquals(
            inst.getEpochSecond(),
            1579555404);
        inst = parser.parse(
            ByteFragment.fromString("2020-01-20 22:23:24"),
            ClickHouseColumnInfo.parse("DateTime", "col", tzLosAngeles), tzLosAngeles);
        assertEquals(
            inst.getEpochSecond(),
            1579587804);
    }

    @Test(groups = "unit")
    public void testParseInstantDateTimeColumnOverride() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            "DateTime(Europe/Berlin)", "col", TimeZone.getTimeZone("Asia/Chongqing"));
        Instant inst = parser.parse(
            ByteFragment.fromString("2020-01-20 22:23:24"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getEpochSecond(),
            1579555404);
    }

    @Test(groups = "unit")
    public void testParseInstantDate() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            "Date", "col", null);
        Instant inst = parser.parse(
            ByteFragment.fromString("2020-01-20"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getEpochSecond(),
            1579478400);
    }

    @Test(
        groups = "unit", 
        dataProvider = ClickHouseTimeParserTestDataProvider.OTHER_DATA_TYPES,
        dataProviderClass = ClickHouseTimeParserTestDataProvider.class)
    public void testParseInstantTimestampSeconds(ClickHouseDataType dataType) throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            dataType.name(), "col", null);
        Instant inst = parser.parse(
            ByteFragment.fromString("1579507200"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getEpochSecond(),
            1579507200);
        inst = parser.parse(
            ByteFragment.fromString("1579507200"), columnInfo, tzBerlin);
        assertEquals(
            inst.getEpochSecond(),
            1579507200);
    }

    @Test(
        groups = "unit", 
        dataProvider = ClickHouseTimeParserTestDataProvider.OTHER_DATA_TYPES,
        dataProviderClass = ClickHouseTimeParserTestDataProvider.class)
    public void parseInstantTimestampMillis(ClickHouseDataType dataType) throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            dataType.name(), "col", null);
        Instant inst = parser.parse(
            ByteFragment.fromString("1579507200000"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getEpochSecond(),
            1579507200);
        inst = parser.parse(
            ByteFragment.fromString("1579507200"), columnInfo, tzBerlin);
        assertEquals(
            inst.getEpochSecond(),
            1579507200);
    }

    @Test(groups = "unit")
    public void testParseInstantString() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            "String", "col", null);
        Instant inst = parser.parse(
            ByteFragment.fromString("2020-01-20T22:23:24.123"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getEpochSecond(),
            1579587804);
        inst = parser.parse(
            ByteFragment.fromString("2020-01-20T22:23:24.123+01:00"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getEpochSecond(),
            1579555404);
    }

    @Test(groups = "unit")
    public void testParseInstantUInt64Overflow() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            "UInt64", "col", null);
        Instant inst = parser.parse(
            ByteFragment.fromString(
                BigInteger.valueOf(Long.MAX_VALUE)
                    .add(BigInteger.valueOf(1337 * 100_000L))
                    .toString()),
            columnInfo,
            tzLosAngeles);
        assertEquals(
            inst.atZone(ZoneId.of("UTC")),
            ZonedDateTime.of(
                2262, 4, 11, 23, 47, 16, 988000000, ZoneId.of("UTC")));
    }

    @Test(groups = "unit")
    public void testParseInstantUInt64Millis() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            "UInt64", "col", null);
        Instant inst = parser.parse(
            ByteFragment.fromString("9223372036854"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.atZone(ZoneId.of("UTC")),
            ZonedDateTime.of(
                2262, 4, 11, 23, 47, 16, 854000000, ZoneId.of("UTC")));
    }

}
