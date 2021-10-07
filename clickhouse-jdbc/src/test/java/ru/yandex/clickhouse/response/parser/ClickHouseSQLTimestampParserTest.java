package ru.yandex.clickhouse.response.parser;

import java.sql.Timestamp;
import java.util.TimeZone;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

import static org.testng.Assert.assertEquals;

public class ClickHouseSQLTimestampParserTest {

    private TimeZone tzLosAngeles;
    private TimeZone tzBerlin;
    private ClickHouseSQLTimestampParser parser;

    @BeforeClass(groups = "unit")
    public void setUp() {
        tzLosAngeles = TimeZone.getTimeZone("America/Los_Angeles");
        tzBerlin = TimeZone.getTimeZone("Europe/Berlin");
        parser = ClickHouseSQLTimestampParser.getInstance();
    }

    @Test(groups = "unit")
    public void testParseTimestampDateTime() throws Exception {
        Timestamp inst = parser.parse(
            ByteFragment.fromString("2020-01-20 22:23:24"),
            ClickHouseColumnInfo.parse("DateTime", "col", tzBerlin), tzBerlin);
        assertEquals(
            inst.getTime(),
            1579555404000L);
        inst = parser.parse(
            ByteFragment.fromString("2020-01-20 22:23:24"),
            ClickHouseColumnInfo.parse("DateTime", "col", tzLosAngeles), tzLosAngeles);
        assertEquals(
            inst.getTime(),
            1579587804000L);
    }

    @Test(groups = "unit")
    public void testParseTimestampDateTimeColumnOverride() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            "DateTime(Europe/Berlin)", "col", TimeZone.getTimeZone("Asia/Chongqing"));
        Timestamp inst = parser.parse(
            ByteFragment.fromString("2020-01-20 22:23:24"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getTime(),
            1579555404000L);
    }

    @Test(groups = "unit")
    public void testParseTimestampDate() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            "Date", "col", null);
        Timestamp inst = parser.parse(
            ByteFragment.fromString("2020-01-20"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getTime(),
            1579507200000L);
    }

    @Test(
        groups = "unit", 
        dataProvider = ClickHouseTimeParserTestDataProvider.OTHER_DATA_TYPES,
        dataProviderClass = ClickHouseTimeParserTestDataProvider.class)
    public void parseTimestampTimestampSeconds(ClickHouseDataType dataType) throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            dataType.name(), "col", null);
        Timestamp inst = parser.parse(
            ByteFragment.fromString("1579507200"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getTime(),
            1579507200000L);
        inst = parser.parse(
            ByteFragment.fromString("1579507200"), columnInfo, tzBerlin);
        assertEquals(
            inst.getTime(),
            1579507200000L);
    }

    @Test(
        groups = "unit", 
        dataProvider = ClickHouseTimeParserTestDataProvider.OTHER_DATA_TYPES,
        dataProviderClass = ClickHouseTimeParserTestDataProvider.class)
    public void parseTimestampTimestampMillis(ClickHouseDataType dataType) throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            dataType.name(), "col", null);
        Timestamp inst = parser.parse(
            ByteFragment.fromString("1579507200000"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getTime(),
            1579507200000L);
        inst = parser.parse(
            ByteFragment.fromString("1579507200"), columnInfo, tzBerlin);
        assertEquals(
            inst.getTime(),
            1579507200000L);
    }

    @Test(groups = "unit")
    public void testParseTimestampString() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            "String", "col", null);
        Timestamp inst = parser.parse(
            ByteFragment.fromString("2020-01-20T22:23:24.123"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getTime(),
            1579587804123L);
        inst = parser.parse(
            ByteFragment.fromString("2020-01-20T22:23:24.123+01:00"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getTime(),
            1579555404123L);
        inst = parser.parse(
            ByteFragment.fromString("2020-01-20 22:23:24.123"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getTime(),
            1579587804123L);
        inst = parser.parse(
            ByteFragment.fromString("2020-01-20 22:23:24.123+01:00"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getTime(),
            1579555404123L);
    }

}
