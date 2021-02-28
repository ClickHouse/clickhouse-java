package ru.yandex.clickhouse.response.parser;

import java.time.OffsetDateTime;
import java.util.TimeZone;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

import static org.testng.Assert.assertEquals;

public class ClickHouseOffsetDateTimeParserTest {

    private TimeZone tzLosAngeles;
    private TimeZone tzBerlin;
    private ClickHouseOffsetDateTimeParser parser;

    @BeforeClass
    public void setUp() {
        tzLosAngeles = TimeZone.getTimeZone("America/Los_Angeles");
        tzBerlin = TimeZone.getTimeZone("Europe/Berlin");
        parser = ClickHouseOffsetDateTimeParser.getInstance();
    }

    @Test
    public void testParseOffsetDateTimeDateTime() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            "DateTime", "col");
        OffsetDateTime inst = parser.parse(
            ByteFragment.fromString("2020-01-20 22:23:24"), columnInfo, tzBerlin);
        assertEquals(
            inst.toInstant().getEpochSecond(),
            1579555404);
        inst = parser.parse(
            ByteFragment.fromString("2020-01-20 22:23:24"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.toInstant().getEpochSecond(),
            1579587804);
    }

    @Test
    public void testParseOffsetDateTimeDateTimeColumnOverride() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            "DateTime(Europe/Berlin)", "col");
        OffsetDateTime inst = parser.parse(
            ByteFragment.fromString("2020-01-20 22:23:24"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.toInstant().getEpochSecond(),
            1579555404);
    }

    @Test
    public void testParseOffsetDateTimeDate() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            "Date", "col");
        OffsetDateTime inst = parser.parse(
            ByteFragment.fromString("2020-01-20"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.toInstant().getEpochSecond(),
            1579507200);
    }

    @Test(
        dataProvider = ClickHouseTimeParserTestDataProvider.OTHER_DATA_TYPES,
        dataProviderClass = ClickHouseTimeParserTestDataProvider.class)
    public void parseOffsetDateTimeTimestampSeconds(ClickHouseDataType dataType) throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            dataType.name(), "col");
        OffsetDateTime inst = parser.parse(
            ByteFragment.fromString("1579507200"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.toInstant().getEpochSecond(),
            1579507200);
        inst = parser.parse(
            ByteFragment.fromString("1579507200"), columnInfo, tzBerlin);
        assertEquals(
            inst.toInstant().getEpochSecond(),
            1579507200);
    }

    @Test(
        dataProvider = ClickHouseTimeParserTestDataProvider.OTHER_DATA_TYPES,
        dataProviderClass = ClickHouseTimeParserTestDataProvider.class)
    public void parseOffsetDateTimeTimestampMillis(ClickHouseDataType dataType) throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            dataType.name(), "col");
        OffsetDateTime inst = parser.parse(
            ByteFragment.fromString("1579507200000"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.toInstant().getEpochSecond(),
            1579507200);
        inst = parser.parse(ByteFragment.fromString(
            "1579507200"), columnInfo, tzBerlin);
        assertEquals(
            inst.toInstant().getEpochSecond(),
            1579507200);
    }

    @Test
    public void testParseOffsetDateTimeString() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            "String", "col");
        OffsetDateTime inst = parser.parse(
            ByteFragment.fromString("2020-01-20T22:23:24.123"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.toInstant().getEpochSecond(),
            1579587804);
        inst = parser.parse(
            ByteFragment.fromString("2020-01-20T22:23:24.123+01:00"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.toInstant().getEpochSecond(),
            1579555404);
    }

}
