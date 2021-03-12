package ru.yandex.clickhouse.integration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.zone.ZoneRules;
import java.util.Calendar;
import java.util.Objects;
import java.util.TimeZone;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseContainerForTest;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;
import ru.yandex.clickhouse.response.parser.ClickHouseValueParser;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

public class ClickHouseDataTypeTest {
    private ClickHouseConnection conn;

    private LocalDate instantToLocalDate(Instant instant, ZoneId zone) {
        Objects.requireNonNull(instant, "instant");
        Objects.requireNonNull(zone, "zone");
        ZoneRules rules = zone.getRules();
        ZoneOffset offset = rules.getOffset(instant);
        long localSecond = instant.getEpochSecond() + offset.getTotalSeconds();
        long localEpochDay = Math.floorDiv(localSecond, 24 * 3600);
        return LocalDate.ofEpochDay(localEpochDay);
    }

    private LocalTime instantToLocalTime(Instant instant, ZoneId zone) {
        Objects.requireNonNull(instant, "instant");
        Objects.requireNonNull(zone, "zone");
        ZoneOffset offset = zone.getRules().getOffset(instant);
        long localSecond = instant.getEpochSecond() + offset.getTotalSeconds();
        int secondsADay = 24 * 3600;
        int secsOfDay = (int) (localSecond - Math.floorDiv(localSecond, secondsADay) * secondsADay);
        return LocalTime.ofNanoOfDay(secsOfDay * 1000_000_000L + instant.getNano());
    }

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseDataSource dataSource = ClickHouseContainerForTest.newDataSource();
        conn = (ClickHouseConnection) dataSource.getConnection();
    }

    @AfterTest
    public void tearDown() throws Exception {
        conn.close();
    }

    @DataProvider(name = "uniqTimeZones")
    public static Object[][] provideUniqTimeZones() {
        return new Object[][] { new String[] { "Asia/Chongqing" }, new String[] { "America/Los_Angeles" },
                new String[] { "Europe/Moscow" }, new String[] { "Etc/UTC" }, new String[] { "Europe/Berlin" } };
    }

    @DataProvider(name = "testTimeZones")
    public static Object[][] provieTestTimeZones() {
        return new Object[][] { new String[] { "Asia/Chongqing", "America/Los_Angeles", "Europe/Moscow" },
                new String[] { "America/Los_Angeles", "Asia/Chongqing", "Europe/Moscow" },
                new String[] { "Europe/Moscow", "Asia/Chongqing", "America/Los_Angeles" },
                new String[] { "Asia/Chongqing", "Europe/Moscow", "America/Los_Angeles" },
                new String[] { "Europe/Moscow", "America/Los_Angeles", "Asia/Chongqing" },
                new String[] { "America/Los_Angeles", "Europe/Moscow", "Asia/Chongqing" },
                new String[] { "Asia/Chongqing", "Asia/Chongqing", "Asia/Chongqing" } };
    }

    @Test(groups = { "sit", "timezone" }, dataProvider = "testTimeZones")
    public void testDateTimeWithTimeZone(String d1TimeZone, String d2TimeZone, String testTimeZone) throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS test_datetime_with_timezone");
            s.execute("CREATE TABLE IF NOT EXISTS test_datetime_with_timezone(d0 DateTime, d1 DateTime('" + d1TimeZone
                    + "'), d2 DateTime('" + d2TimeZone + "')) ENGINE = Memory");
        } catch (ClickHouseException e) {
            return;
        }

        ClickHouseProperties props1 = new ClickHouseProperties();
        props1.setUseServerTimeZone(false);
        props1.setUseServerTimeZoneForDates(false);
        props1.setUseTimeZone(TimeZone.getDefault().getID());

        ClickHouseProperties props2 = new ClickHouseProperties();
        props2.setUseServerTimeZone(true);
        props2.setUseServerTimeZoneForDates(true);

        ClickHouseProperties props3 = new ClickHouseProperties();
        props3.setUseServerTimeZone(false);
        props3.setUseServerTimeZoneForDates(true);
        props3.setUseTimeZone(testTimeZone);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        long timestamp = 1546300800L; // '2019-01-01 00:00:00' in GMT
        try (ClickHouseConnection connDefaultTz = (ClickHouseConnection) ClickHouseContainerForTest
                .newDataSource(props1).getConnection();
                ClickHouseConnection connServerTz = (ClickHouseConnection) ClickHouseContainerForTest
                        .newDataSource(props2).getConnection();
                ClickHouseConnection connCustomTz = (ClickHouseConnection) ClickHouseContainerForTest
                        .newDataSource(props3).getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("insert into test_datetime_with_timezone values (" + timestamp + ", " + timestamp + ", "
                    + timestamp + ")");

            // time is saved as unix timestamp, while timezone is stored in column metadata
            // which only affects display and parsing
            String query = "select d0, d1, d2, toUInt32(d0) i0, toUInt32(d1) i1, toUInt32(d2) i2 from test_datetime_with_timezone";
            try (ResultSet rs = stmt.executeQuery(query)) {
                // datetime as long
                assertTrue(rs.next());
                assertEquals(rs.getLong("i0"), timestamp); // server/client timezone
                assertEquals(rs.getObject("i0", Long.class), Long.valueOf(timestamp));
                assertEquals(rs.getLong("i1"), timestamp); // d1TimeZone
                assertEquals(rs.getObject("i1", Long.class), Long.valueOf(timestamp));
                assertEquals(rs.getLong("i2"), timestamp); // d2TimeZone
                assertEquals(rs.getObject("i2", Long.class), Long.valueOf(timestamp));
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getLong("i0"), timestamp);
                    assertEquals(r.getObject("i0", Long.class), Long.valueOf(timestamp));
                    assertEquals(r.getLong("i1"), timestamp);
                    assertEquals(r.getObject("i1", Long.class), Long.valueOf(timestamp));
                    assertEquals(r.getLong("i2"), timestamp);
                    assertEquals(r.getObject("i2", Long.class), Long.valueOf(timestamp));
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getLong("i0"), timestamp);
                    assertEquals(r.getObject("i0", Long.class), Long.valueOf(timestamp));
                    assertEquals(r.getLong("i1"), timestamp);
                    assertEquals(r.getObject("i1", Long.class), Long.valueOf(timestamp));
                    assertEquals(r.getLong("i2"), timestamp);
                    assertEquals(r.getObject("i2", Long.class), Long.valueOf(timestamp));
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getLong("i0"), timestamp);
                    assertEquals(r.getObject("i0", Long.class), Long.valueOf(timestamp));
                    assertEquals(r.getLong("i1"), timestamp);
                    assertEquals(r.getObject("i1", Long.class), Long.valueOf(timestamp));
                    assertEquals(r.getLong("i2"), timestamp);
                    assertEquals(r.getObject("i2", Long.class), Long.valueOf(timestamp));
                }

                // datetime as string
                final long epochMilli = timestamp * 1000L;
                Instant testInstant = Instant.ofEpochMilli(epochMilli);
                assertEquals(rs.getString("d0"),
                        testInstant.atZone(conn.getServerTimeZone().toZoneId()).toLocalDateTime().format(formatter));
                assertEquals(rs.getObject("d0", String.class),
                        testInstant.atZone(conn.getServerTimeZone().toZoneId()).toLocalDateTime().format(formatter));
                assertEquals(rs.getString("d1"),
                        testInstant.atZone(ZoneId.of(d1TimeZone)).toLocalDateTime().format(formatter));
                assertEquals(rs.getObject("d1", String.class),
                        testInstant.atZone(ZoneId.of(d1TimeZone)).toLocalDateTime().format(formatter));
                assertEquals(rs.getString("d2"),
                        testInstant.atZone(ZoneId.of(d2TimeZone)).toLocalDateTime().format(formatter));
                assertEquals(rs.getObject("d2", String.class),
                        testInstant.atZone(ZoneId.of(d2TimeZone)).toLocalDateTime().format(formatter));
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());

                    String expectedDateTime = testInstant.atZone(ZoneId.systemDefault()).toLocalDateTime()
                            .format(formatter);
                    assertEquals(r.getString("d0"), expectedDateTime);
                    assertEquals(r.getObject("d0", String.class), expectedDateTime);
                    assertEquals(r.getString("d1"), expectedDateTime);
                    assertEquals(r.getObject("d1", String.class), expectedDateTime);
                    assertEquals(r.getString("d2"), expectedDateTime);
                    assertEquals(r.getObject("d2", String.class), expectedDateTime);
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());

                    assertEquals(r.getString("d0"), testInstant.atZone(connServerTz.getServerTimeZone().toZoneId())
                            .toLocalDateTime().format(formatter));
                    assertEquals(r.getObject("d0", String.class), testInstant
                            .atZone(connServerTz.getServerTimeZone().toZoneId()).toLocalDateTime().format(formatter));
                    assertEquals(r.getString("d1"),
                            testInstant.atZone(ZoneId.of(d1TimeZone)).toLocalDateTime().format(formatter));
                    assertEquals(r.getObject("d1", String.class),
                            testInstant.atZone(ZoneId.of(d1TimeZone)).toLocalDateTime().format(formatter));
                    assertEquals(r.getString("d2"),
                            testInstant.atZone(ZoneId.of(d2TimeZone)).toLocalDateTime().format(formatter));
                    assertEquals(r.getObject("d2", String.class),
                            testInstant.atZone(ZoneId.of(d2TimeZone)).toLocalDateTime().format(formatter));
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());

                    String expectedDateTime = testInstant.atZone(ZoneId.of(testTimeZone)).toLocalDateTime()
                            .format(formatter);
                    assertEquals(r.getString("d0"), expectedDateTime);
                    assertEquals(r.getObject("d0", String.class), expectedDateTime);
                    assertEquals(r.getString("d1"), expectedDateTime);
                    assertEquals(r.getObject("d1", String.class), expectedDateTime);
                    assertEquals(r.getString("d2"), expectedDateTime);
                    assertEquals(r.getObject("d2", String.class), expectedDateTime);
                }

                // datetime as timestamp
                Timestamp expectedTimestamp = new Timestamp(epochMilli);
                assertEquals(rs.getTimestamp("d0"), expectedTimestamp);
                assertEquals(rs.getObject("d0"), expectedTimestamp);
                assertEquals(rs.getObject("d0", Timestamp.class), expectedTimestamp);
                assertEquals(rs.getTimestamp("d1"), expectedTimestamp);
                assertEquals(rs.getObject("d1"), expectedTimestamp);
                assertEquals(rs.getObject("d1", Timestamp.class), expectedTimestamp);
                assertEquals(rs.getTimestamp("d2"), expectedTimestamp);
                assertEquals(rs.getObject("d2"), expectedTimestamp);
                assertEquals(rs.getObject("d2", Timestamp.class), expectedTimestamp);
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTimestamp("d0"), expectedTimestamp);
                    assertEquals(r.getObject("d0"), expectedTimestamp);
                    assertEquals(r.getObject("d0", Timestamp.class), expectedTimestamp);
                    assertEquals(r.getTimestamp("d1"), expectedTimestamp);
                    assertEquals(r.getObject("d1"), expectedTimestamp);
                    assertEquals(r.getObject("d1", Timestamp.class), expectedTimestamp);
                    assertEquals(r.getTimestamp("d2"), expectedTimestamp);
                    assertEquals(r.getObject("d2"), expectedTimestamp);
                    assertEquals(r.getObject("d2", Timestamp.class), expectedTimestamp);
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTimestamp("d0"), expectedTimestamp);
                    assertEquals(r.getObject("d0"), expectedTimestamp);
                    assertEquals(r.getObject("d0", Timestamp.class), expectedTimestamp);
                    assertEquals(r.getTimestamp("d1"), expectedTimestamp);
                    assertEquals(r.getObject("d1"), expectedTimestamp);
                    assertEquals(r.getObject("d1", Timestamp.class), expectedTimestamp);
                    assertEquals(r.getTimestamp("d2"), expectedTimestamp);
                    assertEquals(r.getObject("d2"), expectedTimestamp);
                    assertEquals(r.getObject("d2", Timestamp.class), expectedTimestamp);
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTimestamp("d0"), expectedTimestamp);
                    assertEquals(r.getObject("d0"), expectedTimestamp);
                    assertEquals(r.getObject("d0", Timestamp.class), expectedTimestamp);
                    assertEquals(r.getTimestamp("d1"), expectedTimestamp);
                    assertEquals(r.getObject("d1"), expectedTimestamp);
                    assertEquals(r.getObject("d1", Timestamp.class), expectedTimestamp);
                    assertEquals(r.getTimestamp("d2"), expectedTimestamp);
                    assertEquals(r.getObject("d2"), expectedTimestamp);
                    assertEquals(r.getObject("d2", Timestamp.class), expectedTimestamp);
                }

                // datetime as timestamp, with calendar
                assertEquals(rs.getTimestamp("d0"),
                        rs.getTimestamp("d0", Calendar.getInstance(TimeZone.getTimeZone(testTimeZone))));
                assertEquals(rs.getTimestamp("d1"),
                        rs.getTimestamp("d1", Calendar.getInstance(TimeZone.getTimeZone(testTimeZone))));
                assertEquals(rs.getTimestamp("d2"),
                        rs.getTimestamp("d2", Calendar.getInstance(TimeZone.getTimeZone(testTimeZone))));
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTimestamp("d0"),
                            r.getTimestamp("d0", Calendar.getInstance(TimeZone.getTimeZone(testTimeZone))));
                    assertEquals(r.getTimestamp("d1"),
                            r.getTimestamp("d1", Calendar.getInstance(TimeZone.getTimeZone(testTimeZone))));
                    assertEquals(r.getTimestamp("d2"),
                            r.getTimestamp("d2", Calendar.getInstance(TimeZone.getTimeZone(testTimeZone))));
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTimestamp("d0"),
                            r.getTimestamp("d0", Calendar.getInstance(TimeZone.getTimeZone(testTimeZone))));
                    assertEquals(r.getTimestamp("d1"),
                            r.getTimestamp("d1", Calendar.getInstance(TimeZone.getTimeZone(testTimeZone))));
                    assertEquals(r.getTimestamp("d2"),
                            r.getTimestamp("d2", Calendar.getInstance(TimeZone.getTimeZone(testTimeZone))));
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTimestamp("d0"),
                            r.getTimestamp("d0", Calendar.getInstance(TimeZone.getTimeZone(testTimeZone))));
                    assertEquals(r.getTimestamp("d1"),
                            r.getTimestamp("d1", Calendar.getInstance(TimeZone.getTimeZone(testTimeZone))));
                    assertEquals(r.getTimestamp("d2"),
                            r.getTimestamp("d2", Calendar.getInstance(TimeZone.getTimeZone(testTimeZone))));
                }

                // datetime as date
                Date expectedDate = new Date(testInstant.atZone(ZoneId.systemDefault()).truncatedTo(ChronoUnit.DAYS)
                        .toInstant().toEpochMilli());
                assertEquals(rs.getDate("d0"), expectedDate);
                assertEquals(rs.getObject("d0", Date.class), expectedDate);
                assertEquals(rs.getDate("d1"), expectedDate);
                assertEquals(rs.getObject("d1", Date.class), expectedDate);
                assertEquals(rs.getDate("d2"), expectedDate);
                assertEquals(rs.getObject("d2", Date.class), expectedDate);
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getDate("d0"), expectedDate);
                    assertEquals(r.getObject("d0", Date.class), expectedDate);
                    assertEquals(r.getDate("d1"), expectedDate);
                    assertEquals(r.getObject("d1", Date.class), expectedDate);
                    assertEquals(r.getDate("d2"), expectedDate);
                    assertEquals(r.getObject("d2", Date.class), expectedDate);
                }
                //expectedDate = new Date(testInstant.atZone(connServerTz.getServerTimeZone().toZoneId())
                //        .truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli());
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getDate("d0"), expectedDate);
                    assertEquals(r.getObject("d0", Date.class), expectedDate);
                    assertEquals(r.getDate("d1"), expectedDate);
                    assertEquals(r.getObject("d1", Date.class), expectedDate);
                    assertEquals(r.getDate("d2"), expectedDate);
                    assertEquals(r.getObject("d2", Date.class), expectedDate);
                }
                expectedDate = new Date(testInstant.atZone(ZoneId.of(testTimeZone)).truncatedTo(ChronoUnit.DAYS)
                        .toInstant().toEpochMilli());
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getDate("d0"), expectedDate);
                    assertEquals(r.getObject("d0", Date.class), expectedDate);
                    assertEquals(r.getDate("d1"), expectedDate);
                    assertEquals(r.getObject("d1", Date.class), expectedDate);
                    assertEquals(r.getDate("d2"), expectedDate);
                    assertEquals(r.getObject("d2", Date.class), expectedDate);
                }

                // datetime as time
                Time expectedTime = new Time(ClickHouseValueParser.normalizeTime(null, epochMilli));
                assertEquals(rs.getTime("d0"), expectedTime);
                assertEquals(rs.getObject("d0", Time.class), expectedTime);
                assertEquals(rs.getTime("d1"), expectedTime);
                assertEquals(rs.getObject("d1", Time.class), expectedTime);
                assertEquals(rs.getTime("d2"), expectedTime);
                assertEquals(rs.getObject("d2", Time.class), expectedTime);
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTime("d0"), expectedTime);
                    assertEquals(r.getObject("d0", Time.class), expectedTime);
                    assertEquals(r.getTime("d1"), expectedTime);
                    assertEquals(r.getObject("d1", Time.class), expectedTime);
                    assertEquals(r.getTime("d2"), expectedTime);
                    assertEquals(r.getObject("d2", Time.class), expectedTime);
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTime("d0"), expectedTime);
                    assertEquals(r.getObject("d0", Time.class), expectedTime);
                    assertEquals(r.getTime("d1"), expectedTime);
                    assertEquals(r.getObject("d1", Time.class), expectedTime);
                    assertEquals(r.getTime("d2"), expectedTime);
                    assertEquals(r.getObject("d2", Time.class), expectedTime);
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTime("d0"), expectedTime);
                    assertEquals(r.getObject("d0", Time.class), expectedTime);
                    assertEquals(r.getTime("d1"), expectedTime);
                    assertEquals(r.getObject("d1", Time.class), expectedTime);
                    assertEquals(r.getTime("d2"), expectedTime);
                    assertEquals(r.getObject("d2", Time.class), expectedTime);
                }

                // datetime as Instant
                assertEquals(rs.getObject("d0", Instant.class), testInstant);
                assertEquals(rs.getObject("d1", Instant.class), testInstant);
                assertEquals(rs.getObject("d2", Instant.class), testInstant);
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", Instant.class), testInstant);
                    assertEquals(r.getObject("d1", Instant.class), testInstant);
                    assertEquals(r.getObject("d2", Instant.class), testInstant);
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", Instant.class), testInstant);
                    assertEquals(r.getObject("d1", Instant.class), testInstant);
                    assertEquals(r.getObject("d2", Instant.class), testInstant);
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", Instant.class), testInstant);
                    assertEquals(r.getObject("d1", Instant.class), testInstant);
                    assertEquals(r.getObject("d2", Instant.class), testInstant);
                }

                // datetime as OffsetDateTime
                assertEquals(rs.getObject("d0", OffsetDateTime.class),
                        testInstant.atOffset(conn.getServerTimeZone().toZoneId().getRules().getOffset(testInstant)));
                assertEquals(rs.getObject("d1", OffsetDateTime.class),
                        testInstant.atOffset(ZoneId.of(d1TimeZone).getRules().getOffset(testInstant)));
                assertEquals(rs.getObject("d2", OffsetDateTime.class),
                        testInstant.atOffset(ZoneId.of(d2TimeZone).getRules().getOffset(testInstant)));
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", OffsetDateTime.class),
                            testInstant.atOffset(ZoneId.systemDefault().getRules().getOffset(testInstant)));
                    assertEquals(r.getObject("d1", OffsetDateTime.class),
                            testInstant.atOffset(ZoneId.systemDefault().getRules().getOffset(testInstant)));
                    assertEquals(r.getObject("d2", OffsetDateTime.class),
                            testInstant.atOffset(ZoneId.systemDefault().getRules().getOffset(testInstant)));
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", OffsetDateTime.class), testInstant
                            .atOffset(connServerTz.getServerTimeZone().toZoneId().getRules().getOffset(testInstant)));
                    assertEquals(r.getObject("d1", OffsetDateTime.class),
                            testInstant.atOffset(ZoneId.of(d1TimeZone).getRules().getOffset(testInstant)));
                    assertEquals(r.getObject("d2", OffsetDateTime.class),
                            testInstant.atOffset(ZoneId.of(d2TimeZone).getRules().getOffset(testInstant)));
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", OffsetDateTime.class),
                            testInstant.atOffset(ZoneId.of(testTimeZone).getRules().getOffset(testInstant)));
                    assertEquals(r.getObject("d1", OffsetDateTime.class),
                            testInstant.atOffset(ZoneId.of(testTimeZone).getRules().getOffset(testInstant)));
                    assertEquals(r.getObject("d2", OffsetDateTime.class),
                            testInstant.atOffset(ZoneId.of(testTimeZone).getRules().getOffset(testInstant)));
                }

                // datetime as OffsetTime
                assertEquals(rs.getObject("d0", OffsetTime.class),
                        testInstant.atOffset(conn.getServerTimeZone().toZoneId().getRules().getOffset(testInstant))
                                .toOffsetTime());
                assertEquals(rs.getObject("d1", OffsetTime.class),
                        testInstant.atOffset(ZoneId.of(d1TimeZone).getRules().getOffset(testInstant)).toOffsetTime());
                assertEquals(rs.getObject("d2", OffsetTime.class),
                        testInstant.atOffset(ZoneId.of(d2TimeZone).getRules().getOffset(testInstant)).toOffsetTime());
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", OffsetTime.class), testInstant
                            .atOffset(ZoneId.systemDefault().getRules().getOffset(testInstant)).toOffsetTime());
                    assertEquals(r.getObject("d1", OffsetTime.class), testInstant
                            .atOffset(ZoneId.systemDefault().getRules().getOffset(testInstant)).toOffsetTime());
                    assertEquals(r.getObject("d2", OffsetTime.class), testInstant
                            .atOffset(ZoneId.systemDefault().getRules().getOffset(testInstant)).toOffsetTime());
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", OffsetTime.class),
                            testInstant.atOffset(
                                    connServerTz.getServerTimeZone().toZoneId().getRules().getOffset(testInstant))
                                    .toOffsetTime());
                    assertEquals(r.getObject("d1", OffsetTime.class), testInstant
                            .atOffset(ZoneId.of(d1TimeZone).getRules().getOffset(testInstant)).toOffsetTime());
                    assertEquals(r.getObject("d2", OffsetTime.class), testInstant
                            .atOffset(ZoneId.of(d2TimeZone).getRules().getOffset(testInstant)).toOffsetTime());
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", OffsetTime.class), testInstant
                            .atOffset(ZoneId.of(testTimeZone).getRules().getOffset(testInstant)).toOffsetTime());
                    assertEquals(r.getObject("d1", OffsetTime.class), testInstant
                            .atOffset(ZoneId.of(testTimeZone).getRules().getOffset(testInstant)).toOffsetTime());
                    assertEquals(r.getObject("d2", OffsetTime.class), testInstant
                            .atOffset(ZoneId.of(testTimeZone).getRules().getOffset(testInstant)).toOffsetTime());
                }

                // datetime as ZonedDateTime
                assertEquals(rs.getObject("d0", ZonedDateTime.class),
                        testInstant.atZone(conn.getServerTimeZone().toZoneId()));
                assertEquals(rs.getObject("d1", ZonedDateTime.class), testInstant.atZone(ZoneId.of(d1TimeZone)));
                assertEquals(rs.getObject("d2", ZonedDateTime.class), testInstant.atZone(ZoneId.of(d2TimeZone)));
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", ZonedDateTime.class), testInstant.atZone(ZoneId.systemDefault()));
                    assertEquals(r.getObject("d1", ZonedDateTime.class), testInstant.atZone(ZoneId.systemDefault()));
                    assertEquals(r.getObject("d2", ZonedDateTime.class), testInstant.atZone(ZoneId.systemDefault()));
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", ZonedDateTime.class),
                            testInstant.atZone(connServerTz.getServerTimeZone().toZoneId()));
                    assertEquals(r.getObject("d1", ZonedDateTime.class), testInstant.atZone(ZoneId.of(d1TimeZone)));
                    assertEquals(r.getObject("d2", ZonedDateTime.class), testInstant.atZone(ZoneId.of(d2TimeZone)));
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", ZonedDateTime.class), testInstant.atZone(ZoneId.of(testTimeZone)));
                    assertEquals(r.getObject("d1", ZonedDateTime.class), testInstant.atZone(ZoneId.of(testTimeZone)));
                    assertEquals(r.getObject("d2", ZonedDateTime.class), testInstant.atZone(ZoneId.of(testTimeZone)));
                }

                // datetime as LocalDateTime
                LocalDateTime expectedLocalDateTime = LocalDateTime.ofInstant(testInstant, ZoneOffset.UTC);
                assertEquals(rs.getObject("d0", LocalDateTime.class), expectedLocalDateTime);
                assertEquals(rs.getObject("d1", LocalDateTime.class), expectedLocalDateTime);
                assertEquals(rs.getObject("d2", LocalDateTime.class), expectedLocalDateTime);
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", LocalDateTime.class), expectedLocalDateTime);
                    assertEquals(r.getObject("d1", LocalDateTime.class), expectedLocalDateTime);
                    assertEquals(r.getObject("d2", LocalDateTime.class), expectedLocalDateTime);
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", LocalDateTime.class), expectedLocalDateTime);
                    assertEquals(r.getObject("d1", LocalDateTime.class), expectedLocalDateTime);
                    assertEquals(r.getObject("d2", LocalDateTime.class), expectedLocalDateTime);
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", LocalDateTime.class), expectedLocalDateTime);
                    assertEquals(r.getObject("d1", LocalDateTime.class), expectedLocalDateTime);
                    assertEquals(r.getObject("d2", LocalDateTime.class), expectedLocalDateTime);
                }

                // datetime as LocalDate
                LocalDate expectedLocalDate = instantToLocalDate(testInstant, ZoneOffset.UTC);
                assertEquals(rs.getObject("d0", LocalDate.class), expectedLocalDate);
                assertEquals(rs.getObject("d1", LocalDate.class), expectedLocalDate);
                assertEquals(rs.getObject("d2", LocalDate.class), expectedLocalDate);
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", LocalDate.class), expectedLocalDate);
                    assertEquals(r.getObject("d1", LocalDate.class), expectedLocalDate);
                    assertEquals(r.getObject("d2", LocalDate.class), expectedLocalDate);
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", LocalDate.class), expectedLocalDate);
                    assertEquals(r.getObject("d1", LocalDate.class), expectedLocalDate);
                    assertEquals(r.getObject("d2", LocalDate.class), expectedLocalDate);
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", LocalDate.class), expectedLocalDate);
                    assertEquals(r.getObject("d1", LocalDate.class), expectedLocalDate);
                    assertEquals(r.getObject("d2", LocalDate.class), expectedLocalDate);
                }

                // datetime as LocalTime
                LocalTime expectedLocalTime = instantToLocalTime(testInstant, ZoneOffset.UTC);
                assertEquals(rs.getObject("d0", LocalTime.class), expectedLocalTime);
                assertEquals(rs.getObject("d1", LocalTime.class), expectedLocalTime);
                assertEquals(rs.getObject("d2", LocalTime.class), expectedLocalTime);
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", LocalTime.class), expectedLocalTime);
                    assertEquals(r.getObject("d1", LocalTime.class), expectedLocalTime);
                    assertEquals(r.getObject("d2", LocalTime.class), expectedLocalTime);
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", LocalTime.class), expectedLocalTime);
                    assertEquals(r.getObject("d1", LocalTime.class), expectedLocalTime);
                    assertEquals(r.getObject("d2", LocalTime.class), expectedLocalTime);
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d0", LocalTime.class), expectedLocalTime);
                    assertEquals(r.getObject("d1", LocalTime.class), expectedLocalTime);
                    assertEquals(r.getObject("d2", LocalTime.class), expectedLocalTime);
                }
            }

            stmt.execute("truncate table test_datetime_with_timezone");
        }
    }

    @Test(groups = { "sit", "timezone" }, dataProvider = "uniqTimeZones")
    public void testDateWithTimeZone(String testTimeZone) throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS test_date_with_timezone");
            s.execute("CREATE TABLE IF NOT EXISTS test_date_with_timezone(d Date) ENGINE = Memory");
        } catch (ClickHouseException e) {
            return;
        }

        ClickHouseProperties props1 = new ClickHouseProperties();
        props1.setUseServerTimeZone(false);
        props1.setUseServerTimeZoneForDates(false);
        props1.setUseTimeZone(TimeZone.getDefault().getID());

        ClickHouseProperties props2 = new ClickHouseProperties();
        props2.setUseServerTimeZone(true);
        props2.setUseServerTimeZoneForDates(true);

        ClickHouseProperties props3 = new ClickHouseProperties();
        props3.setUseServerTimeZone(false);
        props3.setUseServerTimeZoneForDates(true);
        props3.setUseTimeZone(testTimeZone);

        long timestamp = 1546300800L; // '2019-01-01 00:00:00' in GMT
        int date = (int) timestamp / 24 / 3600; // '2019-01-01' in GMT
        try (ClickHouseConnection connDefaultTz = (ClickHouseConnection) ClickHouseContainerForTest
                .newDataSource(props1).getConnection();
                ClickHouseConnection connServerTz = (ClickHouseConnection) ClickHouseContainerForTest
                        .newDataSource(props2).getConnection();
                ClickHouseConnection connCustomTz = (ClickHouseConnection) ClickHouseContainerForTest
                        .newDataSource(props3).getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("insert into test_date_with_timezone values (" + date + ")");

            String query = "select d, toUInt16(d) i from test_date_with_timezone";
            try (ResultSet rs = stmt.executeQuery(query)) {
                // date as integer
                assertTrue(rs.next());
                assertEquals(rs.getInt("i"), date);
                assertEquals(rs.getObject("i", Integer.class), Integer.valueOf(date));
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getInt("i"), date);
                    assertEquals(r.getObject("i", Integer.class), Integer.valueOf(date));
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getInt("i"), date);
                    assertEquals(r.getObject("i", Integer.class), Integer.valueOf(date));
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getInt("i"), date);
                    assertEquals(r.getObject("i", Integer.class), Integer.valueOf(date));
                }

                // date as string
                final long epochMilli = timestamp * 1000L;
                LocalDate testLocalDate = LocalDate.ofEpochDay(date);
                Instant testInstant = Instant.ofEpochMilli(epochMilli);
                String d = "2019-01-01";
                assertEquals(rs.getString("d"), d);
                assertEquals(rs.getObject("d", String.class), d);
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getString("d"), d);
                    assertEquals(r.getObject("d", String.class), d);
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getString("d"), d);
                    assertEquals(r.getObject("d", String.class), d);
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getString("d"), d);
                    assertEquals(r.getObject("d", String.class), d);
                }

                // date as timestamp
                Timestamp expectedTimestamp = Timestamp
                        .from(testLocalDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
                Timestamp serverTimestamp = Timestamp
                        .from(testLocalDate.atStartOfDay(connServerTz.getServerTimeZone().toZoneId()).toInstant());
                Timestamp customTimestamp = Timestamp
                        .from(testLocalDate.atStartOfDay(ZoneId.of(testTimeZone)).toInstant());
                assertEquals(rs.getTimestamp("d"), expectedTimestamp);
                assertEquals(rs.getObject("d", Timestamp.class), expectedTimestamp);
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTimestamp("d"), expectedTimestamp);
                    assertEquals(r.getObject("d", Timestamp.class), expectedTimestamp);
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTimestamp("d"), serverTimestamp);
                    assertEquals(r.getObject("d", Timestamp.class), serverTimestamp);
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTimestamp("d"), customTimestamp);
                    assertEquals(r.getObject("d", Timestamp.class), customTimestamp);
                }

                // date as timestamp, with calendar
                assertEquals(rs.getTimestamp("d"),
                        rs.getTimestamp("d", Calendar.getInstance(TimeZone.getTimeZone(testTimeZone))));
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTimestamp("d"),
                            r.getTimestamp("d", Calendar.getInstance(TimeZone.getTimeZone(testTimeZone))));
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTimestamp("d"),
                            r.getTimestamp("d", Calendar.getInstance(TimeZone.getTimeZone(testTimeZone))));
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTimestamp("d"),
                            r.getTimestamp("d", Calendar.getInstance(TimeZone.getTimeZone(testTimeZone))));
                }

                // date as date
                Date expectedDate = new Date(expectedTimestamp.getTime());
                Date serverDate = new Date(serverTimestamp.getTime());
                Date customDate = new Date(customTimestamp.getTime());
                assertEquals(rs.getDate("d"), expectedDate);
                assertEquals(rs.getObject("d"), expectedDate);
                assertEquals(rs.getObject("d", Date.class), expectedDate);
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getDate("d"), expectedDate);
                    assertEquals(r.getObject("d"), expectedDate);
                    assertEquals(r.getObject("d", Date.class), expectedDate);
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getDate("d"), serverDate);
                    assertEquals(r.getObject("d"), serverDate);
                    assertEquals(r.getObject("d", Date.class), serverDate);
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getDate("d"), customDate);
                    assertEquals(r.getObject("d"), customDate);
                    assertEquals(r.getObject("d", Date.class), customDate);
                }

                // date as time
                Time expectedTime = new Time(ClickHouseValueParser.normalizeTime(null, expectedTimestamp.getTime()));
                Time serverTime = new Time(ClickHouseValueParser.normalizeTime(null, serverTimestamp.getTime()));
                Time customTime = new Time(ClickHouseValueParser.normalizeTime(null, customTimestamp.getTime()));
                assertEquals(rs.getTime("d"), expectedTime);
                assertEquals(rs.getObject("d", Time.class), expectedTime);
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTime("d"), expectedTime);
                    assertEquals(r.getObject("d", Time.class), expectedTime);
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTime("d"), serverTime);
                    assertEquals(r.getObject("d", Time.class), serverTime);
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getTime("d"), customTime);
                    assertEquals(r.getObject("d", Time.class), customTime);
                }

                // date as Instant
                assertEquals(rs.getObject("d", Instant.class), testInstant);
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", Instant.class), testInstant);
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", Instant.class), testInstant);
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", Instant.class), testInstant);
                }

                // date as OffsetDateTime
                ZoneOffset expectedOffset = ZoneId.systemDefault().getRules().getOffset(testInstant);
                ZoneOffset serverOffset = connServerTz.getServerTimeZone().toZoneId().getRules().getOffset(testInstant);
                ZoneOffset customOffset = ZoneId.of(testTimeZone).getRules().getOffset(testInstant);
                OffsetDateTime expectedOffsetDateTime = testLocalDate.atStartOfDay(ZoneId.systemDefault())
                        .toOffsetDateTime();
                OffsetDateTime serverOffsetDateTime = testLocalDate
                        .atStartOfDay(connServerTz.getServerTimeZone().toZoneId()).toOffsetDateTime();
                OffsetDateTime customOffsetDateTime = testLocalDate.atStartOfDay(ZoneId.of(testTimeZone))
                        .toOffsetDateTime();
                assertEquals(rs.getObject("d", OffsetDateTime.class), expectedOffsetDateTime);
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", OffsetDateTime.class), expectedOffsetDateTime);
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", OffsetDateTime.class), serverOffsetDateTime);
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", OffsetDateTime.class), customOffsetDateTime);
                }

                // date as OffsetTime
                assertEquals(rs.getObject("d", OffsetTime.class), LocalTime.MIDNIGHT.atOffset(expectedOffset));
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", OffsetTime.class), LocalTime.MIDNIGHT.atOffset(expectedOffset));
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", OffsetTime.class), LocalTime.MIDNIGHT.atOffset(serverOffset));
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", OffsetTime.class), LocalTime.MIDNIGHT.atOffset(customOffset));
                }

                // date as ZonedDateTime
                assertEquals(rs.getObject("d", ZonedDateTime.class),
                        testLocalDate.atStartOfDay(ZoneId.systemDefault()));
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", ZonedDateTime.class),
                            testLocalDate.atStartOfDay(ZoneId.systemDefault()));
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", ZonedDateTime.class),
                            testLocalDate.atStartOfDay(connServerTz.getServerTimeZone().toZoneId()));
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", ZonedDateTime.class),
                            testLocalDate.atStartOfDay(ZoneId.of(testTimeZone)));
                }

                // date as LocalDateTime
                LocalDateTime expectedLocalDateTime = testLocalDate.atStartOfDay(ZoneId.systemDefault())
                        .toLocalDateTime();
                assertEquals(rs.getObject("d", LocalDateTime.class), expectedLocalDateTime);
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", LocalDateTime.class), expectedLocalDateTime);
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", LocalDateTime.class),
                            testLocalDate.atStartOfDay(connServerTz.getServerTimeZone().toZoneId()).toLocalDateTime());
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", LocalDateTime.class),
                            testLocalDate.atStartOfDay(ZoneId.of(testTimeZone)).toLocalDateTime());
                }

                // date as LocalDate
                assertEquals(rs.getObject("d", LocalDate.class), testLocalDate);
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", LocalDate.class), testLocalDate);
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", LocalDate.class), testLocalDate);
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", LocalDate.class), testLocalDate);
                }

                // date as LocalTime
                assertEquals(rs.getObject("d", LocalTime.class), LocalTime.MIDNIGHT);
                try (Statement s = connDefaultTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", LocalTime.class), LocalTime.MIDNIGHT);
                }
                try (Statement s = connServerTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", LocalTime.class), LocalTime.MIDNIGHT);
                }
                try (Statement s = connCustomTz.createStatement(); ResultSet r = s.executeQuery(query);) {
                    assertTrue(r.next());
                    assertEquals(r.getObject("d", LocalTime.class), LocalTime.MIDNIGHT);
                }
            }

            stmt.execute("truncate table test_date_with_timezone");
        }
    }

    @Test
    public void testDateTimes() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS test_datetimes");
            s.execute(
                    "CREATE TABLE IF NOT EXISTS test_datetimes(d DateTime, d32 DateTime32, d64 DateTime64(9)) ENGINE = Memory");
        } catch (ClickHouseException e) {
            return;
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        long timestamp = 1614881594000L;
        Timestamp expected = new Timestamp(timestamp);
        expected.setNanos(123456789);
        String str = Instant.ofEpochMilli(timestamp).atZone(conn.getServerTimeZone().toZoneId()).format(formatter);
        String before = Instant.ofEpochMilli(timestamp - 3 * 24 * 3600000).atZone(conn.getServerTimeZone().toZoneId())
                .format(formatter);

        try (Statement s = conn.createStatement()) {
            s.execute("insert into test_datetimes values ('" + str + "', '" + str + "', '" + str + ".123456789')");

            try (ResultSet rs = s.executeQuery("select * from test_datetimes")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject("d"), new Timestamp(timestamp));
                assertEquals(rs.getObject("d32"), new Timestamp(timestamp));
                assertEquals(rs.getObject("d64"), expected);
            }

            s.execute("truncate table test_datetimes");
        }

        try (PreparedStatement s = conn.prepareStatement("insert into test_datetimes values(?,?,?)")) {
            s.setString(1, str);
            s.setString(2, str);
            s.setString(3, str + ".123456789");
            s.execute();
        }
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("select * from test_datetimes")) {
            assertTrue(rs.next());
            assertEquals(rs.getObject("d"), new Timestamp(timestamp));
            assertEquals(rs.getObject("d32"), new Timestamp(timestamp));
            assertEquals(rs.getObject("d64"), expected);
            s.execute("truncate table test_datetimes");
        }

        try (PreparedStatement s = conn.prepareStatement("insert into test_datetimes values(?,?,?)")) {
            s.setObject(1, new Timestamp(timestamp));
            s.setObject(2, new Timestamp(timestamp));
            s.setObject(3, expected);
            s.execute();
        }
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("select * from test_datetimes")) {
            assertTrue(rs.next());
            assertEquals(rs.getObject("d"), new Timestamp(timestamp));
            assertEquals(rs.getObject("d32"), new Timestamp(timestamp));
            assertEquals(rs.getObject("d64"), expected);
            s.execute("truncate table test_datetimes");
        }

        try (Statement s = conn.createStatement()) {
            s.execute("insert into test_datetimes values ('" + str + "', '" + str + "', '" + str + ".123456789')");

            try (ResultSet rs = s.executeQuery("select * from test_datetimes")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject("d"), new Timestamp(timestamp));
                assertEquals(rs.getObject("d32"), new Timestamp(timestamp));
                assertEquals(rs.getObject("d64"), expected);
            }

            s.execute("truncate table test_datetimes");
        }

        try (PreparedStatement s = conn.prepareStatement("insert into test_datetimes values(?,?,?)")) {
            s.setString(1, before);
            s.setString(2, before);
            s.setString(3, before + ".123456789");
            s.execute();

            s.setObject(1, new Timestamp(expected.getTime()));
            s.setObject(2, new Timestamp(expected.getTime()));
            s.setObject(3, expected);
            s.execute();
        }

        try (Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery("select * from test_datetimes order by d")) {
                assertTrue(rs.next());
                Timestamp ts = new Timestamp(expected.getTime() - 3 * 24 * 3600000);
                ts.setNanos(expected.getNanos());
                assertEquals(rs.getObject("d"), new Timestamp(ts.getTime() - ts.getNanos() / 1000000));
                assertEquals(rs.getObject("d32"), new Timestamp(ts.getTime() - ts.getNanos() / 1000000));
                assertEquals(rs.getObject("d64"), ts);

                assertTrue(rs.next());
                assertEquals(rs.getObject("d"), new Timestamp(expected.getTime() - expected.getNanos() / 1000000));
                assertEquals(rs.getObject("d32"), new Timestamp(expected.getTime() - expected.getNanos() / 1000000));
                assertEquals(rs.getObject("d64"), expected);

                assertEquals(rs.getTime("d"), new Time(
                        ClickHouseValueParser.normalizeTime(null, expected.getTime() - expected.getTime() % 1000)));
                assertEquals(rs.getTime("d32"), new Time(
                        ClickHouseValueParser.normalizeTime(null, expected.getTime() - expected.getTime() % 1000)));
                assertEquals(rs.getTime("d64"), new Time(ClickHouseValueParser
                        .normalizeTime(ClickHouseColumnInfo.parse("DateTime64(9)", "d64", null), expected.getTime())));
            }
        }
    }

    @Test
    public void testIPs() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS test_ips");
            s.execute("CREATE TABLE IF NOT EXISTS test_ips(ip4 IPv4, ip6 IPv6) ENGINE = Memory");
            s.execute("insert into test_ips values ('0.0.0.0', '::')");

            try (ResultSet rs = s.executeQuery("select * from test_ips")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject("ip4"), "0.0.0.0");
                assertEquals(rs.getObject("ip6"), "::");
            }

            s.execute("truncate table test_ips");
        }

        try (PreparedStatement s = conn.prepareStatement("insert into test_ips values(?,?)")) {
            s.setString(1, "0.0.0.0");
            s.setString(2, "::");
            s.execute();

            s.setObject(1, 16909060);
            s.setObject(2, "2607:f8b0:4005:805::2004");
            s.execute();
        }

        try (Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery("select * from test_ips order by ip4")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject("ip4"), "0.0.0.0");
                assertEquals(rs.getObject("ip6"), "::");

                assertTrue(rs.next());
                assertEquals(rs.getObject("ip4"), "1.2.3.4");
                assertEquals(rs.getObject("ip6"), "2607:f8b0:4005:805::2004");
            }
        }
    }
}
