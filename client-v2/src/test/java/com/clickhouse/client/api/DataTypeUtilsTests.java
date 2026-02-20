package com.clickhouse.client.api;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.clickhouse.data.ClickHouseDataType;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class DataTypeUtilsTests {

    @Test
    void testDateTimeFormatter() {
        LocalDateTime dateTime = LocalDateTime.of(2021, 12, 31, 23, 59, 59);
        String formattedDateTime = dateTime.format(DataTypeUtils.DATETIME_FORMATTER);
        assertEquals(formattedDateTime, "2021-12-31 23:59:59");
    }

    @Test
    void testDateFormatter() {
        LocalDateTime date = LocalDateTime.of(2021, 12, 31, 10, 20);
        String formattedDate = date.toLocalDate().format(DataTypeUtils.DATE_FORMATTER);
        assertEquals(formattedDate, "2021-12-31");
    }

    @Test
    void testDateTimeWithNanosFormatter() {
        LocalDateTime dateTime = LocalDateTime.of(2021, 12, 31, 23, 59, 59, 123456789);
        String formattedDateTimeWithNanos = dateTime.format(DataTypeUtils.DATETIME_WITH_NANOS_FORMATTER);
        assertEquals(formattedDateTimeWithNanos, "2021-12-31 23:59:59.123456789");
    }

    @Test
    void formatInstantForDateNullInstant() {
        assertThrows(
            NullPointerException.class,
            () -> DataTypeUtils.formatInstant(null, ClickHouseDataType.Date,
                ZoneId.systemDefault()));
    }

    @Test
    void formatInstantForDateNullTimeZone() {
        assertThrows(
            NullPointerException.class,
            () -> DataTypeUtils.formatInstant(Instant.now(), ClickHouseDataType.Date, null));
    }

    @Test
    void formatInstantForDate() {
        ZoneId tzBER = ZoneId.of("Europe/Berlin");
        ZoneId tzLAX = ZoneId.of("America/Los_Angeles");
        Instant instant = ZonedDateTime.of(
            2025, 7, 20, 5, 5, 42, 0, tzBER).toInstant();
        assertEquals(
            DataTypeUtils.formatInstant(instant, ClickHouseDataType.Date, tzBER),
            "2025-07-20");
        assertEquals(
            DataTypeUtils.formatInstant(instant, ClickHouseDataType.Date, tzLAX),
            "2025-07-19");
    }

    @Test
    void formatInstantNullValue() {
        assertThrows(
            NullPointerException.class,
            () -> DataTypeUtils.formatInstant(null));
    }

    @Test
    void formatInstantForDateTime() {
        TimeZone tzBER = TimeZone.getTimeZone("Europe/Berlin");
        Instant instant = ZonedDateTime.of(
            2025, 7, 20, 5, 5, 42, 232323, tzBER.toZoneId()).toInstant();
        String formatted = DataTypeUtils.formatInstant(instant, ClickHouseDataType.DateTime);
        assertEquals(formatted, "1752980742");
        assertEquals(
            Instant.ofEpochSecond(Long.parseLong(formatted)),
            instant.truncatedTo(ChronoUnit.SECONDS));
    }

    @Test
    void formatInstantForDateTime64() {
        TimeZone tzBER = TimeZone.getTimeZone("Europe/Berlin");
        Instant instant = ZonedDateTime.of(
            2025, 7, 20, 5, 5, 42, 232323232, tzBER.toZoneId()).toInstant();
        String formatted = DataTypeUtils.formatInstant(instant);
        assertEquals(formatted, "1752980742.232323232");
        String[] formattedParts = formatted.split("\\.");
        assertEquals(
            Instant
                .ofEpochSecond(Long.parseLong(formattedParts[0]))
                .plusNanos(Long.parseLong(formattedParts[1])),
            instant);
    }

    @Test
    void formatInstantForDateTime64SmallerNanos() {
        TimeZone tzBER = TimeZone.getTimeZone("Europe/Berlin");
        Instant instant = ZonedDateTime.of(
            2025, 7, 20, 5, 5, 42, 23, tzBER.toZoneId()).toInstant();
        String formatted = DataTypeUtils.formatInstant(instant);
        assertEquals(formatted, "1752980742.000000023");
        String[] formattedParts = formatted.split("\\.");
        assertEquals(
            Instant
                .ofEpochSecond(Long.parseLong(formattedParts[0]))
                .plusNanos(Long.parseLong(formattedParts[1])),
            instant);
    }

    @Test
    void formatInstantForDateTime64Truncated() {
        // precision is constant for Instant
        TimeZone tzBER = TimeZone.getTimeZone("Europe/Berlin");
        Instant instant = ZonedDateTime.of(
            2025, 7, 20, 5, 5, 42, 232323232, tzBER.toZoneId()).toInstant();
        assertEquals(
            DataTypeUtils.formatInstant(
                instant.truncatedTo(ChronoUnit.SECONDS)),
            "1752980742.000000000");
        assertEquals(
            DataTypeUtils.formatInstant(
                instant.truncatedTo(ChronoUnit.MILLIS)),
            "1752980742.232000000");
    }

    @Test(groups = {"unit"})
    void testDifferentDateConversions() throws Exception {
        Calendar externalSystemTz = Calendar.getInstance(TimeZone.getTimeZone("UTC+12"));
        Calendar utcTz = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        Calendar applicationLocalTz = Calendar.getInstance(TimeZone.getTimeZone("UTC-8"));


        String externalDateStr = externalSystemTz.get(Calendar.YEAR) + "-" + (externalSystemTz.get(Calendar.MONTH) + 1)  + "-" + externalSystemTz.get(Calendar.DAY_OF_MONTH);
        java.sql.Date externalDate = new java.sql.Date(externalSystemTz.getTimeInMillis());
        System.out.println(externalDate.toLocalDate());
        System.out.println(externalDateStr);
        System.out.println(externalDate);

        Calendar extCal2 = (Calendar) externalSystemTz.clone();
        extCal2.setTime(externalDate);

        System.out.println("> " + extCal2);
        String externalDateStr2 = extCal2.get(Calendar.YEAR) + "-" + (extCal2.get(Calendar.MONTH) + 1)  + "-" + extCal2.get(Calendar.DAY_OF_MONTH);
        System.out.println("> " + externalDateStr2);

        Calendar extCal3 = (Calendar) externalSystemTz.clone();
        LocalDate localDateFromExternal = externalDate.toLocalDate(); // converted date to local timezone (day may shift)
        extCal3.clear();
        extCal3.set(localDateFromExternal.getYear(), localDateFromExternal.getMonthValue() - 1, localDateFromExternal.getDayOfMonth(), 0, 0, 0);
        System.out.println("converted> " + extCal3.toInstant()); // wrong date!! 
    }
    @Test(groups = {"unit"})
    void testToLocalDateNullTimeZone() {
        Date sqlDate = Date.valueOf("2024-01-15");
        assertThrows(NullPointerException.class,
                () -> DataTypeUtils.toLocalDate(sqlDate, (TimeZone) null));
    }


    @Test(groups = {"unit"})
    void testToLocalDateWithCalendar() {
        // Create a date that represents midnight Jan 15, 2024 in UTC
        Calendar utcCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        utcCal.clear();
        utcCal.set(2024, Calendar.JANUARY, 15, 0, 0, 0);
        Date sqlDate = new Date(utcCal.getTimeInMillis());

        // Using UTC calendar should give us Jan 15
        LocalDate resultUtc = DataTypeUtils.toLocalDate(sqlDate, utcCal.getTimeZone());
        assertEquals(resultUtc, LocalDate.of(2024, 1, 15));
    }

    /**
     * Test the "day shift" problem: when a Date's millis are created in one timezone
     * but interpreted in another, the day can shift.
     */
    @Test(groups = {"unit"})
    void testToLocalDateDayShiftProblem() {
        // Simulate: Date created in Pacific/Auckland (UTC+12/+13)
        // At midnight Jan 15 in Auckland, it's still Jan 14 in UTC
        TimeZone aucklandTz = TimeZone.getTimeZone("Pacific/Auckland");
        Calendar aucklandCal = new GregorianCalendar(aucklandTz);
        aucklandCal.clear();
        aucklandCal.set(2024, Calendar.JANUARY, 15, 0, 0, 0);
        Date dateFromAuckland = new Date(aucklandCal.getTimeInMillis());

        // Using Auckland calendar should correctly extract Jan 15
        LocalDate withAucklandCal = DataTypeUtils.toLocalDate(dateFromAuckland, aucklandCal.getTimeZone());
        assertEquals(withAucklandCal, LocalDate.of(2024, 1, 15),
                "With correct timezone, should get Jan 15");

        // Using UTC calendar on the same Date would give a different (earlier) day
        Calendar utcCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        LocalDate withUtcCal = DataTypeUtils.toLocalDate(dateFromAuckland, utcCal.getTimeZone());
        assertEquals(withUtcCal, LocalDate.of(2024, 1, 14),
                "With UTC timezone, should get Jan 14 (day shift demonstrated)");
    }

    @DataProvider(name = "timezonesForDateTest")
    public Object[][] timezonesForDateTest() {
        return new Object[][] {
                {"UTC", "2024-01-15", 2024, 1, 15},
                {"America/New_York", "2024-01-15", 2024, 1, 15},
                {"America/Los_Angeles", "2024-01-15", 2024, 1, 15},
                {"Europe/London", "2024-01-15", 2024, 1, 15},
                {"Europe/Moscow", "2024-01-15", 2024, 1, 15},
                {"Asia/Tokyo", "2024-01-15", 2024, 1, 15},
                {"Pacific/Auckland", "2024-01-15", 2024, 1, 15},
                {"Pacific/Honolulu", "2024-01-15", 2024, 1, 15},
        };
    }

    @Test(groups = {"unit"}, dataProvider = "timezonesForDateTest")
    void testToLocalDateWithVariousTimezones(String tzId, String dateStr, int year, int month, int day) {
        TimeZone tz = TimeZone.getTimeZone(tzId);
        Calendar cal = new GregorianCalendar(tz);
        cal.clear();
        cal.set(year, month - 1, day, 0, 0, 0);
        Date sqlDate = new Date(cal.getTimeInMillis());

        LocalDate result = DataTypeUtils.toLocalDate(sqlDate, tz);
        assertEquals(result, LocalDate.of(year, month, day),
                "Date should be preserved in timezone: " + tzId);
    }

    @Test(groups = {"unit"})
    void testToLocalDateWithTimeZoneObject() {
        TimeZone utc = TimeZone.getTimeZone("UTC");
        Calendar utcCal = new GregorianCalendar(utc);
        utcCal.clear();
        utcCal.set(2024, Calendar.JULY, 4, 0, 0, 0);
        Date sqlDate = new Date(utcCal.getTimeInMillis());

        LocalDate result = DataTypeUtils.toLocalDate(sqlDate, utc);
        assertEquals(result, LocalDate.of(2024, 7, 4));
    }

    // ==================== Tests for toLocalTime ====================

    @Test(groups = {"unit"})
    void testToLocalTimeWithCalendar() {
        // Create a time that represents 14:30:00 in UTC
        Calendar utcCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        utcCal.clear();
        utcCal.set(1970, Calendar.JANUARY, 1, 14, 30, 0);
        Time sqlTime = new Time(utcCal.getTimeInMillis());

        // Using UTC calendar should give us 14:30:00
        LocalTime resultUtc = DataTypeUtils.toLocalTime(sqlTime, utcCal.getTimeZone());
        assertEquals(resultUtc.getHour(), 14);
        assertEquals(resultUtc.getMinute(), 30);
        assertEquals(resultUtc.getSecond(), 0);
    }

    @Test(groups = {"unit"})
    void testToLocalTimeTimeZoneShift() {
        // Create time in UTC: 14:00:00
        Calendar utcCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        utcCal.clear();
        utcCal.set(1970, Calendar.JANUARY, 1, 14, 0, 0);
        Time sqlTime = new Time(utcCal.getTimeInMillis());

        // In UTC, should be 14:00
        LocalTime inUtc = DataTypeUtils.toLocalTime(sqlTime, utcCal.getTimeZone());
        assertEquals(inUtc, LocalTime.of(14, 0, 0));

        // In New York (UTC-5), same instant would be 09:00
        Calendar nyCal = new GregorianCalendar(TimeZone.getTimeZone("America/New_York"));
        LocalTime inNy = DataTypeUtils.toLocalTime(sqlTime, nyCal.getTimeZone());
        assertEquals(inNy, LocalTime.of(9, 0, 0));
    }

    @Test(groups = {"unit"})
    void testToLocalTimeWithTimeZoneObject() {
        TimeZone utc = TimeZone.getTimeZone("UTC");
        Calendar utcCal = new GregorianCalendar(utc);
        utcCal.clear();
        utcCal.set(1970, Calendar.JANUARY, 1, 23, 59, 59);
        Time sqlTime = new Time(utcCal.getTimeInMillis());

        LocalTime result = DataTypeUtils.toLocalTime(sqlTime, utc);
        assertEquals(result, LocalTime.of(23, 59, 59));
    }

    // ==================== Tests for toLocalDateTime ====================

    @Test(groups = {"unit"})
    void testToLocalDateTimeTimezoneShift() {
        // Create timestamp in UTC: 2024-01-15 04:00:00
        Calendar utcCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        utcCal.clear();
        utcCal.set(2024, Calendar.JANUARY, 15, 4, 0, 0);
        Timestamp sqlTimestamp = new Timestamp(utcCal.getTimeInMillis());

        // In UTC: 2024-01-15 04:00:00
        LocalDateTime inUtc = DataTypeUtils.toLocalDateTime(sqlTimestamp, utcCal.getTimeZone());
        assertEquals(inUtc, LocalDateTime.of(2024, 1, 15, 4, 0, 0));

        // In New York (UTC-5): same instant is 2024-01-14 23:00:00
        Calendar nyCal = new GregorianCalendar(TimeZone.getTimeZone("America/New_York"));
        LocalDateTime inNy = DataTypeUtils.toLocalDateTime(sqlTimestamp, nyCal.getTimeZone());
        assertEquals(inNy, LocalDateTime.of(2024, 1, 14, 23, 0, 0));
    }

    @Test(groups = {"unit"})
    void testToLocalDateTimeWithTimeZoneObject() {
        TimeZone utc = TimeZone.getTimeZone("UTC");
        Calendar utcCal = new GregorianCalendar(utc);
        utcCal.clear();
        utcCal.set(2024, Calendar.DECEMBER, 31, 23, 59, 59);
        Timestamp sqlTimestamp = new Timestamp(utcCal.getTimeInMillis());
        sqlTimestamp.setNanos(999999999);

        LocalDateTime result = DataTypeUtils.toLocalDateTime(sqlTimestamp, utc);
        assertEquals(result, LocalDateTime.of(2024, 12, 31, 23, 59, 59, 999999999));
    }

    @Test(groups = {"unit"})
    void testToLocalDateTimeNanosPreservedWithTimeZone() {
        // Verify nanoseconds are preserved when using TimeZone overload
        TimeZone tokyo = TimeZone.getTimeZone("Asia/Tokyo");
        Calendar tokyoCal = new GregorianCalendar(tokyo);
        tokyoCal.clear();
        tokyoCal.set(2024, Calendar.JUNE, 15, 10, 30, 45);
        Timestamp sqlTimestamp = new Timestamp(tokyoCal.getTimeInMillis());
        sqlTimestamp.setNanos(123456789);

        LocalDateTime result = DataTypeUtils.toLocalDateTime(sqlTimestamp, tokyo);
        assertEquals(result.getNano(), 123456789);
        assertEquals(result.getHour(), 10);
        assertEquals(result.getMinute(), 30);
        assertEquals(result.getSecond(), 45);
    }

    /**
     * Comprehensive test demonstrating the day shift problem and its solution.
     */
    @Test(groups = {"unit"})
    void testDayShiftProblemAndSolution() {
        // Scenario: Financial system in Tokyo (UTC+9) records a trade at 11 PM on Dec 31
        // Server is running in UTC
        TimeZone tokyoTz = TimeZone.getTimeZone("Asia/Tokyo");
        TimeZone utcTz = TimeZone.getTimeZone("UTC");

        // Trade timestamp: Dec 31, 2024 23:30:00 Tokyo time
        Calendar tokyoCal = new GregorianCalendar(tokyoTz);
        tokyoCal.clear();
        tokyoCal.set(2024, Calendar.DECEMBER, 31, 23, 30, 0);
        Timestamp tradeTimestamp = new Timestamp(tokyoCal.getTimeInMillis());

        // At 23:30 Tokyo (UTC+9), it's 14:30 UTC - still Dec 31
        LocalDateTime inTokyo = DataTypeUtils.toLocalDateTime(tradeTimestamp, tokyoCal.getTimeZone());
        assertEquals(inTokyo.toLocalDate(), LocalDate.of(2024, 12, 31),
                "In Tokyo timezone, trade date should be Dec 31");

        LocalDateTime inUtc = DataTypeUtils.toLocalDateTime(tradeTimestamp,
                new GregorianCalendar(utcTz).getTimeZone());
        assertEquals(inUtc.toLocalDate(), LocalDate.of(2024, 12, 31),
                "In UTC, same trade is also Dec 31 (14:30 UTC)");

        // But if the trade was at 00:30 Tokyo time on Jan 1...
        tokyoCal.clear();
        tokyoCal.set(2025, Calendar.JANUARY, 1, 0, 30, 0);
        Timestamp newYearTrade = new Timestamp(tokyoCal.getTimeInMillis());

        LocalDateTime newYearInTokyo = DataTypeUtils.toLocalDateTime(newYearTrade, tokyoCal.getTimeZone());
        assertEquals(newYearInTokyo.toLocalDate(), LocalDate.of(2025, 1, 1),
                "In Tokyo, it's New Year's Day");

        LocalDateTime newYearInUtc = DataTypeUtils.toLocalDateTime(newYearTrade,
                new GregorianCalendar(utcTz).getTimeZone());
        assertEquals(newYearInUtc.toLocalDate(), LocalDate.of(2024, 12, 31),
                "In UTC, it's still Dec 31 (15:30 UTC on Dec 31)");
    }

    // ==================== Tests for toSqlDate ====================

    @Test(groups = {"unit"})
    void testToSqlDateNullTimeZone() {
        LocalDate localDate = LocalDate.of(2024, 1, 15);
        assertThrows(NullPointerException.class,
                () -> DataTypeUtils.toSqlDate(localDate, (TimeZone) null));
    }

    @Test(groups = {"unit"})
    void testToSqlDateWithTimeZone() {
        LocalDate localDate = LocalDate.of(2024, 7, 4);
        TimeZone utc = TimeZone.getTimeZone("UTC");

        Date sqlDate = DataTypeUtils.toSqlDate(localDate, utc);

        // Convert back to verify round-trip
        LocalDate roundTrip = DataTypeUtils.toLocalDate(sqlDate, utc);
        assertEquals(roundTrip, localDate);
    }

    @Test(groups = {"unit"})
    void testToSqlDateRoundTripWithVariousTimezones() {
        LocalDate localDate = LocalDate.of(2024, 1, 15);
        String[] tzIds = {"UTC", "America/New_York", "Asia/Tokyo", "Pacific/Auckland"};

        for (String tzId : tzIds) {
            TimeZone tz = TimeZone.getTimeZone(tzId);
            Calendar cal = new GregorianCalendar(tz);

            // Convert to SQL Date and back
            Date sqlDate = DataTypeUtils.toSqlDate(localDate, cal.getTimeZone());
            LocalDate roundTrip = DataTypeUtils.toLocalDate(sqlDate, cal.getTimeZone());

            assertEquals(roundTrip, localDate,
                    "Round-trip should preserve date in timezone: " + tzId);
        }
    }

    // ==================== Tests for toSqlTimestamp ====================

    @Test(groups = {"unit"})
    void testToSqlTimestampWithTimeZone() {
        LocalDateTime localDateTime = LocalDateTime.of(2024, 12, 31, 23, 59, 59, 999999999);
        TimeZone utc = TimeZone.getTimeZone("UTC");

        Timestamp sqlTimestamp = DataTypeUtils.toSqlTimestamp(localDateTime, utc);

        // Convert back to verify round-trip
        LocalDateTime roundTrip = DataTypeUtils.toLocalDateTime(sqlTimestamp, utc);
        assertEquals(roundTrip, localDateTime);
    }

    @Test(groups = {"unit"})
    void testToSqlTimestampPreservesNanoseconds() {
        LocalDateTime localDateTime = LocalDateTime.of(2024, 6, 15, 10, 30, 45, 123456789);
        Calendar utcCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

        Timestamp sqlTimestamp = DataTypeUtils.toSqlTimestamp(localDateTime, utcCal.getTimeZone());

        assertEquals(sqlTimestamp.getNanos(), 123456789);
    }

    @Test(groups = {"unit"})
    void testToSqlTimestampRoundTripWithVariousTimezones() {
        LocalDateTime localDateTime = LocalDateTime.of(2024, 1, 15, 23, 30, 45, 123456789);
        String[] tzIds = {"UTC", "America/New_York", "Asia/Tokyo", "Pacific/Auckland"};

        for (String tzId : tzIds) {
            TimeZone tz = TimeZone.getTimeZone(tzId);
            Calendar cal = new GregorianCalendar(tz);

            // Convert to SQL Timestamp and back
            Timestamp sqlTimestamp = DataTypeUtils.toSqlTimestamp(localDateTime, cal.getTimeZone());
            LocalDateTime roundTrip = DataTypeUtils.toLocalDateTime(sqlTimestamp, cal.getTimeZone());

            assertEquals(roundTrip, localDateTime,
                    "Round-trip should preserve datetime in timezone: " + tzId);
        }
    }

    /**
     * Comprehensive round-trip test demonstrating timezone handling.
     */
    @Test(groups = {"unit"})
    void testRoundTripConversionsWithDifferentTimezones() {
        // Original values
        LocalDate date = LocalDate.of(2024, 7, 4);
        LocalTime time = LocalTime.of(3, 30, 45, 123000000);
        LocalDateTime dateTime = LocalDateTime.of(date, time);

        TimeZone tokyo = TimeZone.getTimeZone("Asia/Tokyo");
        TimeZone newYork = TimeZone.getTimeZone("America/New_York");

        // Convert to SQL types using Tokyo timezone
        Calendar tokyoCal = new GregorianCalendar(tokyo);
        Date sqlDateTokyo = DataTypeUtils.toSqlDate(date, tokyoCal.getTimeZone());
        Time sqlTimeTokyo = DataTypeUtils.toSqlTime(time, tokyoCal.getTimeZone());
        Timestamp sqlTimestampTokyo = DataTypeUtils.toSqlTimestamp(dateTime, tokyoCal.getTimeZone());

        // Round-trip back using same timezone should preserve values
        assertEquals(DataTypeUtils.toLocalDate(sqlDateTokyo, tokyoCal.getTimeZone()), date);
        LocalTime timeRoundTrip = DataTypeUtils.toLocalTime(sqlTimeTokyo, tokyoCal.getTimeZone());
        assertEquals(timeRoundTrip.getHour(), time.getHour());
        assertEquals(timeRoundTrip.getMinute(), time.getMinute());
        assertEquals(timeRoundTrip.getSecond(), time.getSecond());
        assertEquals(DataTypeUtils.toLocalDateTime(sqlTimestampTokyo, tokyoCal.getTimeZone()), dateTime);

        // If we interpret the same SQL values in a different timezone, we get different local values
        // This is expected - the same instant in time represents different local times in different zones
        Calendar nyCal = new GregorianCalendar(newYork);
        LocalDateTime dateTimeInNy = DataTypeUtils.toLocalDateTime(sqlTimestampTokyo, nyCal.getTimeZone());
        // Tokyo is 13-14 hours ahead of NY, so the local time should be different
        // (03:30 Tokyo = 14:30 NY previous day during EDT)
        assertEquals(dateTimeInNy.toLocalDate(), LocalDate.of(2024, 7, 4).minusDays(1),
                "Same instant should be previous day in New York");
    }
}
