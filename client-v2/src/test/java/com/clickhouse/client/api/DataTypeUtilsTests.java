package com.clickhouse.client.api;

import org.testng.annotations.Test;

import com.clickhouse.data.ClickHouseDataType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.TimeZone;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

class DataTypeUtilsTests {

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

}
