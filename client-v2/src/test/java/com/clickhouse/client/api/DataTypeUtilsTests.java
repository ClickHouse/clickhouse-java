package com.clickhouse.client.api;

import org.testng.annotations.Test;

import java.time.LocalDateTime;

import static org.testng.AssertJUnit.assertEquals;

public class DataTypeUtilsTests {


    @Test
    public void testDateTimeFormatter() {
        LocalDateTime dateTime = LocalDateTime.of(2021, 12, 31, 23, 59, 59);
        String formattedDateTime = dateTime.format(DataTypeUtils.DATETIME_FORMATTER);
        assertEquals("2021-12-31 23:59:59", formattedDateTime);
    }

    @Test
    public void testDateFormatter() {
        LocalDateTime date = LocalDateTime.of(2021, 12, 31, 10, 20);
        String formattedDate = date.toLocalDate().format(DataTypeUtils.DATE_FORMATTER);
        assertEquals("2021-12-31", formattedDate);
    }

    @Test
    public void testDateTimeWithNanosFormatter() {
        LocalDateTime dateTime = LocalDateTime.of(2021, 12, 31, 23, 59, 59, 123456789);
        String formattedDateTimeWithNanos = dateTime.format(DataTypeUtils.DATETIME_WITH_NANOS_FORMATTER);
        assertEquals("2021-12-31 23:59:59.123456789", formattedDateTimeWithNanos);
    }
}
