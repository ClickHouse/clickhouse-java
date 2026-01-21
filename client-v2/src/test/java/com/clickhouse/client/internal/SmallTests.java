package com.clickhouse.client.internal;

import com.clickhouse.client.api.DataTypeUtils;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Tests playground
 */
public class SmallTests {


    @Test
    public void localDateTimeValues() {
        String srcDate = "2024-12-31";
        String srcTime = "11:59:59";
        String srcTimestamp = srcDate + "T" + srcTime;


        LocalDate localDate = LocalDate.parse(srcDate, DateTimeFormatter.ISO_LOCAL_DATE);
        LocalTime localTime = LocalTime.parse(srcTime, DateTimeFormatter.ISO_LOCAL_TIME);
        LocalDateTime localTimestamp = LocalDateTime.parse(srcTimestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME);

        System.out.println("\n" + localDate + "\n" + localTime + "\n" +localTimestamp);

        Timestamp oldTimestamp = new Timestamp(ZonedDateTime.now(ZoneId.of("UTC")).toEpochSecond() * 1000);
        System.out.println("oldTimestamp instant: " + oldTimestamp.toInstant());
        System.out.println("local datetime: " + LocalDateTime.now());
        System.out.println("local datetime from oldTS: " + oldTimestamp.toLocalDateTime());
    }


    @Test
    public void oldJDBCDateConversion() throws Exception {
        // see com.clickhouse.jdbc.internal.SqlBasedPreparedStatement.setDate

        Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        utc.set(2026, 0, 1, 0, 0, 0);
        System.out.println("utc: " + utc.toInstant());
        Date d1 = new Date(utc.getTimeInMillis());
        System.out.println("d1: " + d1.toLocalDate() + " (ts: " + d1.getTime() + ")");
        System.out.println("d1 converted: " + DataTypeUtils.toLocalDate(d1, utc));

        // else
        Calendar convertCal = (Calendar) utc.clone();
        convertCal.setTime(d1);
        Instant convInst =convertCal.toInstant();
        System.out.println("converted Instant: " + convInst);
        ZonedDateTime convZoned = convInst.atZone(utc.getTimeZone().toZoneId()).withZoneSameInstant(ZoneId.of("America/New_York"));
        System.out.println("convZoned: " + convZoned);
        LocalDate convDate = convZoned.toLocalDate();
        System.out.println("convDate: " + convDate);
        System.out.println("local date time " + convZoned.toLocalDateTime());
    }

}
