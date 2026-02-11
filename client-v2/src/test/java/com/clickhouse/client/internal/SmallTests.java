package com.clickhouse.client.internal;

import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Tests playground
 */
public class SmallTests {



    @Test
    public void testTimestamp() {

        Calendar utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        utcCal.set(2016, 6 - 1, 8, 11, 00, 00);
        Calendar local = Calendar.getInstance();
        local.set(2016, 6 - 1, 8, 11, 00, 00);


        Timestamp utcTs = new Timestamp(utcCal.getTimeInMillis());
        Timestamp localTs = new Timestamp(local.getTimeInMillis());

        printTimestamp(utcTs, "utcTs");
        printTimestamp(localTs, "localTs");
        printTimestamp(Timestamp.valueOf(LocalDateTime.now()), "nowTs");


    }

    private void printTimestamp(Timestamp ts, String name) {
        System.out.println("----------------");
        System.out.println(name + ": " + ts);
        System.out.println(name + ".toLocalDateTime: " + ts.toLocalDateTime());
        System.out.println(name + ".toInstant: " + ts.toInstant());
        System.out.println(name + ".getTime: " + ts.getTime());
    }
}
