package com.clickhouse.client.internal;

import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

/**
 * Tests playground
 */
public class SmallTests {


    @Test
    public void testInstantVsLocalTime() {

        // Date
        LocalDate longBeforeEpoch = LocalDate.ofEpochDay(-47482);
        LocalDate beforeEpoch = LocalDate.ofEpochDay(-1);
        LocalDate epoch = LocalDate.ofEpochDay(0);
        LocalDate dateMaxValue = LocalDate.ofEpochDay(65535);
        LocalDate date32MaxValue = LocalDate.ofEpochDay(47482);

        System.out.println(longBeforeEpoch);
        System.out.println(beforeEpoch);
        System.out.println(epoch);
        System.out.println(date32MaxValue);
        System.out.println(dateMaxValue);

        System.out.println();

        // Time

        LocalDateTime beforeEpochTime = LocalDateTime.ofEpochSecond(-999, 0, ZoneOffset.UTC);
        LocalDateTime epochTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);
        LocalDateTime maxTime = LocalDateTime.ofEpochSecond(TimeUnit.HOURS.toSeconds(999) + TimeUnit.MINUTES.toSeconds(59) + 59,
                123999999, ZoneOffset.UTC);

        System.out.println(beforeEpochTime);
        System.out.println("before time: " + (beforeEpochTime.getSecond()));
        System.out.println(epochTime);
        System.out.println(maxTime);
        System.out.println(maxTime.getDayOfYear());
    }
}
