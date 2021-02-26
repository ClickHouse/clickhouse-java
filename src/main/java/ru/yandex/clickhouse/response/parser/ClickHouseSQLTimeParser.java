package ru.yandex.clickhouse.response.parser;

import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

final class ClickHouseSQLTimeParser extends ClickHouseDateValueParser<Time> {

    private static ClickHouseSQLTimeParser instance;

    static ClickHouseSQLTimeParser getInstance() {
        if (instance == null) {
            instance = new ClickHouseSQLTimeParser();
        }
        return instance;
    }

    private ClickHouseSQLTimeParser() {
        super(Time.class);
    }

    @Override
    Time parseDate(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return new Time(
            LocalDateTime.of(
                LocalDate.ofEpochDay(0),
                LocalTime.MIDNIGHT)
            .atZone(effectiveTimeZone(columnInfo, timeZone))
            .toInstant()
            .toEpochMilli());
    }

    @Override
    Time parseDateTime(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return new Time(parseAsLocalDateTime(value)
            .atZone(effectiveTimeZone(columnInfo, timeZone))
            .withFixedOffsetZone()
            .withYear(1970)
            .withMonth(1)
            .withDayOfMonth(1)
            .toInstant()
            .toEpochMilli());
    }

    @Override
    Time parseNumber(long value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return new Time(
            LocalDateTime.of(
                LocalDate.ofEpochDay(0),
                parseAsLocalTime(value))
            .atZone(effectiveTimeZone(columnInfo, timeZone))
            .toInstant()
            .toEpochMilli());
    }

    @Override
    Time parseOther(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return new Time(
                LocalDateTime.of(
                    LocalDate.ofEpochDay(0),
                    LocalTime.parse(value, DateTimeFormatter.ISO_LOCAL_TIME))
                .atZone(effectiveTimeZone(columnInfo, timeZone))
                .toInstant()
                .toEpochMilli());
        } catch (DateTimeParseException dtpe) {
            // try next pattern candidate
        }

        return new Time(
            LocalDateTime.of(
                LocalDate.ofEpochDay(0),
                parseAsLocalTime(value))
            .atZone(effectiveTimeZone(columnInfo, timeZone))
            .toInstant()
            .toEpochMilli());
    }

}
