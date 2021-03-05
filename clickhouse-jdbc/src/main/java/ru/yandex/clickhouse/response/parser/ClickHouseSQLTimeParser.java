package ru.yandex.clickhouse.response.parser;

import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

final class ClickHouseSQLTimeParser extends ClickHouseDateValueParser<Time> {
    private static final long MILLISECONDS_A_DAY = 24 * 3600 * 1000;

    private static ClickHouseSQLTimeParser instance;

    static ClickHouseSQLTimeParser getInstance() {
        if (instance == null) {
            instance = new ClickHouseSQLTimeParser();
        }
        return instance;
    }

    static long normalize(ClickHouseColumnInfo info, long time) {
        if (info == null ||
            (info.getClickHouseDataType() != ClickHouseDataType.DateTime64 && info.getScale() == 0)) {
            time -= time % 1000;
        }
        return (time + MILLISECONDS_A_DAY) % MILLISECONDS_A_DAY;
    }

    private ClickHouseSQLTimeParser() {
        super(Time.class);
    }

    @Override
    Time parseDate(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return new Time(normalize(columnInfo,
            LocalDateTime.of(
                LocalDate.ofEpochDay(0),
                LocalTime.MIDNIGHT)
            .atZone(effectiveTimeZone(columnInfo, timeZone))
            .toInstant()
            .toEpochMilli()));
    }

    @Override
    Time parseDateTime(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return new Time(normalize(columnInfo, parseAsLocalDateTime(value)
            .atZone(effectiveTimeZone(columnInfo, timeZone))
            .toInstant()
            .toEpochMilli()));
    }

    @Override
    Time parseNumber(long value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return new Time(normalize(columnInfo,
            LocalDateTime.of(
                LocalDate.ofEpochDay(0),
                parseAsLocalTime(value))
            .atZone(effectiveTimeZone(columnInfo, timeZone))
            .toInstant()
            .toEpochMilli()));
    }

    @Override
    Time parseOther(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return new Time(normalize(columnInfo,
                LocalDateTime.of(
                    LocalDate.ofEpochDay(0),
                    LocalTime.parse(value, DateTimeFormatter.ISO_LOCAL_TIME))
                .atZone(effectiveTimeZone(columnInfo, timeZone))
                .toInstant()
                .toEpochMilli()));
        } catch (DateTimeParseException dtpe) {
            // try next pattern candidate
        }

        return new Time(normalize(columnInfo,
            LocalDateTime.of(
                LocalDate.ofEpochDay(0),
                parseAsLocalTime(value))
            .atZone(effectiveTimeZone(columnInfo, timeZone))
            .toInstant()
            .toEpochMilli()));
    }

}
