package ru.yandex.clickhouse.response.parser;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

final class ClickHouseLocalDateTimeParser extends ClickHouseDateValueParser<LocalDateTime> {

    private static ClickHouseLocalDateTimeParser instance;

    static ClickHouseLocalDateTimeParser getInstance() {
        if (instance == null) {
            instance = new ClickHouseLocalDateTimeParser();
        }
        return instance;
    }

    private ClickHouseLocalDateTimeParser() {
        super(LocalDateTime.class);
    }

    @Override
    LocalDateTime parseDate(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsLocalDate(value).atStartOfDay();
    }

    @Override
    LocalDateTime parseDateTime(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsLocalDateTime(value);
    }

    @Override
    LocalDateTime parseNumber(long value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsInstant(value)
            .atZone(timeZone.toZoneId())
            .toLocalDateTime();
    }

    @Override
    LocalDateTime parseOther(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return parseAsLocalDate(value).atStartOfDay();
        } catch (DateTimeParseException dtpe) {
            // not parseable as date
        }
        try {
            return parseAsLocalDateTime(value);
        } catch (DateTimeParseException dtpe) {
            // not parseable as datetime
        }
        Instant i = parseAsInstant(value);
        return i.atZone(timeZone.toZoneId()).toLocalDateTime();
    }

}
