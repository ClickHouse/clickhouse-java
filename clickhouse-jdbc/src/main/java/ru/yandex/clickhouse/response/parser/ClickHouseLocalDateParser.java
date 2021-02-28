package ru.yandex.clickhouse.response.parser;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

final class ClickHouseLocalDateParser extends ClickHouseDateValueParser<LocalDate> {

    private static ClickHouseLocalDateParser instance;

    static ClickHouseLocalDateParser getInstance() {
        if (instance == null) {
            instance = new ClickHouseLocalDateParser();
        }
        return instance;
    }

    private ClickHouseLocalDateParser() {
        super(LocalDate.class);
    }

    @Override
    LocalDate parseDate(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsLocalDate(value);
    }

    @Override
    LocalDate parseDateTime(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsLocalDateTime(value).toLocalDate();
    }

    @Override
    LocalDate parseNumber(long value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsInstant(value).atZone(timeZone.toZoneId()).toLocalDate();
    }

    @Override
    LocalDate parseOther(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return parseAsLocalDate(value);
        } catch (DateTimeParseException dtpe) {
            // not parseable as date
        }
        try {
            return parseAsLocalDateTime(value).toLocalDate();
        } catch (DateTimeParseException dtpe) {
            // not parseable as datetime
        }
        Instant i = parseAsInstant(value);
        return i.atZone(timeZone.toZoneId()).toLocalDate();
    }

}
