package ru.yandex.clickhouse.response.parser;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

final class ClickHouseInstantParser extends ClickHouseDateValueParser<Instant> {

    private static ClickHouseInstantParser instance;

    static ClickHouseInstantParser getInstance() {
        if (instance == null) {
            instance = new ClickHouseInstantParser();
        }
        return instance;
    }

    private ClickHouseInstantParser() {
        super(Instant.class);
    }

    @Override
    Instant parseDate(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsLocalDate(value)
            .atStartOfDay(effectiveTimeZone(columnInfo, timeZone))
            .toInstant();
    }

    @Override
    Instant parseDateTime(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsLocalDateTime(value)
            .atZone(effectiveTimeZone(columnInfo, timeZone))
            .toInstant();
    }

    @Override
    Instant parseNumber(long value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return value > Integer.MAX_VALUE
            ? Instant.ofEpochMilli(value)
            : Instant.ofEpochSecond(value);
    }

    @Override
    Instant parseOther(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return parseAsLocalDate(value)
                .atStartOfDay(effectiveTimeZone(columnInfo, timeZone))
                .toInstant();
        } catch (DateTimeParseException dtpe) {
            // better luck next time
        }
        try {
            return parseAsLocalDateTime(value)
                .atZone(effectiveTimeZone(columnInfo, timeZone))
                .toInstant();
        } catch (DateTimeParseException dtpe) {
            // better luck next time
        }
        try {
            return parseAsOffsetDateTime(value)
                .toInstant();
        } catch (DateTimeParseException dtpe) {
            // better luck next time
        }
        return parseAsInstant(value);
    }
}
