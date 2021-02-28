package ru.yandex.clickhouse.response.parser;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

final class ClickHouseOffsetDateTimeParser extends ClickHouseDateValueParser<OffsetDateTime> {

    private static ClickHouseOffsetDateTimeParser instance;

    static ClickHouseOffsetDateTimeParser getInstance() {
        if (instance == null) {
            instance = new ClickHouseOffsetDateTimeParser();
        }
        return instance;
    }

    private ClickHouseOffsetDateTimeParser() {
        super(OffsetDateTime.class);
    }

    @Override
    OffsetDateTime parseDate(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsLocalDate(value)
            .atStartOfDay(effectiveTimeZone(columnInfo, timeZone))
            .toOffsetDateTime();
    }

    @Override
    OffsetDateTime parseDateTime(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsLocalDateTime(value)
            .atZone(effectiveTimeZone(columnInfo, timeZone))
            .toOffsetDateTime();
    }

    @Override
    OffsetDateTime parseNumber(long value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsInstant(value)
            .atZone(effectiveTimeZone(columnInfo, timeZone))
            .toOffsetDateTime();
    }

    @Override
    OffsetDateTime parseOther(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return parseAsLocalDateTime(value)
                .atZone(effectiveTimeZone(columnInfo, timeZone))
                .toOffsetDateTime();
        } catch (DateTimeParseException dtpe) {
            // try another way
        }
        try {
            return OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        } catch (DateTimeParseException dtpe) {
            // try another way
        }
        return parseAsInstant(value)
            .atZone(effectiveTimeZone(columnInfo, timeZone))
            .toOffsetDateTime();
    }

}
