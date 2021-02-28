package ru.yandex.clickhouse.response.parser;

import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

final class ClickHouseZonedDateTimeParser extends ClickHouseDateValueParser<ZonedDateTime> {

    private static ClickHouseZonedDateTimeParser instance;

    static ClickHouseZonedDateTimeParser getInstance() {
        if (instance == null) {
            instance = new ClickHouseZonedDateTimeParser();
        }
        return instance;
    }

    private ClickHouseZonedDateTimeParser() {
        super(ZonedDateTime.class);
    }

    @Override
    ZonedDateTime parseDate(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsLocalDate(value)
            .atStartOfDay(effectiveTimeZone(columnInfo, timeZone));
    }

    @Override
    ZonedDateTime parseDateTime(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsLocalDateTime(value)
            .atZone(effectiveTimeZone(columnInfo, timeZone));
    }

    @Override
    ZonedDateTime parseNumber(long value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsInstant(value)
            .atZone(effectiveTimeZone(columnInfo, timeZone));
    }

    @Override
    ZonedDateTime parseOther(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return parseAsLocalDateTime(value)
                .atZone(effectiveTimeZone(columnInfo, timeZone));
        } catch(DateTimeParseException dtpe) {
            // try next candidate
        }

        try {
            return OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .toZonedDateTime();
        } catch (DateTimeParseException dtpe) {
            // try another way
        }

        return parseAsInstant(value)
            .atZone(effectiveTimeZone(columnInfo, timeZone));
    }

}
