package ru.yandex.clickhouse.response.parser;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

final class ClickHouseOffsetTimeParser extends ClickHouseDateValueParser<OffsetTime> {

    private static ClickHouseOffsetTimeParser instance;

    static ClickHouseOffsetTimeParser getInstance() {
        if (instance == null) {
            instance = new ClickHouseOffsetTimeParser();
        }
        return instance;
    }

    private ClickHouseOffsetTimeParser() {
        super(OffsetTime.class);
    }

    @Override
    OffsetTime parseDate(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return dateToZonedDateTime(value, columnInfo, timeZone).toOffsetDateTime().toOffsetTime();
    }

    @Override
    OffsetTime parseDateTime(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return dateTimeToZonedDateTime(value, columnInfo, timeZone).toOffsetDateTime().toOffsetTime();
    }

    @Override
    OffsetTime parseNumber(long value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return OffsetTime.of(
            parseAsLocalTime(value),
            effectiveTimeZone(columnInfo, timeZone).getRules().getOffset(Instant.now()));
    }

    @Override
    OffsetTime parseOther(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return OffsetTime.parse(value, DateTimeFormatter.ISO_OFFSET_TIME);
        } catch (DateTimeParseException dtpe) {
            // try next pattern
        }
        try {
            return OffsetTime.of(
                LocalTime.parse(value, DateTimeFormatter.ISO_LOCAL_TIME),
                effectiveTimeZone(columnInfo, timeZone).getRules().getOffset(Instant.now()));
        } catch (DateTimeParseException dtpe) {
            // try next pattern
        }
        return OffsetTime.of(
            parseAsLocalTime(value),
            effectiveTimeZone(columnInfo, timeZone).getRules().getOffset(Instant.now()));
    }

}
