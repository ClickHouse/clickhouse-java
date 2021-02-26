package ru.yandex.clickhouse.response.parser;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

final class ClickHouseLocalTimeParser extends ClickHouseDateValueParser<LocalTime> {

    private static ClickHouseLocalTimeParser instance;

    static ClickHouseLocalTimeParser getInstance() {
        if (instance == null) {
            instance = new ClickHouseLocalTimeParser();
        }
        return instance;
    }

    private ClickHouseLocalTimeParser() {
        super(LocalTime.class);
    }

    @Override
    LocalTime parseDate(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return LocalTime.MIDNIGHT;
    }

    @Override
    LocalTime parseDateTime(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsLocalDateTime(value).toLocalTime();
    }

    @Override
    LocalTime parseNumber(long value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsLocalTime(value);
    }

    @Override
    LocalTime parseOther(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return LocalTime.parse(value, DateTimeFormatter.ISO_LOCAL_TIME);
        } catch (DateTimeParseException dtpe) {
            // try different pattern
        }
        try {
            return LocalTime.parse(value, DateTimeFormatter.ISO_OFFSET_TIME);
        } catch (DateTimeParseException dtpe) {
            // try different pattern
        }
        return parseAsLocalTime(value);
    }

}
