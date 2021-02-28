package ru.yandex.clickhouse.response.parser;

import java.sql.Date;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.TimeZone;

import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

final class ClickHouseSQLDateParser extends ClickHouseDateValueParser<Date> {

    private static ClickHouseSQLDateParser instance;

    static ClickHouseSQLDateParser getInstance() {
        if (instance == null) {
            instance = new ClickHouseSQLDateParser();
        }
        return instance;
    }

    private ClickHouseSQLDateParser() {
        super(Date.class);
    }

    @Override
    Date parseDate(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return new Date(parseAsLocalDate(value)
            .atStartOfDay(getParsingTimeZone(columnInfo, timeZone))
            .toInstant()
            .toEpochMilli());
    }

    @Override
    Date parseDateTime(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
      return new Date(parseAsLocalDateTime(value)
          .atZone(getParsingTimeZone(columnInfo, timeZone))
          .withZoneSameInstant(getResultTimeZone(timeZone))
          .truncatedTo(ChronoUnit.DAYS)
          .toInstant()
          .toEpochMilli());
    }

    @Override
    Date parseNumber(long value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return new Date(parseAsInstant(value)
            .atZone(getResultTimeZone(timeZone))
            .truncatedTo(ChronoUnit.DAYS)
            .toInstant()
            .toEpochMilli());
    }

    @Override
    Date parseOther(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return new Date(parseAsInstant(value)
                .atZone(getResultTimeZone(timeZone))
                .truncatedTo(ChronoUnit.DAYS)
                .toInstant()
                .toEpochMilli());
        } catch (DateTimeParseException dtpe) {
            // try next candidate
        }

        try {
            return new Date(parseAsOffsetDateTime(value)
                .toInstant()
                .atZone(getResultTimeZone(timeZone))
                .truncatedTo(ChronoUnit.DAYS)
                .toInstant()
                .toEpochMilli());
        } catch (DateTimeParseException dtpe) {
            // try next candidate
        }

        try {
            return new Date(parseAsLocalDateTime(value)
                  .atZone(getParsingTimeZone(columnInfo, timeZone))
                  .withZoneSameInstant(getResultTimeZone(timeZone))
                  .truncatedTo(ChronoUnit.DAYS)
                  .toInstant()
                  .toEpochMilli());
        } catch (DateTimeParseException dtpe) {
            // try next candidate
        }

        return new Date(LocalDateTime
            .of(
                parseAsLocalDate(value),
                LocalTime.MIDNIGHT)
            .atZone(getResultTimeZone(timeZone))
            .toInstant()
            .toEpochMilli());
    }

    private static ZoneId getParsingTimeZone(ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return columnInfo.getTimeZone() != null
            ? columnInfo.getTimeZone().toZoneId()
            : timeZone != null
                ? timeZone.toZoneId()
                : ZoneId.systemDefault();
    }

    private static ZoneId getResultTimeZone(TimeZone timeZone) {
        return timeZone != null
            ? timeZone.toZoneId()
            : ZoneId.systemDefault();
    }

}
