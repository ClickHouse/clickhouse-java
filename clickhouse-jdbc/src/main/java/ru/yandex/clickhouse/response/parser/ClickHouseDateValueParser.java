package ru.yandex.clickhouse.response.parser;

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Objects;
import java.util.TimeZone;
import java.util.regex.Pattern;

import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.except.ClickHouseUnknownException;
import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

abstract class ClickHouseDateValueParser<T> extends ClickHouseValueParser<T> {
    private static final ZoneId UTC_ZONE = ZoneId.of("UTC");

    private static final Pattern PATTERN_EMPTY_DATE =
        Pattern.compile("^(0000-00-00|0000-00-00 00:00:00|0)$");

    private static final DateTimeFormatter DATE_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy[-]MM[-]dd");
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd['T'][ ]HH:mm:ss");
    private static final DateTimeFormatter TIME_FORMATTER_NUMBERS =
        DateTimeFormatter.ofPattern("HH[mm][ss]")
            .withResolverStyle(ResolverStyle.STRICT);

    private final Class<T> clazz;

    protected ClickHouseDateValueParser(Class<T> clazz) {
        this.clazz = Objects.requireNonNull(clazz);
    }

    protected LocalDateTime dateToLocalDate(String value, ClickHouseColumnInfo columnInfo, TimeZone timeZone) {
        return parseAsLocalDate(value).atStartOfDay();
    }

    protected LocalDateTime dateTimeToLocalDateTime(String value, ClickHouseColumnInfo columnInfo, TimeZone timeZone) {
        TimeZone serverTimeZone = columnInfo.getTimeZone();
        LocalDateTime localDateTime = parseAsLocalDateTime(value);
        if (serverTimeZone != null && (serverTimeZone.useDaylightTime() || serverTimeZone.getRawOffset() > 0)) { // non-UTC
            localDateTime = localDateTime.atZone(columnInfo.getTimeZone().toZoneId())
                .withZoneSameInstant(java.time.ZoneId.of("UTC")).toLocalDateTime();
        }

        return localDateTime;
    }

    protected ZonedDateTime dateToZonedDateTime(String value, ClickHouseColumnInfo columnInfo, TimeZone timeZone) {
        LocalDate localDate = parseAsLocalDate(value);
        return localDate.atStartOfDay(timeZone != null ? timeZone.toZoneId() : ZoneId.systemDefault());
    }

    protected ZonedDateTime dateTimeToZonedDateTime(String value, ClickHouseColumnInfo columnInfo, TimeZone timeZone) {
        LocalDateTime localDateTime = parseAsLocalDateTime(value);
        return timeZone != null && !timeZone.equals(columnInfo.getTimeZone())
            ? localDateTime.atZone(columnInfo.getTimeZone().toZoneId()).withZoneSameInstant(timeZone.toZoneId())
            : localDateTime.atZone(columnInfo.getTimeZone().toZoneId());
    }

    @Override
    public T parse(ByteFragment value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone) throws ClickHouseException
    {

        if (value.isNull()) {
            return null;
        }

        String s = value.asString();

        /*
         *  filter default values for relevant data types,
         *  even if the column has nullable flag set.
         */
        if (PATTERN_EMPTY_DATE.matcher(s).matches()) {
            return null;
        }

        switch (columnInfo.getEffectiveClickHouseDataType()) {
            case Date:
                try {
                    return parseDate(s, columnInfo, timeZone);
                } catch (Exception e) {
                    throw new ClickHouseUnknownException(
                        "Error parsing '" + s + "' of data type '"
                            + columnInfo.getOriginalTypeName()
                            + "' as " + clazz.getName(),
                        e);
                }
            case DateTime:
            case DateTime32:
            case DateTime64:
                try {
                    return parseDateTime(s, columnInfo, timeZone);
                } catch (Exception e) {
                    throw new ClickHouseUnknownException(
                        "Error parsing '" + s + "' of data type '"
                            + columnInfo.getOriginalTypeName()
                            + "' as " + clazz.getName(),
                        e);
                }
            case Int8:
            case Int16:
            case Int32:
            case Int64:
            case UInt8:
            case UInt16:
            case UInt32:
                try {
                    long l = Long.parseLong(s);
                    return parseNumber(l, columnInfo, timeZone);
                } catch (Exception e) {
                    throw new ClickHouseUnknownException(
                        "Error parsing '" + s + "' of data type '"
                            + columnInfo.getOriginalTypeName()
                            + "' as " + clazz.getName(),
                        e);
                }
            case UInt64:
                // If we have a large nanos value, we trim to millis
                try {
                    BigInteger bi = new BigInteger(s);
                    if (bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
                        return parseNumber(
                            bi.divide(BigInteger.valueOf(1000_000L)).longValue(),
                            columnInfo,
                            timeZone);
                    }
                    return parseNumber(bi.longValue(), columnInfo, timeZone);
                } catch (Exception e) {
                    throw new ClickHouseUnknownException(
                        "Error parsing '" + s + "' of data type '"
                            + columnInfo.getOriginalTypeName()
                            + "' as " + clazz.getName(),
                        e);
                }
            case String:
            case Unknown:
                try {
                    return parseOther(s, columnInfo, timeZone);
                } catch (Exception e) {
                    throw new ClickHouseUnknownException(
                        "Error parsing '" + s + "' as " + clazz.getName(), e);
                }
            default:
                throw new ClickHouseUnknownException(
                    "Error parsing '" + s + "' of data type '"
                        + columnInfo.getOriginalTypeName()
                        + "' as " + clazz.getName(),
                    null);
        }

    }

    abstract T parseDate(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone);

    abstract T parseDateTime(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone);

    abstract T parseNumber(long value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone);

    abstract T parseOther(String value, ClickHouseColumnInfo columnInfo,
        TimeZone timeZone);

    protected final ZoneId effectiveTimeZone(ClickHouseColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return timeZone != null 
            ? timeZone.toZoneId()
            : columnInfo.getTimeZone() != null
                ? columnInfo.getTimeZone().toZoneId()
                : ZoneId.systemDefault();
    }

    protected final LocalDate parseAsLocalDate(String value) {
        return LocalDate.parse(value, DATE_FORMATTER);
    }

    protected final LocalDateTime parseAsLocalDateTime(String value) {
        int index = Objects.requireNonNull(value).indexOf('.');
        if (index > 0) {
            int endIndex = -1;
            for (int i = index + 1, len = value.length(); i < len; i++) {
                char ch = value.charAt(i);
                if (!Character.isDigit(ch)) {
                    endIndex = i;
                    break;
                }
            }
            String part1 = value.substring(0, index);
            if (endIndex > index) {
                part1 += value.substring(endIndex);
            }
            String part2 = endIndex > index ? value.substring(index, endIndex) : value.substring(index);

            LocalDateTime ts = LocalDateTime.parse(part1, DATE_TIME_FORMATTER);
            int nanoSeconds = (int) Math.round(Double.parseDouble(part2) * 1000000000);
            return LocalDateTime.of(ts.getYear(), ts.getMonth(), ts.getDayOfMonth(), 
                ts.getHour(), ts.getMinute(), ts.getSecond(), nanoSeconds);
        }

        return LocalDateTime.parse(value, DATE_TIME_FORMATTER);
    }

    protected final OffsetDateTime parseAsOffsetDateTime(String value) {
        return OffsetDateTime.parse(value, DateTimeFormatter.ISO_DATE_TIME);
    }

    protected final Instant parseAsInstant(String value) {
        try {
            long l = Long.parseLong(value);
            return parseAsInstant(l);
        } catch (NumberFormatException nfe) {
            throw new DateTimeParseException("unparsable as long", value, -1, nfe);
        }
    }

    protected final Instant parseAsInstant(long value) {
        return value > Integer.MAX_VALUE
            ? Instant.ofEpochMilli(value)
            : Instant.ofEpochSecond(value);
    }

    protected final LocalTime parseAsLocalTime(String value) {
        return LocalTime.parse(
            value.length() % 2 == 0 ? value : "0" + value,
            TIME_FORMATTER_NUMBERS);
    }

    protected final LocalTime parseAsLocalTime(long value) {
        return parseAsLocalTime(String.valueOf(value));
    }

}
