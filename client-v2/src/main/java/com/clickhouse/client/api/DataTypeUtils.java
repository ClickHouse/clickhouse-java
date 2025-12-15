package com.clickhouse.client.api;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.data.ClickHouseDataType;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Objects;

import static com.clickhouse.client.api.data_formats.internal.BinaryStreamReader.BASES;

public class DataTypeUtils {

    /**
     * Formatter for the DateTime type.
     */
    public static DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");

    /**
     * Formatter for the Date type.
     */
    public static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");

    /**
     * Formatter for the DateTime type with nanoseconds.
     */
    public static DateTimeFormatter DATETIME_WITH_NANOS_FORMATTER = new DateTimeFormatterBuilder().appendPattern("uuuu-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter();

    private static final DateTimeFormatter INSTANT_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.INSTANT_SECONDS)
        .appendFraction(ChronoField.NANO_OF_SECOND, 9, 9, true)
        .toFormatter();

    public static final DateTimeFormatter TIME_WITH_NANOS_FORMATTER = INSTANT_FORMATTER;

    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    /**
     * Formats an {@link Instant} object for use in SQL statements or as query
     * parameter.
     *
     * @param instant
     *            the Java object to format
     * @return a suitable String representation of {@code instant}
     * @throws NullPointerException
     *             if {@code instant} is null
     */
    public static String formatInstant(Instant instant) {
        return formatInstant(instant, null);
    }

    /**
     * Formats an {@link Instant} object for use in SQL statements or as query
     * parameter.
     *
     * This method uses the {@code dataTypeHint} parameter to find the best
     * suitable format for the instant.
     *
     * @param instant
     *            the Java object to format
     * @param dataTypeHint
     *            the ClickHouse data type {@code instant} should be used for
     * @return a suitable String representation of {@code instant}
     * @throws NullPointerException
     *             if {@code instant} is null
     */
    public static String formatInstant(Instant instant, ClickHouseDataType dataTypeHint) {
        return formatInstant(instant, dataTypeHint, null);
    }

    /**
     * Formats an {@link Instant} object for use in SQL statements or as query
     * parameter.
     *
     * This method uses the {@code dataTypeHint} parameter to find the best
     * suitable format for the instant.
     *
     * For <em>some</em> formatting operations, providing a {@code timeZone} is
     * mandatory, e.g. for {@link ClickHouseDataType#Date}.
     *
     * @param instant
     *            the Java object to format
     * @param dataTypeHint
     *            the ClickHouse data type {@code object} should be used for
     * @param timeZone
     *            the time zone to be used when formatting the instant for use
     *            in non-time-zone-based ClickHouse data types
     * @return a suitable String representation of {@code object}, or the empty
     *         String for {@code null} objects
     * @throws NullPointerException
     *             if {@code instant} is null
     */
    public static String formatInstant(Instant instant, ClickHouseDataType dataTypeHint,
        ZoneId timeZone)
    {
        Objects.requireNonNull(instant, "Instant required for formatInstant");
        if (dataTypeHint == null) {
            return formatInstantDefault(instant);
        }
        switch (dataTypeHint) {
            case Date:
            case Date32:
                Objects.requireNonNull(
                    timeZone,
                    "TimeZone required for formatting Instant for '" + dataTypeHint + "' use");
                return DATE_FORMATTER.format(
                    instant.atZone(timeZone).toLocalDate());
            case DateTime:
            case DateTime32:
                return String.valueOf(instant.getEpochSecond());
            default:
                return formatInstantDefault(instant);
        }
    }

    private static String formatInstantDefault(Instant instant) {
        return INSTANT_FORMATTER.format(instant);
    }

    public static Instant instantFromTime64Integer(int precision, long value) {
        int nanoSeconds = 0;
        if (precision > 0) {
            int factor = BinaryStreamReader.BASES[precision];
            nanoSeconds = (int) (value % factor);
            value /= factor;
            if (nanoSeconds < 0) {
                nanoSeconds += factor;
                value--;
            }
            if (nanoSeconds > 0L) {
                nanoSeconds *= BASES[9 - precision];
            }
        }

        return Instant.ofEpochSecond(value, nanoSeconds);
    }

    /**
     * Formats a {@link Date} object to ClickHouse date format string.
     * The result can be used in SQL statements or with functions like parseDateTimeBestEffort.
     *
     * @param date the Java Date object to format
     * @return a date string in format "yyyy-MM-dd"
     * @throws NullPointerException if {@code date} is null
     */
    public static String formatDate(Date date) {
        Objects.requireNonNull(date, "Date required for formatDate");
        return DATE_FORMATTER.format(date.toLocalDate());
    }

    /**
     * Formats a {@link Time} object to ClickHouse time format string.
     * The result can be used in SQL statements or with functions like parseDateTimeBestEffort.
     *
     * @param time the Java Time object to format
     * @return a time string in format "HH:mm:ss"
     * @throws NullPointerException if {@code time} is null
     */
    public static String formatTime(Time time) {
        Objects.requireNonNull(time, "Time required for formatTime");
        return TIME_FORMATTER.format(time.toLocalTime());
    }

    /**
     * Formats a {@link Timestamp} object to ClickHouse datetime format string.
     * The result can be used in SQL statements or with functions like parseDateTimeBestEffort.
     *
     * @param timestamp the Java Timestamp object to format
     * @return a datetime string in format "yyyy-MM-dd HH:mm:ss" or "yyyy-MM-dd HH:mm:ss.nnnnnnnnn" if nanoseconds are present
     * @throws NullPointerException if {@code timestamp} is null
     */
    public static String formatTimestamp(Timestamp timestamp) {
        Objects.requireNonNull(timestamp, "Timestamp required for formatTimestamp");
        LocalDateTime ldt = timestamp.toLocalDateTime();
        if (timestamp.getNanos() % 1_000_000_000 == 0) {
            return DATETIME_FORMATTER.format(ldt);
        } else {
            return DATETIME_WITH_NANOS_FORMATTER.format(ldt);
        }
    }

    /**
     * Formats an {@link Instant} object to ClickHouse datetime format string.
     * The result can be used in SQL statements or with functions like parseDateTimeBestEffort.
     * Uses UTC timezone for formatting.
     *
     * @param instant the Java Instant object to format
     * @return a datetime string in format "yyyy-MM-dd HH:mm:ss" or "yyyy-MM-dd HH:mm:ss.nnnnnnnnn" if nanoseconds are present
     * @throws NullPointerException if {@code instant} is null
     */
    public static String formatInstantToDateTime(Instant instant) {
        Objects.requireNonNull(instant, "Instant required for formatInstantToDateTime");
        LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
        if (instant.getNano() % 1_000_000_000 == 0) {
            return DATETIME_FORMATTER.format(ldt);
        } else {
            return DATETIME_WITH_NANOS_FORMATTER.format(ldt);
        }
    }

    /**
     * Formats an {@link OffsetDateTime} object to ClickHouse datetime format string.
     * The result can be used in SQL statements or with functions like parseDateTimeBestEffort.
     *
     * @param offsetDateTime the Java OffsetDateTime object to format
     * @return a datetime string in format "yyyy-MM-dd HH:mm:ss" or "yyyy-MM-dd HH:mm:ss.nnnnnnnnn" if nanoseconds are present
     * @throws NullPointerException if {@code offsetDateTime} is null
     */
    public static String formatOffsetDateTime(OffsetDateTime offsetDateTime) {
        Objects.requireNonNull(offsetDateTime, "OffsetDateTime required for formatOffsetDateTime");
        LocalDateTime ldt = offsetDateTime.toLocalDateTime();
        if (offsetDateTime.getNano() % 1_000_000_000 == 0) {
            return DATETIME_FORMATTER.format(ldt);
        } else {
            return DATETIME_WITH_NANOS_FORMATTER.format(ldt);
        }
    }

    /**
     * Formats a {@link ZonedDateTime} object to ClickHouse datetime format string.
     * The result can be used in SQL statements or with functions like parseDateTimeBestEffort.
     * Uses the local time component from the ZonedDateTime, which may be normalized by Java
     * if the original offset didn't match the zone's actual offset at that time.
     *
     * @param zonedDateTime the Java ZonedDateTime object to format
     * @return a datetime string in format "yyyy-MM-dd HH:mm:ss" or "yyyy-MM-dd HH:mm:ss.nnnnnnnnn" if nanoseconds are present
     * @throws NullPointerException if {@code zonedDateTime} is null
     */
    public static String formatZonedDateTime(ZonedDateTime zonedDateTime) {
        Objects.requireNonNull(zonedDateTime, "ZonedDateTime required for formatZonedDateTime");
        // Java normalizes ZonedDateTime when the explicit offset in the parse string doesn't match
        // the zone's actual offset at that time. We use the normalized local time, which is what
        // Java provides after normalization. If you need to preserve the exact offset, use
        // OffsetDateTime instead of ZonedDateTime.
        LocalDateTime ldt = zonedDateTime.toLocalDateTime();
        if (zonedDateTime.getNano() % 1_000_000_000 == 0) {
            return DATETIME_FORMATTER.format(ldt);
        } else {
            return DATETIME_WITH_NANOS_FORMATTER.format(ldt);
        }
    }
}
