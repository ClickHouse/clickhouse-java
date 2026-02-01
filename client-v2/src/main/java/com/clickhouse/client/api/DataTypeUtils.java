package com.clickhouse.client.api;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.data.ClickHouseDataType;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
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

    public static LocalDateTime localTimeFromTime64Integer(int precision, long value) {
        int nanoSeconds = 0;
        if (precision > 0) {
            int factor = BinaryStreamReader.BASES[precision];
            nanoSeconds = Math.abs((int) (value % factor)); // nanoseconds are stored separately and only positive values accepted
            value /= factor;

            if (nanoSeconds > 0L) {
                nanoSeconds *= BASES[9 - precision];
            }

        }

        return LocalDateTime.ofEpochSecond(value, nanoSeconds, ZoneOffset.UTC);
    }

    /**
     * Converts a {@link Duration} to a time string in the format {@code [-]HH:mm:ss[.nnnnnnnnn]}.
     * <p>
     * Unlike standard time formats, hours can exceed 24 and can be negative.
     * The precision parameter controls the number of fractional second digits (0-9).
     *
     * @param duration  the duration to convert
     * @param precision the number of fractional second digits (0-9)
     * @return a string representation like {@code -999:59:59.123456789}
     * @throws NullPointerException if {@code duration} is null
     */
    public static String durationToTimeString(Duration duration, int precision) {
        Objects.requireNonNull(duration, "Duration required for durationToTimeString");

        boolean negative = duration.isNegative();
        if (negative) {
            duration = duration.negated();
        }

        long totalSeconds = duration.getSeconds();
        int nanos = duration.getNano();

        long hours = totalSeconds / 3600;
        int minutes = (int) ((totalSeconds % 3600) / 60);
        int seconds = (int) (totalSeconds % 60);

        StringBuilder sb = new StringBuilder();
        if (negative) {
            sb.append('-');
        }
        sb.append(hours);
        sb.append(':');
        if (minutes < 10) {
            sb.append('0');
        }
        sb.append(minutes);
        sb.append(':');
        if (seconds < 10) {
            sb.append('0');
        }
        sb.append(seconds);

        if (precision > 0 && precision <= 9) {
            sb.append('.');
            // Format nanos with leading zeros, then truncate to precision
            String nanosStr = String.format("%09d", nanos);
            sb.append(nanosStr, 0, precision);
        }

        return sb.toString();
    }

    public static Duration localDateTimeToDuration(LocalDateTime localDateTime) {
        return Duration.ofSeconds(localDateTime.toEpochSecond(ZoneOffset.UTC))
            .plusNanos(localDateTime.getNano());
    }
}
