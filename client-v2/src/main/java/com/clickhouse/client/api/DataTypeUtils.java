package com.clickhouse.client.api;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.data.ClickHouseDataType;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

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

    public static final DateTimeFormatter TIME_WITH_NANOS_FORMATTER = new DateTimeFormatterBuilder().appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter();;

    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static final DateTimeFormatter DATE_TIME_WITH_OPTIONAL_NANOS = new DateTimeFormatterBuilder().appendPattern("uuuu-MM-dd HH:mm:ss")
            .appendOptional(new DateTimeFormatterBuilder().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter())
            .toFormatter();

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


    /**
     * Converts a {@link java.sql.Date} to {@link LocalDate} using the specified timezone.
     *
     * <p>For default JVM timezone behavior, use {@link Date#toLocalDate()} directly.</p>
     *
     * @param sqlDate the java.sql.Date to convert
     * @param timeZone the timezone context
     * @return the LocalDate representing the date in the specified timezone
     * @throws NullPointerException if sqlDate or timeZone is null
     */
    public static LocalDate toLocalDate(Date sqlDate, TimeZone timeZone) {
        Objects.requireNonNull(sqlDate, "sqlDate must not be null");
        Objects.requireNonNull(timeZone, "timeZone must not be null");

        ZoneId zoneId = timeZone.toZoneId();
        return Instant.ofEpochMilli(sqlDate.getTime())
                .atZone(zoneId)
                .toLocalDate();
    }

    /**
     * Converts a {@link java.sql.Time} to {@link LocalTime} using the specified timezone.
     *
     * <p>For default JVM timezone behavior, use {@link Time#toLocalTime()} directly.</p>
     *
     * @param sqlTime the java.sql.Time to convert
     * @param timeZone the timezone context
     * @return the LocalTime representing the time in the specified timezone
     * @throws NullPointerException if sqlTime or timeZone is null
     */
    public static LocalTime toLocalTime(Time sqlTime, TimeZone timeZone) {
        Objects.requireNonNull(sqlTime, "sqlTime must not be null");
        Objects.requireNonNull(timeZone, "timeZone must not be null");

        ZoneId zoneId = timeZone.toZoneId();
        return Instant.ofEpochMilli(sqlTime.getTime())
                .atZone(zoneId)
                .toLocalTime();
    }

    /**
     * Converts a {@link java.sql.Timestamp} to {@link LocalDateTime} using the specified timezone.
     *
     * <p>Note: This method preserves nanosecond precision from the Timestamp.</p>
     *
     * <p>For default JVM timezone behavior, use {@link Timestamp#toLocalDateTime()} directly.</p>
     *
     * @param sqlTimestamp the java.sql.Timestamp to convert
     * @param timeZone the timezone context
     * @return the LocalDateTime representing the timestamp in the specified timezone
     * @throws NullPointerException if sqlTimestamp or timeZone is null
     */
    public static LocalDateTime toLocalDateTime(Timestamp sqlTimestamp, TimeZone timeZone) {
        Objects.requireNonNull(sqlTimestamp, "sqlTimestamp must not be null");
        Objects.requireNonNull(timeZone, "timeZone must not be null");

        ZoneId zoneId = timeZone.toZoneId();
        // Use Instant to preserve nanoseconds
        return LocalDateTime.ofInstant(sqlTimestamp.toInstant(), zoneId);
    }

    /**
     * Converts a {@link java.sql.Timestamp} to {@link ZonedDateTime} by expressing
     * the timestamp's instant in the specified timezone.
     *
     * <p>The underlying instant is preserved — only the timezone context changes.
     * This matches the JDBC {@code setTimestamp(int, Timestamp, Calendar)} contract
     * where the Calendar's timezone is used to interpret the Timestamp's absolute
     * point in time.</p>
     *
     * <p>Note: This method preserves nanosecond precision from the Timestamp.</p>
     *
     * @param sqlTimestamp the java.sql.Timestamp to convert
     * @param timeZone the timezone to express the instant in
     * @return the ZonedDateTime representing the same instant in the specified timezone
     * @throws NullPointerException if sqlTimestamp or timeZone is null
     */
    public static ZonedDateTime toZonedDateTime(Timestamp sqlTimestamp, TimeZone timeZone) {
        Objects.requireNonNull(sqlTimestamp, "sqlTimestamp must not be null");
        Objects.requireNonNull(timeZone, "timeZone must not be null");

        return sqlTimestamp.toInstant().atZone(timeZone.toZoneId());
    }

    // ==================== LocalDate/LocalTime/LocalDateTime to SQL types ====================

    /**
     * Converts a {@link LocalDate} to {@link java.sql.Date} using the specified timezone.
     *
     * <p>For default JVM timezone behavior, use {@link Date#valueOf(LocalDate)} directly.</p>
     *
     * @param localDate the LocalDate to convert
     * @param timeZone the timezone context
     * @return the java.sql.Date representing midnight on the specified date in the given timezone
     * @throws NullPointerException if localDate or timeZone is null
     */
    public static Date toSqlDate(LocalDate localDate, TimeZone timeZone) {
        Objects.requireNonNull(localDate, "localDate must not be null");
        Objects.requireNonNull(timeZone, "timeZone must not be null");

        long time = ZonedDateTime.of(localDate, LocalTime.MIDNIGHT, timeZone.toZoneId()).toEpochSecond() * 1000;
        return new Date(time);
    }

    /**
     * Converts a {@link LocalTime} to {@link java.sql.Time} using the specified timezone.
     *
     * <p>For default JVM timezone behavior, use {@link Time#valueOf(LocalTime)} directly.</p>
     *
     * @param localTime the LocalTime to convert
     * @param timeZone the timezone context
     * @return the java.sql.Time representing the specified time
     * @throws NullPointerException if localTime or timeZone is null
     */
    public static Time toSqlTime(LocalTime localTime, TimeZone timeZone) {
        Objects.requireNonNull(localTime, "localTime must not be null");
        Objects.requireNonNull(timeZone, "timeZone must not be null");

        ZoneId zoneId = timeZone.toZoneId();
        // java.sql.Time is based on January 1, 1970
        long epochMillis = localTime.atDate(LocalDate.of(1970, 1, 1))
                .atZone(zoneId)
                .toInstant()
                .toEpochMilli();
        return new Time(epochMillis);
    }


    /**
     * Converts a {@link LocalDateTime} to {@link java.sql.Timestamp} using the specified timezone.
     *
     * <p>Note: This method preserves nanosecond precision from the LocalDateTime.</p>
     *
     * <p>For default JVM timezone behavior, use {@link Timestamp#valueOf(LocalDateTime)} directly.</p>
     *
     * @param localDateTime the LocalDateTime to convert
     * @param timeZone the timezone context
     * @return the java.sql.Timestamp representing the specified date and time
     * @throws NullPointerException if localDateTime or timeZone is null
     */
    public static Timestamp toSqlTimestamp(LocalDateTime localDateTime, TimeZone timeZone) {
        Objects.requireNonNull(localDateTime, "localDateTime must not be null");
        Objects.requireNonNull(timeZone, "timeZone must not be null");

        ZoneId zoneId = timeZone.toZoneId();
        Instant instant = localDateTime.atZone(zoneId).toInstant();
        Timestamp timestamp = Timestamp.from(instant);
        // Timestamp.from() may lose nanosecond precision, so set it explicitly
        timestamp.setNanos(localDateTime.getNano());
        return timestamp;
    }

    private static final BigInteger NANOS_IN_SECOND = BigInteger.valueOf(1_000_000_000L);

    // Max value of epoch second that can be converted to nanosecond without overflow (and fine to add Integer.MAX nanoseconds)
    // Used to avoid BigInteger on small numbers
    private static final long MAX_EPOCH_SECONDS_WITHOUT_OVERFLOW = (Long.MAX_VALUE - Integer.MAX_VALUE) / 1_000_000_000L;

    public static String toUnixTimestampString(long seconds, int nanos) {
        if (seconds <= MAX_EPOCH_SECONDS_WITHOUT_OVERFLOW) {
            return String.valueOf(TimeUnit.SECONDS.toNanos(seconds) + nanos);
        } else {
            return BigInteger.valueOf(seconds).multiply(NANOS_IN_SECOND).add(BigInteger.valueOf(nanos)).toString();
        }
    }

    /**
     * Returns Unix Timestamp in nanoseconds as string
     *
     * @param localTs - LocalDateTime timestamp
     * @param localTz - local timezone (useful to override default)
     * @return String value.
     */
    public static String toUnixTimestampString(LocalDateTime localTs, TimeZone localTz) {
        return toUnixTimestampString(localTs.toEpochSecond(localTz.toZoneId().getRules().getOffset(localTs)), localTs.getNano());
    }

    /**
     * Returns Unix Timestamp in nanoseconds as string
     *
     * @param instant - instant to convert
     * @return String value.
     */
    public static String toUnixTimestampString(Instant instant) {
        return toUnixTimestampString(instant.getEpochSecond(), instant.getNano());
    }
}
