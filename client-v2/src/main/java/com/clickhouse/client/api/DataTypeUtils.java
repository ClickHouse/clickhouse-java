package com.clickhouse.client.api;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.data.ClickHouseDataType;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.Objects;
import java.util.TimeZone;

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

    /**
     * Converts a {@link java.sql.Date} to {@link LocalDate} using the specified Calendar's timezone.
     *
     * <p>The Calendar parameter specifies the timezone context in which to interpret the
     * Date's internal epoch milliseconds. This is important because java.sql.Date stores
     * milliseconds since epoch, and interpreting those millis in different timezones
     * can result in different calendar dates (the "day shift" problem).</p>
     *
     * <p>Example: A Date with millis representing "2024-01-15 00:00:00 UTC" would be
     * interpreted as "2024-01-14" in America/New_York (UTC-5) if not handled correctly.</p>
     *
     * <p>For default JVM timezone behavior, use {@link Date#toLocalDate()} directly.</p>
     *
     * @param sqlDate the java.sql.Date to convert
     * @param calendar the Calendar specifying the timezone context
     * @return the LocalDate representing the date in the specified timezone
     * @throws NullPointerException if sqlDate or calendar is null
     */
    public static LocalDate toLocalDate(Date sqlDate, Calendar calendar) {
        Objects.requireNonNull(sqlDate, "sqlDate must not be null");
        Objects.requireNonNull(calendar, "calendar must not be null");

        // Clone the calendar to avoid modifying the original
        Calendar cal = (Calendar) calendar.clone();
        cal.setTimeInMillis(sqlDate.getTime());

        return LocalDate.of(
                cal.get(Calendar.YEAR),
                cal.get(Calendar.MONTH) + 1,  // Calendar months are 0-based
                cal.get(Calendar.DAY_OF_MONTH)
        );
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
     * Converts a {@link java.sql.Time} to {@link LocalTime} using the specified Calendar's timezone.
     *
     * <p>The Calendar parameter specifies the timezone context in which to interpret the
     * Time's internal epoch milliseconds. java.sql.Time stores the time as millis since
     * epoch on January 1, 1970, so timezone affects which hour/minute/second is extracted.</p>
     *
     * <p>For default JVM timezone behavior, use {@link Time#toLocalTime()} directly.</p>
     *
     * @param sqlTime the java.sql.Time to convert
     * @param calendar the Calendar specifying the timezone context
     * @return the LocalTime representing the time in the specified timezone
     * @throws NullPointerException if sqlTime or calendar is null
     */
    public static LocalTime toLocalTime(Time sqlTime, Calendar calendar) {
        Objects.requireNonNull(sqlTime, "sqlTime must not be null");
        Objects.requireNonNull(calendar, "calendar must not be null");

        // Clone the calendar to avoid modifying the original
        Calendar cal = (Calendar) calendar.clone();
        cal.setTimeInMillis(sqlTime.getTime());

        return LocalTime.of(
                cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE),
                cal.get(Calendar.SECOND),
                // Calendar doesn't store nanos, but millis - convert to nanos
                cal.get(Calendar.MILLISECOND) * 1_000_000
        );
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
     * Converts a {@link java.sql.Timestamp} to {@link LocalDateTime} using the specified Calendar's timezone.
     *
     * <p>The Calendar parameter specifies the timezone context in which to interpret the
     * Timestamp's internal epoch milliseconds. This is crucial for correct date and time
     * extraction when the application and database are in different timezones.</p>
     *
     * <p>Note: This method preserves nanosecond precision from the Timestamp.</p>
     *
     * <p>For default JVM timezone behavior, use {@link Timestamp#toLocalDateTime()} directly.</p>
     *
     * @param sqlTimestamp the java.sql.Timestamp to convert
     * @param calendar the Calendar specifying the timezone context
     * @return the LocalDateTime representing the timestamp in the specified timezone
     * @throws NullPointerException if sqlTimestamp or calendar is null
     */
    public static LocalDateTime toLocalDateTime(Timestamp sqlTimestamp, Calendar calendar) {
        Objects.requireNonNull(sqlTimestamp, "sqlTimestamp must not be null");
        Objects.requireNonNull(calendar, "calendar must not be null");

        // Clone the calendar to avoid modifying the original
        Calendar cal = (Calendar) calendar.clone();
        cal.setTimeInMillis(sqlTimestamp.getTime());

        // Preserve nanoseconds from Timestamp (Calendar only has millisecond precision)
        int nanos = sqlTimestamp.getNanos();

        return LocalDateTime.of(
                cal.get(Calendar.YEAR),
                cal.get(Calendar.MONTH) + 1,  // Calendar months are 0-based
                cal.get(Calendar.DAY_OF_MONTH),
                cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE),
                cal.get(Calendar.SECOND),
                nanos
        );
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

    // ==================== LocalDate/LocalTime/LocalDateTime to SQL types ====================

    /**
     * Converts a {@link LocalDate} to {@link java.sql.Date} using the specified Calendar's timezone.
     *
     * <p>The Calendar parameter specifies the timezone context in which to interpret the
     * LocalDate when calculating the epoch milliseconds for the resulting java.sql.Date.
     * The resulting Date will represent midnight on the specified date in the Calendar's timezone.</p>
     *
     * <p>For default JVM timezone behavior, use {@link Date#valueOf(LocalDate)} directly.</p>
     *
     * @param localDate the LocalDate to convert
     * @param calendar the Calendar specifying the timezone context
     * @return the java.sql.Date representing midnight on the specified date in the given timezone
     * @throws NullPointerException if localDate or calendar is null
     */
    public static Date toSqlDate(LocalDate localDate, Calendar calendar) {
        Objects.requireNonNull(localDate, "localDate must not be null");
        Objects.requireNonNull(calendar, "calendar must not be null");

        // Clone the calendar to avoid modifying the original
        Calendar cal = (Calendar) calendar.clone();
        cal.clear();
        cal.set(localDate.getYear(), localDate.getMonthValue() - 1, localDate.getDayOfMonth(), 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return new Date(cal.getTimeInMillis());
    }

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

        ZoneId zoneId = timeZone.toZoneId();
        long epochMillis = localDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
        return new Date(epochMillis);
    }

    /**
     * Converts a {@link LocalTime} to {@link java.sql.Time} using the specified Calendar's timezone.
     *
     * <p>The Calendar parameter specifies the timezone context in which to interpret the
     * LocalTime when calculating the epoch milliseconds for the resulting java.sql.Time.
     * The resulting Time will represent the specified time on January 1, 1970 in the Calendar's timezone.</p>
     *
     * <p>For default JVM timezone behavior, use {@link Time#valueOf(LocalTime)} directly.</p>
     *
     * @param localTime the LocalTime to convert
     * @param calendar the Calendar specifying the timezone context
     * @return the java.sql.Time representing the specified time
     * @throws NullPointerException if localTime or calendar is null
     */
    public static Time toSqlTime(LocalTime localTime, Calendar calendar) {
        Objects.requireNonNull(localTime, "localTime must not be null");
        Objects.requireNonNull(calendar, "calendar must not be null");

        // Clone the calendar to avoid modifying the original
        Calendar cal = (Calendar) calendar.clone();
        cal.clear();
        // java.sql.Time is based on January 1, 1970
        cal.set(1970, Calendar.JANUARY, 1,
                localTime.getHour(), localTime.getMinute(), localTime.getSecond());
        cal.set(Calendar.MILLISECOND, localTime.getNano() / 1_000_000);

        return new Time(cal.getTimeInMillis());
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
     * Converts a {@link LocalDateTime} to {@link java.sql.Timestamp} using the specified Calendar's timezone.
     *
     * <p>The Calendar parameter specifies the timezone context in which to interpret the
     * LocalDateTime when calculating the epoch milliseconds for the resulting java.sql.Timestamp.</p>
     *
     * <p>Note: This method preserves nanosecond precision from the LocalDateTime.</p>
     *
     * <p>For default JVM timezone behavior, use {@link Timestamp#valueOf(LocalDateTime)} directly.</p>
     *
     * @param localDateTime the LocalDateTime to convert
     * @param calendar the Calendar specifying the timezone context
     * @return the java.sql.Timestamp representing the specified date and time
     * @throws NullPointerException if localDateTime or calendar is null
     */
    public static Timestamp toSqlTimestamp(LocalDateTime localDateTime, Calendar calendar) {
        Objects.requireNonNull(localDateTime, "localDateTime must not be null");
        Objects.requireNonNull(calendar, "calendar must not be null");

        // Clone the calendar to avoid modifying the original
        Calendar cal = (Calendar) calendar.clone();
        cal.clear();
        cal.set(localDateTime.getYear(), localDateTime.getMonthValue() - 1, localDateTime.getDayOfMonth(),
                localDateTime.getHour(), localDateTime.getMinute(), localDateTime.getSecond());
        cal.set(Calendar.MILLISECOND, 0);  // We'll set nanos separately

        Timestamp timestamp = new Timestamp(cal.getTimeInMillis());
        timestamp.setNanos(localDateTime.getNano());
        return timestamp;
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
}
