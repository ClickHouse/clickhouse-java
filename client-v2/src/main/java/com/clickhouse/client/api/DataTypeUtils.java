package com.clickhouse.client.api;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Objects;

import com.clickhouse.data.ClickHouseDataType;

public class DataTypeUtils {

    /**
     * Formatter for the DateTime type.
     */
    public static DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Formatter for the Date type.
     */
    public static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * Formatter for the DateTime type with nanoseconds.
     */
    public static DateTimeFormatter DATETIME_WITH_NANOS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnn");

    /**
     * Formats a Java object for use in SQL statements or as query parameter.
     *
     * Note, that this method returns the empty {@link java.lang.String}
     * &quot;&quot; for {@code null} objects.
     *
     * @param object
     *            the Java object to format
     * @return a suitable String representation of {@code object}, or the empty
     *         String for {@code null} objects
     */
    public static String format(Object object) {
        return format(object, null);
    }

    /**
     * Formats a Java object for use in SQL statements or as query parameter.
     *
     * This method uses the {@code dataTypeHint} parameter to find be best
     * suitable format for the object.
     *
     * Note, that this method returns the empty {@link java.lang.String}
     * &quot;&quot; for {@code null} objects. This might not be the correct
     * default value for the {@code dataTypeHint}
     *
     * @param object
     *            the Java object to format
     * @param dataTypeHint
     *            the ClickHouse data type {@code object} should be used for
     * @return a suitable String representation of {@code object}, or the empty
     *         String for {@code null} objects
     */
    public static String format(Object object, ClickHouseDataType dataTypeHint) {
        return format(object, dataTypeHint, null);
    }

    /**
     * Formats a Java object for use in SQL statements or as query parameter.
     *
     * This method uses the {@code dataTypeHint} parameter to find be best
     * suitable format for the object.
     *
     * For <em>some</em> formatting operations, providing a {@code timeZone} is
     * mandatory: When formatting time-zone-based values (e.g.
     * {@link java.time.OffsetDateTime}, {@link java.time.ZonedDateTime}, etc.)
     * for use as ClickHouse data types which are not time-zone-based, e.g.
     * {@link ClickHouseDataType#Date}, or vice-versa when formatting
     * non-time-zone-based Java objects (e.g. {@link java.time.LocalDateTime} or
     * any {@link java.util.Calendar} based objects without time-zone) for use
     * as time-zone-based ClickHouse data types, e.g.
     * {@link ClickHouseDataType#DateTime64}. Although the ClickHouse server
     * might understand simple wall-time Strings (&quot;2025-08-20 13:37:42&quot;)
     * even for those data types, it is preferable to use timestamp values.
     *
     * Note, that this method returns the empty {@link java.lang.String}
     * &quot;&quot; for {@code null} objects. This might not be the correct
     * default value for {@code dataTypeHint}.
     *
     * @param object
     *            the Java object to format
     * @param dataTypeHint
     *            the ClickHouse data type {@code object} should be used for
     * @param timeZone
     *            the time zone to be used when formatting time-zone-based Java
     *            objects for use in non-time-zone-based ClickHouse data types
     *            and vice versa
     * @return a suitable String representation of {@code object}, or the empty
     *         String for {@code null} objects
     */
    public static String format(Object object, ClickHouseDataType dataTypeHint,
        ZoneId timeZone)
    {
        if (object == null) {
            return "";
        }
        if (object instanceof Instant) {
            return formatInstant((Instant) object, dataTypeHint, timeZone);
        }
        return String.valueOf(object);
    }

    private static String formatInstant(Instant instant, ClickHouseDataType dataTypeHint,
        ZoneId timeZone)
    {
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
        String nanos = String.valueOf(instant.getNano());
        char[] n = new char[9];
        Arrays.fill(n, '0');
        int nanosLength = Math.min(9, nanos.length());
        nanos.getChars(0, nanosLength, n, 9 - nanosLength);
        return String.valueOf(instant.getEpochSecond()) + "." + new String(n);
    }

}
