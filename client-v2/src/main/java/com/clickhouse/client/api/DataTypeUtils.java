package com.clickhouse.client.api;

import java.time.format.DateTimeFormatter;

public class DataTypeUtils {

    /**
     * Formatter for the DateTime type.
     */
    public static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Formatter for the Date type.
     */
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * Formatter for the DateTime type with nanoseconds.
     */
    public static final DateTimeFormatter DATETIME_WITH_NANOS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnn");

}
