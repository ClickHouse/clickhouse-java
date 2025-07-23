package com.clickhouse.client.api;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static com.clickhouse.client.api.data_formats.internal.BinaryStreamReader.BASES;

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
}
