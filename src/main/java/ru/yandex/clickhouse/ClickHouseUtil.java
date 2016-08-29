package ru.yandex.clickhouse;


import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;

public class ClickHouseUtil {


    private final static Escaper CLICKHOUSE_ESCAPER = Escapers.builder()
        .addEscape('\\', "\\\\")
        .addEscape('\n', "\\n")
        .addEscape('\t', "\\t")
        .addEscape('\b', "\\b")
        .addEscape('\f', "\\f")
        .addEscape('\r', "\\r")
        .addEscape('\0', "\\0")
        .addEscape('\'', "\\'")
        .addEscape('`', "\\`")
        .build();

    public static String escape(String s) {
        if (s == null) {
            return "NULL";
        }
        return CLICKHOUSE_ESCAPER.escape(s);
    }

    public static String quoteIdentifier(String s) {
        if (s == null) {
            throw new IllegalArgumentException("Can't quote null as identifier");
        }
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('`');
        sb.append(CLICKHOUSE_ESCAPER.escape(s));
        sb.append('`');
        return sb.toString();
    }

}
