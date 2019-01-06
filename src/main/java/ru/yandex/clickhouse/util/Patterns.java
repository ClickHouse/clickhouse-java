package ru.yandex.clickhouse.util;

import java.util.regex.Pattern;

/**
 * A collection of usable predefined regular expressions.
 */
public final class Patterns {

    private Patterns() {
    }

    public static final Pattern SEMICOLON   = Pattern.compile(";");

}
