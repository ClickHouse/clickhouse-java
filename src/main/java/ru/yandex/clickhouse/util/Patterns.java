package ru.yandex.clickhouse.util;

import java.util.regex.Pattern;

/**
 * A collection of usable predefined regular expressions.
 */
public final class Patterns {

    private Patterns() {
    }

    public static final Pattern COMMA       = Pattern.compile(",");

    public static final Pattern COLON       = Pattern.compile(":");

    public static final Pattern SEMICOLON   = Pattern.compile(";");

    public static final Pattern DOT         = Pattern.compile("\\.");

    public static final Pattern HYPHEN      = Pattern.compile("-");

    public static final Pattern SLASH       = Pattern.compile("/");

    public static final Pattern TAB         = Pattern.compile("\t");

    public static final Pattern NEWLINE     = Pattern.compile("\n");

    public static final Pattern SPACE       = Pattern.compile(" ");

    public static final Pattern PIPE        = Pattern.compile("\\|");

    public static final Pattern COMMA_SPACE = Pattern.compile(", ");

    public static final Pattern UNDERSCORE  = Pattern.compile("_");

}
