package com.clickhouse.client.api.internal;

import com.clickhouse.data.ClickHouseFormat;
import org.slf4j.Logger;

import java.io.Closeable;

/**
 * Class containing utility methods used across the client.
 */
public final class ClientUtils {

    private ClientUtils() {}

    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    public static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    /**
     * <p>Returns the format of a trailing {@code FORMAT <name>} clause of a statement, or {@code null} when the
     * statement has no such clause or names a format this client does not know.</p>
     *
     * <p>String literals, quoted identifiers and comments are skipped, so a {@code FORMAT} written inside them is
     * not taken as a clause. Only a clause that closes the statement is reported - a {@code SETTINGS} clause may
     * follow it - so an {@code INSERT} that carries its data after the clause returns {@code null}.</p>
     *
     * @param sqlQuery statement to read, may be null
     * @return format of the trailing FORMAT clause or null
     */
    public static ClickHouseFormat extractTrailingFormat(String sqlQuery) {
        if (sqlQuery == null) {
            return null;
        }

        // state of reading a FORMAT clause: no clause seen, the name of the format is expected, the name was read,
        // or a SETTINGS clause follows the name and closes the statement
        int state = NO_CLAUSE;
        String formatName = null;
        final int len = sqlQuery.length();
        int i = 0;
        while (i < len) {
            final char c = sqlQuery.charAt(i);
            if (Character.isWhitespace(c) || c == ';') {
                i++;
                continue;
            }
            if ((c == '-' && i + 1 < len && sqlQuery.charAt(i + 1) == '-') || c == '#') {
                i = skipLineComment(sqlQuery, i);
                continue;
            }
            if (c == '/' && i + 1 < len && sqlQuery.charAt(i + 1) == '*') {
                i = skipBlockComment(sqlQuery, i);
                continue;
            }
            if (isWordChar(c)) {
                final int start = i;
                while (i < len && isWordChar(sqlQuery.charAt(i))) {
                    i++;
                }
                final String word = sqlQuery.substring(start, i);
                if (state == EXPECT_NAME) {
                    formatName = word;
                    state = NAME_READ;
                } else if (state == NAME_READ) {
                    // only a SETTINGS clause may close a statement after the format name
                    state = "SETTINGS".equalsIgnoreCase(word) ? IN_SETTINGS : NO_CLAUSE;
                } else if (state != IN_SETTINGS && "FORMAT".equalsIgnoreCase(word)) {
                    state = EXPECT_NAME;
                }
                continue;
            }
            // a quoted part or any other character cannot be part of a FORMAT clause
            i = (c == '\'' || c == '"' || c == '`') ? skipQuoted(sqlQuery, i, c) : i + 1;
            if (state == EXPECT_NAME || state == NAME_READ) {
                state = NO_CLAUSE;
            }
        }

        if (formatName == null || (state != NAME_READ && state != IN_SETTINGS)) {
            return null;
        }
        try {
            return ClickHouseFormat.valueOf(formatName);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static final int NO_CLAUSE = 0;
    private static final int EXPECT_NAME = 1;
    private static final int NAME_READ = 2;
    private static final int IN_SETTINGS = 3;

    private static boolean isWordChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$';
    }

    private static int skipLineComment(String str, int i) {
        while (i < str.length() && str.charAt(i) != '\n') {
            i++;
        }
        return i;
    }

    private static int skipBlockComment(String str, int i) {
        i += 2;
        while (i + 1 < str.length() && !(str.charAt(i) == '*' && str.charAt(i + 1) == '/')) {
            i++;
        }
        return Math.min(str.length(), i + 2);
    }

    private static int skipQuoted(String str, int i, char quote) {
        i++; // opening quote
        while (i < str.length()) {
            final char c = str.charAt(i);
            if (c == '\\') {
                i += 2;
            } else if (c == quote) {
                if (i + 1 < str.length() && str.charAt(i + 1) == quote) {
                    i += 2; // doubled quote is an escaped one
                } else {
                    return i + 1;
                }
            } else {
                i++;
            }
        }
        return i;
    }

    public static void quietClose(Closeable closeable, Logger log) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                log.warn("Failed to close object " + closeable, e);
            }
        }
    }
}
