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
     * not taken as a clause. Only a clause that closes the statement is reported: an {@code INSERT} that carries
     * its data after the clause returns {@code null}.</p>
     *
     * @param sqlQuery statement to read, may be null
     * @return format of the trailing FORMAT clause or null
     */
    public static ClickHouseFormat extractTrailingFormat(String sqlQuery) {
        if (sqlQuery == null) {
            return null;
        }

        String previousWord = null;
        String lastWord = null;
        final int len = sqlQuery.length();
        int i = 0;
        while (i < len) {
            final char c = sqlQuery.charAt(i);
            if (Character.isWhitespace(c) || c == ';') {
                i++;
            } else if (c == '-' && i + 1 < len && sqlQuery.charAt(i + 1) == '-') {
                i = skipLineComment(sqlQuery, i);
            } else if (c == '#') {
                i = skipLineComment(sqlQuery, i);
            } else if (c == '/' && i + 1 < len && sqlQuery.charAt(i + 1) == '*') {
                i = skipBlockComment(sqlQuery, i);
            } else if (isWordChar(c)) {
                final int start = i;
                while (i < len && isWordChar(sqlQuery.charAt(i))) {
                    i++;
                }
                previousWord = lastWord;
                lastWord = sqlQuery.substring(start, i);
            } else {
                // a quoted part or any other character ends the word sequence
                i = (c == '\'' || c == '"' || c == '`') ? skipQuoted(sqlQuery, i, c) : i + 1;
                previousWord = lastWord;
                lastWord = null;
            }
        }

        if (lastWord == null || !"FORMAT".equalsIgnoreCase(previousWord)) {
            return null;
        }
        try {
            return ClickHouseFormat.valueOf(lastWord);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

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
