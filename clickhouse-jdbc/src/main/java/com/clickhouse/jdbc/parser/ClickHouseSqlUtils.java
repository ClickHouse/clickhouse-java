package com.clickhouse.jdbc.parser;

@Deprecated
public final class ClickHouseSqlUtils {
    public static boolean isQuote(char ch) {
        return ch == '"' || ch == '\'' || ch == '`';
    }

    /**
     * Escape quotes in given string.
     * 
     * @param str   string
     * @param quote quote to escape
     * @return escaped string
     */
    public static String escape(String str, char quote) {
        if (str == null) {
            return str;
        }

        int len = str.length();
        StringBuilder sb = new StringBuilder(len + 10).append(quote);

        for (int i = 0; i < len; i++) {
            char ch = str.charAt(i);
            if (ch == quote || ch == '\\') {
                sb.append('\\');
            }
            sb.append(ch);
        }

        return sb.append(quote).toString();
    }

    /**
     * Unescape quoted string.
     * 
     * @param str quoted string
     * @return unescaped string
     */
    public static String unescape(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }

        int len = str.length();
        char quote = str.charAt(0);
        if (!isQuote(quote) || quote != str.charAt(len - 1)) { // not a quoted string
            return str;
        }

        StringBuilder sb = new StringBuilder(len = len - 1);
        for (int i = 1; i < len; i++) {
            char ch = str.charAt(i);

            if (++i >= len) {
                sb.append(ch);
            } else {
                char nextChar = str.charAt(i);
                if (ch == '\\' || (ch == quote && nextChar == quote)) {
                    sb.append(nextChar);
                } else {
                    sb.append(ch);
                    i--;
                }
            }
        }

        return sb.toString();
    }

    private ClickHouseSqlUtils() {
    }
}
