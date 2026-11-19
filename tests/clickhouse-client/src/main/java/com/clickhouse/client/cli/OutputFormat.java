package com.clickhouse.client.cli;

import java.util.Locale;

/**
 * How the shim writes a result set. The Java client reads every result in a binary format (the server prefers the
 * {@code X-ClickHouse-Format} request header to a FORMAT clause), and the shim writes the values itself, so it
 * supports only the {@code TabSeparated} family and {@code Null}.
 */
final class OutputFormat {
    static final OutputFormat DEFAULT = new OutputFormat("TabSeparated", false, false, false, false, true);

    final String name;
    /** A header line with the column names. */
    final boolean names;
    /** A header line with the column types. */
    final boolean types;
    /** Top-level strings and the header lines are not escaped. */
    final boolean raw;
    /** The {@code Null} format: the values are read, but not written. */
    final boolean discard;
    final boolean supported;

    private OutputFormat(String name, boolean names, boolean types, boolean raw, boolean discard, boolean supported) {
        this.name = name;
        this.names = names;
        this.types = types;
        this.raw = raw;
        this.discard = discard;
        this.supported = supported;
    }

    /** Returns the output format of a statement: its top-level FORMAT clause, else TabSeparated. */
    static OutputFormat of(String sql) {
        String name = findFormatName(sql);
        if (name == null) {
            return DEFAULT;
        }
        // Format names are case-insensitive.
        switch (name.toLowerCase(Locale.ROOT)) {
            case "tabseparated":
            case "tsv":
                return new OutputFormat(name, false, false, false, false, true);
            case "tabseparatedwithnames":
            case "tsvwithnames":
                return new OutputFormat(name, true, false, false, false, true);
            case "tabseparatedwithnamesandtypes":
            case "tsvwithnamesandtypes":
                return new OutputFormat(name, true, true, false, false, true);
            case "tabseparatedraw":
            case "tsvraw":
            case "raw":
                return new OutputFormat(name, false, false, true, false, true);
            case "tabseparatedrawwithnames":
            case "tsvrawwithnames":
            case "rawwithnames":
                return new OutputFormat(name, true, false, true, false, true);
            case "tabseparatedrawwithnamesandtypes":
            case "tsvrawwithnamesandtypes":
            case "rawwithnamesandtypes":
                return new OutputFormat(name, true, true, true, false, true);
            case "null":
                return new OutputFormat(name, false, false, false, true, true);
            default:
                return new OutputFormat(name, false, false, false, false, false);
        }
    }

    /** Returns the name of the top-level {@code FORMAT <name>} clause of sql, or null. */
    static String findFormatName(String sql) {
        int depth = 0;
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (c == '\'' || c == '"' || c == '`') {
                i = SqlText.skipQuoted(sql, i);
            } else if (SqlText.lineCommentAt(sql, i)) {
                i = SqlText.skipLineComment(sql, i);
            } else if (sql.startsWith("/*", i)) {
                i = SqlText.skipBlockComment(sql, i);
            } else if (c == '$') {
                int e = SqlText.heredocEnd(sql, i);
                i = e > 0 ? e : i + 1;
            } else if (c == '(' || c == '[') {
                depth++;
                i++;
            } else if (c == ')' || c == ']') {
                depth--;
                i++;
            } else if (SqlText.isWordChar(c)) {
                int j = i;
                while (j < sql.length() && SqlText.isWordChar(sql.charAt(j))) {
                    j++;
                }
                if (depth == 0 && j - i == 6 && sql.regionMatches(true, i, "FORMAT", 0, 6)
                        && (i == 0 || sql.charAt(i - 1) != '.')) {
                    // `ORDER BY format DESC` ends the query too: a clause keyword after FORMAT shows an identifier
                    // named `format`.
                    int[] end = new int[1];
                    String name = formatName(sql, j, end);
                    if (name != null && !SqlText.isClauseKeyword(name) && endsQuery(sql, end[0])) {
                        return name;
                    }
                }
                i = j;
            } else {
                i++;
            }
        }
        return null;
    }

    /** Reads the format name after the FORMAT keyword that ends at j; sets end[0] to the end of the name. */
    private static String formatName(String sql, int j, int[] end) {
        int k = SqlText.skipWhitespaceAndComments(sql, j);
        if (k == j || k >= sql.length()) {
            return null;
        }
        if (sql.charAt(k) == '`' || sql.charAt(k) == '"') {
            // A quoted format name: FORMAT `Null`.
            int m = SqlText.skipQuoted(sql, k);
            end[0] = m;
            return SqlText.unquoteIdentifier(sql.substring(k, m));
        }
        int m = k;
        while (m < sql.length() && SqlText.isWordChar(sql.charAt(m))) {
            m++;
        }
        if (m == k) {
            return null;
        }
        end[0] = m;
        return sql.substring(k, m);
    }

    /**
     * Tells if the text from i on can follow the FORMAT clause of a query: nothing, a semicolon, or a SETTINGS
     * clause. Otherwise FORMAT is an identifier, as in {@code SELECT format FROM t}.
     */
    private static boolean endsQuery(String sql, int i) {
        int k = SqlText.skipWhitespaceAndComments(sql, i);
        if (k >= sql.length() || sql.charAt(k) == ';') {
            return true;
        }
        int m = k;
        while (m < sql.length() && SqlText.isWordChar(sql.charAt(m))) {
            m++;
        }
        return sql.substring(k, m).equalsIgnoreCase("SETTINGS");
    }

    @Override
    public String toString() {
        return name;
    }
}
