package com.clickhouse.client.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Lexical helpers for SQL text: quotes, comments, heredocs and keywords, as the ClickHouse lexer reads them. All
 * the syntax characters are ASCII, so the helpers work on {@code char} positions.
 */
final class SqlText {

    private SqlText() {
    }

    /**
     * SQL words that can follow a column or an alias named {@code format}, as in
     * {@code INSERT INTO t SELECT format FROM s}. No input or output format has such a name, so after FORMAT they
     * show an identifier, not the FORMAT clause.
     */
    private static final Set<String> CLAUSE_KEYWORDS = new HashSet<>(Arrays.asList(
            "FROM", "WHERE", "PREWHERE", "AS", "AND", "OR", "NOT", "IN", "IS", "LIKE", "ILIKE", "BETWEEN",
            "GROUP", "ORDER", "LIMIT", "OFFSET", "HAVING", "QUALIFY", "WINDOW", "UNION", "EXCEPT", "INTERSECT",
            "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "ARRAY", "ON", "USING", "SETTINGS", "INTO",
            "ASC", "DESC", "NULLS", "COLLATE", "WITH", "THEN", "ELSE", "END", "WHEN", "OVER",
            "GLOBAL", "DIV", "MOD", "REGEXP", "FINAL", "SAMPLE", "FILL", "TIES", "BY"));

    static boolean isClauseKeyword(String word) {
        return CLAUSE_KEYWORDS.contains(word.toUpperCase(Locale.ROOT));
    }

    static boolean isWordChar(char c) {
        return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
    }

    static boolean isSpace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == 0x0B;
    }

    /** Tells if a line comment starts at i: {@code --}, {@code #!} or {@code # }. */
    static boolean lineCommentAt(String text, int i) {
        return text.startsWith("--", i) || text.startsWith("#!", i) || text.startsWith("# ", i);
    }

    /** Returns the end of the quoted text that starts at i (after the closing quote). */
    static int skipQuoted(String text, int i) {
        char q = text.charAt(i);
        for (int j = i + 1; j < text.length(); j++) {
            char c = text.charAt(j);
            if (c == '\\') {
                j++;
            } else if (c == q) {
                // A doubled quote is an escaped quote.
                if (j + 1 < text.length() && text.charAt(j + 1) == q) {
                    j++;
                    continue;
                }
                return j + 1;
            }
        }
        return text.length();
    }

    /** Returns the position of the newline that ends the line comment at i, or the end of the text. */
    static int skipLineComment(String text, int i) {
        int nl = text.indexOf('\n', i);
        return nl >= 0 ? nl : text.length();
    }

    /** Returns the end of the block comment at i. Block comments nest in ClickHouse. */
    static int skipBlockComment(String text, int i) {
        int depth = 0;
        int j = i;
        while (j < text.length() - 1) {
            if (text.charAt(j) == '/' && text.charAt(j + 1) == '*') {
                depth++;
                j += 2;
            } else if (text.charAt(j) == '*' && text.charAt(j + 1) == '/') {
                depth--;
                j += 2;
                if (depth == 0) {
                    return j;
                }
            } else {
                j++;
            }
        }
        return text.length();
    }

    /**
     * Returns the end of the {@code $tag$ ... $tag$} heredoc that starts at i, or 0 when the {@code $} at i does
     * not start one.
     */
    static int heredocEnd(String text, int i) {
        int tagEnd = text.indexOf('$', i + 1);
        if (tagEnd < 0) {
            return 0;
        }
        for (int k = i + 1; k < tagEnd; k++) {
            if (!isWordChar(text.charAt(k))) {
                return 0;
            }
        }
        String delim = text.substring(i, tagEnd + 1);
        int end = text.indexOf(delim, tagEnd + 1);
        if (end < 0) {
            return 0;
        }
        return end + delim.length();
    }

    static int skipWhitespace(String text, int i) {
        while (i < text.length() && isSpace(text.charAt(i))) {
            i++;
        }
        return i;
    }

    static int skipWhitespaceAndComments(String text, int i) {
        while (i < text.length()) {
            if (isSpace(text.charAt(i))) {
                i++;
            } else if (lineCommentAt(text, i)) {
                i = skipLineComment(text, i);
            } else if (text.startsWith("/*", i)) {
                i = skipBlockComment(text, i);
            } else {
                return i;
            }
        }
        return i;
    }

    static boolean hasSignificantToken(String text, int from) {
        return skipWhitespaceAndComments(text, from) < text.length();
    }

    /**
     * Returns the first word of a statement in upper case, after comments, whitespace and opening parentheses.
     */
    static String leadingKeyword(String sql) {
        int i = skipWhitespaceAndComments(sql, 0);
        while (i < sql.length() && (sql.charAt(i) == '(' || isSpace(sql.charAt(i)))) {
            i = skipWhitespaceAndComments(sql, i + 1);
        }
        int j = i;
        while (j < sql.length() && isWordChar(sql.charAt(j))) {
            j++;
        }
        return sql.substring(i, j).toUpperCase(Locale.ROOT);
    }

    /** Returns the words of sql outside quotes, heredocs, comments and parentheses. */
    static List<String> topLevelWords(String sql) {
        List<String> words = new ArrayList<>();
        int depth = 0;
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (c == '\'' || c == '"' || c == '`') {
                i = skipQuoted(sql, i);
            } else if (c == '$') {
                int end = heredocEnd(sql, i);
                i = end > 0 ? end : i + 1;
            } else if (lineCommentAt(sql, i)) {
                i = skipLineComment(sql, i);
            } else if (sql.startsWith("/*", i)) {
                i = skipBlockComment(sql, i);
            } else if (c == '(' || c == '[') {
                depth++;
                i++;
            } else if (c == ')' || c == ']') {
                depth--;
                i++;
            } else if (isWordChar(c)) {
                int j = i;
                while (j < sql.length() && isWordChar(sql.charAt(j))) {
                    j++;
                }
                if (depth == 0) {
                    words.add(sql.substring(i, j));
                }
                i = j;
            } else {
                i++;
            }
        }
        return words;
    }

    /**
     * Removes the quotes of a quoted identifier ({@code `a b`} or {@code "a b"}); returns null when s is not one.
     */
    static String unquoteIdentifier(String s) {
        if (s.length() < 2) {
            return null;
        }
        char q = s.charAt(0);
        if (s.charAt(s.length() - 1) != q) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < s.length() - 1; i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length() - 1) {
                i++;
                sb.append(s.charAt(i));
            } else if (c == q && i + 1 < s.length() - 1 && s.charAt(i + 1) == q) {
                i++;
                sb.append(q);
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
