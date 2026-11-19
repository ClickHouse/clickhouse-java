package com.clickhouse.client.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Splits a script into statements as {@code clickhouse-client} does ({@code ClientBase::executeMultiQuery}):
 * <ul>
 *   <li>The {@code -- Tags:} header (the first line and the {@code --} lines after it) is not part of any
 *       statement.</li>
 *   <li>A statement ends at a top-level {@code ;}. A comment on the same line after the {@code ;} belongs to that
 *       statement; this is where test hints are.</li>
 *   <li>The inline data of {@code INSERT ... FORMAT <name>} (other than {@code Values}) ends at an empty line, not
 *       at {@code ;}.</li>
 *   <li>Comment-only text after the last statement is not a statement.</li>
 * </ul>
 */
final class StatementSplitter {

    private StatementSplitter() {
    }

    static List<Statement> split(String text) {
        List<Statement> out = new ArrayList<>();
        int pos = testTagsLength(text);
        boolean echo = false;
        while (true) {
            pos = SqlText.skipWhitespace(text, pos);
            if (pos >= text.length() || !SqlText.hasSignificantToken(text, pos)) {
                return out;
            }
            int begin = pos;
            int end = scanStatementEnd(text, pos);
            int sqlEnd = end;
            // Consume the semicolons after the statement.
            pos = end;
            while (pos < text.length() && text.charAt(pos) == ';') {
                pos++;
                int next = SqlText.skipWhitespaceAndComments(text, pos);
                if (next < text.length() && text.charAt(next) == ';') {
                    pos = next;
                }
            }
            // Add the same-line trailing comment when the next statement starts after the end of this line.
            int newline = text.indexOf('\n', pos);
            if (newline < 0) {
                newline = text.length();
            }
            if (newline <= SqlText.skipWhitespaceAndComments(text, pos)) {
                pos = newline;
            }
            // clickhouse-client sends the statement with its semicolon. Tests that read system.query_log see it.
            String sql = trimRight(text.substring(begin, sqlEnd));
            if (sqlEnd < text.length() && text.charAt(sqlEnd) == ';') {
                sql = text.substring(begin, sqlEnd + 1);
            }
            String full = trimRight(text.substring(begin, pos));
            if (sqlEnd == text.length()) {
                // The last statement has no semicolon: clickhouse-client echoes all the rest of the text, with its
                // trailing newlines.
                full = text.substring(begin);
            }
            TestHint hint = TestHint.parse(full);
            if (hint.echo != null) {
                echo = hint.echo;
            }
            out.add(new Statement(sql, full, hint, echo));
        }
    }

    static String trimRight(String s) {
        int end = s.length();
        while (end > 0) {
            char c = s.charAt(end - 1);
            if (c != ' ' && c != '\t' && c != '\r' && c != '\n') {
                break;
            }
            end--;
        }
        return s.substring(0, end);
    }

    /**
     * Mirrors {@code getTestTagsLength}: the length of the leading {@code -- Tags: ...} line and the {@code --}
     * comment lines right after it.
     */
    static int testTagsLength(String text) {
        int pos = 0;
        boolean first = true;
        while (true) {
            int lineStart = pos;
            while (pos < text.length() && (text.charAt(pos) == ' ' || text.charAt(pos) == '\t')) {
                pos++;
            }
            if (!text.startsWith("--", pos)) {
                return lineStart;
            }
            pos += 2;
            if (first) {
                while (pos < text.length() && (text.charAt(pos) == ' ' || text.charAt(pos) == '\t')) {
                    pos++;
                }
                if (!text.startsWith("Tags:", pos)) {
                    return 0;
                }
                pos += "Tags:".length();
                first = false;
            }
            int eol = -1;
            for (int k = pos; k < text.length(); k++) {
                char c = text.charAt(k);
                if (c == '\r' || c == '\n') {
                    eol = k;
                    break;
                }
            }
            if (eol < 0) {
                return text.length();
            }
            int eolLen = 1;
            if (eol + 1 < text.length() && (text.charAt(eol + 1) == '\r' || text.charAt(eol + 1) == '\n')
                    && text.charAt(eol + 1) != text.charAt(eol)) {
                eolLen = 2;
            }
            pos = eol + eolLen;
        }
    }

    /** Returns the offset of the {@code ;} that ends the statement at start, or the length of the text. */
    static int scanStatementEnd(String text, int start) {
        int parens = 0;
        int i = start;
        while (i < text.length()) {
            char c = text.charAt(i);
            if (c == '\'' || c == '"' || c == '`') {
                i = SqlText.skipQuoted(text, i);
            } else if (c == '-' && text.startsWith("--", i)) {
                i = SqlText.skipLineComment(text, i);
            } else if (c == '#' && (text.startsWith("#!", i) || text.startsWith("# ", i))) {
                i = SqlText.skipLineComment(text, i);
            } else if (c == '/' && text.startsWith("/*", i)) {
                i = SqlText.skipBlockComment(text, i);
            } else if (c == '$') {
                int end = SqlText.heredocEnd(text, i);
                i = end > 0 ? end : i + 1;
            } else if (c == '(' || c == '[') {
                parens++;
                i++;
            } else if (c == ')' || c == ']') {
                parens--;
                i++;
            } else if (c == ';' && parens <= 0) {
                return i;
            } else if (SqlText.isWordChar(c)) {
                int j = i;
                while (j < text.length() && SqlText.isWordChar(text.charAt(j))) {
                    j++;
                }
                if (parens <= 0 && text.regionMatches(true, i, "FORMAT", 0, 6) && j - i == 6) {
                    int dataStart = insertDataStart(text, start, j);
                    if (dataStart >= 0) {
                        // The inline data ends at an empty line.
                        int k = text.indexOf("\n\n", dataStart);
                        return k >= 0 ? k : text.length();
                    }
                }
                i = j;
            } else {
                i++;
            }
        }
        return text.length();
    }

    /**
     * Tells whether the {@code FORMAT} keyword that ends at formatEnd starts the inline data of an INSERT statement
     * that begins at start. Returns the offset of the data after the format name, or -1. The {@code Values} format
     * is excluded: its data ends at {@code ;}, like a query.
     */
    static int insertDataStart(String text, int start, int formatEnd) {
        if (!"INSERT".equals(SqlText.leadingKeyword(text.substring(start, formatEnd)))) {
            return -1;
        }
        int i = formatEnd;
        if (i >= text.length() || !isBlank(text.charAt(i))) {
            return -1;
        }
        while (i < text.length() && isBlank(text.charAt(i))) {
            i++;
        }
        int j = i;
        while (j < text.length() && SqlText.isWordChar(text.charAt(j))) {
            j++;
        }
        String name = text.substring(i, j);
        if (j == i || name.toUpperCase(Locale.ROOT).equals("VALUES") || SqlText.isClauseKeyword(name)) {
            return -1;
        }
        // After a SELECT, the FORMAT clause comes at the end of the SELECT, so after its top-level FROM:
        // `INSERT INTO t SELECT * FROM input('x UInt8') FORMAT TSV`. Before that FROM, `format` is in the select
        // list (`SELECT format x FROM s`, `SELECT format DIV 2 FROM s`).
        if (!formatAfterSelectFrom(text.substring(start, formatEnd - "FORMAT".length()))) {
            return -1;
        }
        return j;
    }

    private static boolean isBlank(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r';
    }

    /**
     * Tells if a FORMAT that follows head can be the FORMAT clause of an INSERT: head has no top-level SELECT, or a
     * top-level FROM follows its last top-level SELECT.
     */
    static boolean formatAfterSelectFrom(String head) {
        boolean afterSelect = false;
        boolean sawFrom = false;
        for (String w : SqlText.topLevelWords(head)) {
            String u = w.toUpperCase(Locale.ROOT);
            if (u.equals("SELECT")) {
                afterSelect = true;
                sawFrom = false;
            } else if (u.equals("FROM")) {
                sawFrom = true;
            }
        }
        return !afterSelect || sawFrom;
    }
}
