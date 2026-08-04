package com.clickhouse.jdbc;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseParameterizedQuery;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValues;

/**
 * A parameterized query is a parsed query with parameters being extracted for
 * substitution.
 */
@Deprecated
public final class JdbcParameterizedQuery extends ClickHouseParameterizedQuery {
    /**
     * Creates an instance by parsing the given query.
     *
     * @param config non-null configuration
     * @param query  non-empty SQL query
     * @return parameterized query
     */
    public static JdbcParameterizedQuery of(ClickHouseConfig config, String query) {
        // cache if query.length() is greater than 1024?
        return new JdbcParameterizedQuery(config, query);
    }

    private JdbcParameterizedQuery(ClickHouseConfig config, String query) {
        super(config, query);
    }

    @Override
    protected String parse() {
        int paramIndex = 0;
        int partIndex = 0;
        int len = originalQuery.length();
        for (int i = 0; i < len; i++) {
            char ch = originalQuery.charAt(i);
            if (ClickHouseUtils.isQuote(ch)) {
                i = ClickHouseUtils.skipQuotedString(originalQuery, i, len, ch) - 1;
            } else if (ch == '?') {
                int idx = skipUntilTernaryDelimiter(originalQuery, i + 2, len);
                if (idx < len && originalQuery.charAt(idx - 1) == ':' && originalQuery.charAt(idx) != ':'
                        && originalQuery.charAt(idx - 2) != ':') {
                    i = idx - 1;
                } else {
                    addPart(originalQuery.substring(partIndex, i), paramIndex++, null);
                    partIndex = i + 1;
                }
            } else if (ch == ';') {
                throw new IllegalArgumentException(ClickHouseUtils.format(
                        "Multi-statement query cannot be used in prepared statement. Please remove semicolon at %d and everything after it.",
                        i));
            } else if (i + 1 < len) {
                char nextCh = originalQuery.charAt(i + 1);
                if (ch == '-' && nextCh == ch) {
                    i = ClickHouseUtils.skipSingleLineComment(originalQuery, i + 2, len) - 1;
                } else if (ch == '/' && nextCh == '*') {
                    i = ClickHouseUtils.skipMultiLineComment(originalQuery, i + 2, len) - 1;
                } else if (ch == '$') {
                    i = skipHeredoc(originalQuery, i, len) - 1;
                }
            }
        }

        return partIndex < len ? originalQuery.substring(partIndex, len) : null;
    }

    /**
     * Skips quoted strings, brackets, comments and heredocs until seeing a
     * {@code ?} or {@code :}, the delimiters of a ternary operator. Same as
     * {@link ClickHouseUtils#skipContentsUntil(String, int, int, char...)} except
     * that a heredoc is skipped as an opaque token, so that its contents cannot be
     * mistaken for a ternary operator's delimiter.
     *
     * @param query      non-null string to scan
     * @param startIndex start index
     * @param len        end index, usually length of the given string
     * @return index next to the delimiter, or {@code len} when there is none
     */
    private static int skipUntilTernaryDelimiter(String query, int startIndex, int len) {
        for (int i = startIndex; i < len; i++) {
            char ch = query.charAt(i);
            if (ch == '?' || ch == ':') {
                return i + 1;
            } else if (ClickHouseUtils.isQuote(ch)) {
                i = ClickHouseUtils.skipQuotedString(query, i, len, ch) - 1;
            } else if (ClickHouseUtils.isOpenBracket(ch)) {
                i = skipBrackets(query, i, len, ch) - 1;
            } else if (i + 1 < len) {
                char nextCh = query.charAt(i + 1);
                if (ch == '-' && nextCh == ch) {
                    i = ClickHouseUtils.skipSingleLineComment(query, i + 2, len) - 1;
                } else if (ch == '/' && nextCh == '*') {
                    i = ClickHouseUtils.skipMultiLineComment(query, i + 2, len) - 1;
                } else if (ch == '$') {
                    i = skipHeredoc(query, i, len) - 1;
                }
            }
        }

        return len;
    }

    /**
     * Skips brackets and the content inside. Same as
     * {@link ClickHouseUtils#skipBrackets(String, int, int, char)} except that a
     * heredoc is skipped as an opaque token, so that a bracket or quote inside it
     * does not end the enclosing bracket or string.
     *
     * @param query      non-null string to scan
     * @param startIndex start index, optionally index of the opening bracket
     * @param len        end index, usually length of the given string
     * @param bracket    the opening bracket
     * @return index next to the matching close bracket
     * @throws IllegalArgumentException when the bracket is not closed
     */
    private static int skipBrackets(String query, int startIndex, int len, char bracket) {
        char closeBracket = ClickHouseUtils.getCloseBracket(bracket);

        Deque<Character> stack = new ArrayDeque<>();
        for (int i = startIndex + (startIndex < len && query.charAt(startIndex) == bracket ? 1 : 0); i < len; i++) {
            char ch = query.charAt(i);
            if (ClickHouseUtils.isQuote(ch)) {
                i = ClickHouseUtils.skipQuotedString(query, i, len, ch) - 1;
            } else if (ClickHouseUtils.isOpenBracket(ch)) {
                stack.push(closeBracket);
                closeBracket = ClickHouseUtils.getCloseBracket(ch);
            } else if (ch == closeBracket) {
                if (stack.isEmpty()) {
                    return i + 1;
                } else {
                    closeBracket = stack.pop();
                }
            } else if (i + 1 < len) {
                char nextCh = query.charAt(i + 1);
                if (ch == '-' && nextCh == ch) {
                    i = ClickHouseUtils.skipSingleLineComment(query, i + 2, len) - 1;
                } else if (ch == '/' && nextCh == '*') {
                    i = ClickHouseUtils.skipMultiLineComment(query, i + 2, len) - 1;
                } else if (ch == '$') {
                    i = skipHeredoc(query, i, len) - 1;
                }
            }
        }

        throw new IllegalArgumentException(
                ClickHouseUtils.format("Missing '%s' for '%s' at position %d", closeBracket, bracket, startIndex));
    }

    /**
     * Skips a heredoc (dollar quoted string) like {@code $$...$$} or
     * {@code $tag$...$tag$}, where the tag may only contain word characters. When
     * there is no heredoc at {@code startIndex} the dollar sign is treated as an
     * ordinary character, because it is also a valid identifier character: a dollar
     * sign that follows a word character continues an identifier (e.g. {@code a$b}
     * or {@code a$x$}) instead of opening a heredoc, and a dollar sign without a
     * matching closing tag does not open one either.
     *
     * @param query      non-null string to scan
     * @param startIndex index of the dollar sign that may open a heredoc
     * @param len        end index, usually length of the given string
     * @return index next to the closing tag, or {@code startIndex + 1} when there is
     *         no heredoc
     */
    private static int skipHeredoc(String query, int startIndex, int len) {
        if (startIndex > 0 && isWordChar(query.charAt(startIndex - 1))) {
            return startIndex + 1;
        }

        int tagEndIndex = query.indexOf('$', startIndex + 1);
        if (tagEndIndex < 0 || tagEndIndex >= len) {
            return startIndex + 1;
        }

        for (int i = startIndex + 1; i < tagEndIndex; i++) {
            if (!isWordChar(query.charAt(i))) {
                return startIndex + 1;
            }
        }

        String tag = query.substring(startIndex, tagEndIndex + 1);
        int closingTagIndex = query.indexOf(tag, tagEndIndex + 1);
        if (closingTagIndex < 0 || closingTagIndex + tag.length() > len) {
            return startIndex + 1;
        }
        return closingTagIndex + tag.length();
    }

    private static boolean isWordChar(char ch) {
        return ch == '_' || (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z');
    }

    @Override
    public void apply(StringBuilder builder, Collection<String> params) {
        if (!hasParameter()) {
            builder.append(originalQuery);
            return;
        }

        Iterator<String> it = params == null ? Collections.emptyIterator() : params.iterator();
        for (QueryPart p : getParts()) {
            builder.append(p.part);
            builder.append(it.hasNext() ? it.next() : ClickHouseValues.NULL_EXPR);
        }

        appendLastPartIfExists(builder);
    }

    @Override
    public void apply(StringBuilder builder, Object param, Object... more) {
        if (!hasParameter()) {
            builder.append(originalQuery);
            return;
        }

        int len = more == null ? 0 : more.length + 1;
        int index = 0;
        for (QueryPart p : getParts()) {
            builder.append(p.part);
            if (index > 0) {
                param = index < len ? more[index - 1] : null; // NOSONAR
            }
            builder.append(toSqlExpression(p.paramName, param));
            index++;
        }

        appendLastPartIfExists(builder);
    }

    @Override
    public void apply(StringBuilder builder, Object[] values) {
        if (!hasParameter()) {
            builder.append(originalQuery);
            return;
        }

        int len = values == null ? 0 : values.length;
        int index = 0;
        for (QueryPart p : getParts()) {
            builder.append(p.part);
            builder.append(
                    index < len ? toSqlExpression(p.paramName, values[index]) : ClickHouseValues.NULL_EXPR); // NOSONAR
            index++;
        }

        appendLastPartIfExists(builder);
    }

    @Override
    public void apply(StringBuilder builder, String param, String... more) {
        if (!hasParameter()) {
            builder.append(originalQuery);
            return;
        }

        int len = more == null ? 0 : more.length + 1;
        int index = 0;
        for (QueryPart p : getParts()) {
            builder.append(p.part);
            if (index > 0) {
                param = index < len ? more[index - 1] : ClickHouseValues.NULL_EXPR; // NOSONAR
            }
            builder.append(param);
            index++;
        }

        appendLastPartIfExists(builder);
    }

    @Override
    public void apply(StringBuilder builder, String[] values) {
        if (!hasParameter()) {
            builder.append(originalQuery);
            return;
        }

        int len = values == null ? 0 : values.length;
        int index = 0;
        for (QueryPart p : getParts()) {
            builder.append(p.part);
            builder.append(index < len ? values[index] : ClickHouseValues.NULL_EXPR); // NOSONAR
            index++;
        }

        appendLastPartIfExists(builder);
    }
}
