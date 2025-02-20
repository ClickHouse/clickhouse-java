package com.clickhouse.jdbc;

import java.util.Collection;
import java.util.Collections;
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
                int idx = ClickHouseUtils.skipContentsUntil(originalQuery, i + 2, len, '?', ':');
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
                }
            }
        }

        return partIndex < len ? originalQuery.substring(partIndex, len) : null;
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
