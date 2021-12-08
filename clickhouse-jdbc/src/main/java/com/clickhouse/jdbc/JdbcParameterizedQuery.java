package com.clickhouse.jdbc;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import com.clickhouse.client.ClickHouseParameterizedQuery;
import com.clickhouse.client.ClickHouseUtils;

/**
 * A parameterized query is a parsed query with parameters being extracted for
 * substitution.
 */
public final class JdbcParameterizedQuery extends ClickHouseParameterizedQuery {
    /**
     * Creates an instance by parsing the given query.
     *
     * @param query non-empty SQL query
     * @return parameterized query
     */
    public static JdbcParameterizedQuery of(String query) {
        // cache if query.length() is greater than 1024?
        return new JdbcParameterizedQuery(query);
    }

    private JdbcParameterizedQuery(String query) {
        super(query);
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
}
