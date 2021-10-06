package com.clickhouse.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A parameterized query is a parsed query with named parameters being extracted
 * for substitution.
 */
public final class ClickHouseParameterizedQuery {
    /**
     * Substitute named parameters in given SQL.
     *
     * @param sql    SQL containing named parameters
     * @param params mapping between parameter name and correspoding SQL expression
     * @return substituted SQL, or the given sql if one of {@code sql} and
     *         {@code params} is null or empty
     */
    public static String apply(String sql, Map<String, String> params) {
        int len = sql == null ? 0 : sql.length();
        if (len < 2 || params == null || params.size() == 0) {
            return sql;
        }

        StringBuilder builder = new StringBuilder(len * 2);
        for (int i = 0; i < len; i++) {
            char ch = sql.charAt(i);
            if (ClickHouseUtils.isQuote(ch)) {
                int endIndex = ClickHouseUtils.skipQuotedString(sql, i, len, ch);
                builder.append(sql.substring(i, endIndex));
                i = endIndex - 1;
            } else if (i + 1 < len) {
                int endIndex = i + 1;
                char nextCh = sql.charAt(endIndex);
                if (ch == '-' && nextCh == ch) {
                    endIndex = ClickHouseUtils.skipSingleLineComment(sql, i + 2, len);
                    builder.append(sql.substring(i, endIndex));
                    i = endIndex - 1;
                } else if (ch == '/' && nextCh == '*') {
                    endIndex = ClickHouseUtils.skipMultiLineComment(sql, i + 2, len);
                    builder.append(sql.substring(i, endIndex));
                    i = endIndex - 1;
                } else if (ch == ':') {
                    if (nextCh == ch) { // skip PostgreSQL-like type conversion
                        builder.append(ch).append(ch);
                        i = i + 1;
                    } else if (Character.isJavaIdentifierStart(nextCh)) {
                        StringBuilder sb = new StringBuilder().append(nextCh);
                        for (i = i + 2; i < len; i++) {
                            char c = sql.charAt(i);
                            if (c == '(') {
                                i = ClickHouseUtils.skipBrackets(sql, i, len, c) - 1;
                                break;
                            } else if (Character.isJavaIdentifierPart(c)) {
                                sb.append(c);
                            } else {
                                i--;
                                break;
                            }
                        }

                        if (sb.length() > 0) {
                            builder.append(params.getOrDefault(sb.toString(), ClickHouseValues.NULL_EXPR));
                        }
                    } else {
                        builder.append(ch);
                    }
                } else {
                    builder.append(ch);
                }
            } else {
                builder.append(ch);
            }
        }

        return builder.toString();
    }

    /**
     * Creates an instance by parsing the given query.
     *
     * @param query non-empty SQL query
     * @return parameterized query
     */
    public static ClickHouseParameterizedQuery of(String query) {
        // cache if query.length() is greater than 1024?
        return new ClickHouseParameterizedQuery(query);
    }

    private final String originalQuery;
    // 0 - from; 1 - to; 2 - parameter index(-1 means no parameter)
    private final List<int[]> parts;
    private int[] lastPart;
    private String[] names;

    private ClickHouseParameterizedQuery(String query) {
        originalQuery = ClickHouseChecker.nonEmpty(query, "query");

        parts = new LinkedList<>();
        lastPart = null;
        names = new String[0];

        parse();
    }

    private void parse() {
        int paramIndex = 0;
        int partIndex = 0;
        Map<String, Integer> params = new LinkedHashMap<>();
        int len = originalQuery.length();
        for (int i = 0; i < len; i++) {
            char ch = originalQuery.charAt(i);
            if (ClickHouseUtils.isQuote(ch)) {
                i = ClickHouseUtils.skipQuotedString(originalQuery, i, len, ch) - 1;
            } else if (i + 1 < len) {
                char nextCh = originalQuery.charAt(i + 1);
                if (ch == '-' && nextCh == ch) {
                    i = ClickHouseUtils.skipSingleLineComment(originalQuery, i + 2, len) - 1;
                } else if (ch == '/' && nextCh == '*') {
                    i = ClickHouseUtils.skipMultiLineComment(originalQuery, i + 2, len) - 1;
                } else if (ch == ':') {
                    if (nextCh == ch) { // skip PostgreSQL-like type conversion
                        i = i + 1;
                    } else if (Character.isJavaIdentifierStart(nextCh)) {
                        int[] part = new int[] { partIndex, i, -1 };
                        parts.add(part);
                        StringBuilder builder = new StringBuilder().append(nextCh);
                        for (i = i + 2; i < len; i++) {
                            char c = originalQuery.charAt(i);
                            if (c == '(') {
                                i = ClickHouseUtils.skipBrackets(originalQuery, i, len, c);
                                String name = builder.toString();
                                builder.setLength(0);
                                Integer existing = params.get(name);
                                if (existing == null) {
                                    part[2] = paramIndex;
                                    params.put(name, paramIndex++);
                                } else {
                                    part[2] = existing.intValue();
                                }
                            } else if (Character.isJavaIdentifierPart(c)) {
                                builder.append(c);
                            } else {
                                break;
                            }
                        }

                        partIndex = i--;

                        if (builder.length() > 0) {
                            String name = builder.toString();
                            Integer existing = params.get(name);
                            if (existing == null) {
                                part[2] = paramIndex;
                                params.put(name, paramIndex++);
                            } else {
                                part[2] = existing.intValue();
                            }
                        }
                    }
                }
            }
        }

        names = new String[paramIndex];
        int index = 0;
        for (String name : params.keySet()) {
            names[index++] = name;
        }

        if (partIndex < len) {
            lastPart = new int[] { partIndex, len, -1 };
        }
    }

    /**
     * Applies stringified parameters to the query.
     *
     * @param params stringified parameters
     * @return substituted query
     */
    public String apply(Map<String, String> params) {
        if (!hasParameter()) {
            return originalQuery;
        }

        if (params == null) {
            params = Collections.emptyMap();
        }

        StringBuilder builder = new StringBuilder();
        for (int[] part : parts) {
            builder.append(originalQuery.substring(part[0], part[1]));
            builder.append(params.getOrDefault(names[part[2]], ClickHouseValues.NULL_EXPR));
        }

        if (lastPart != null) {
            builder.append(originalQuery.substring(lastPart[0], lastPart[1]));
        }
        return builder.toString();
    }

    /**
     * Applies stringified parameters to the query.
     *
     * @param params stringified parameters
     * @return substituted query
     */
    public String apply(Collection<String> params) {
        if (!hasParameter()) {
            return originalQuery;
        }

        StringBuilder builder = new StringBuilder();
        Iterator<String> it = params == null ? null : params.iterator();
        boolean hasMore = it != null && it.hasNext();
        for (int[] part : parts) {
            builder.append(originalQuery.substring(part[0], part[1]));
            builder.append(hasMore ? it.next() : ClickHouseValues.NULL_EXPR);
            hasMore = hasMore && it.hasNext();
        }

        if (lastPart != null) {
            builder.append(originalQuery.substring(lastPart[0], lastPart[1]));
        }
        return builder.toString();
    }

    /**
     * Applies raw parameters to the query.
     * {@link ClickHouseValues#convertToSqlExpression(Object)} will be used to
     * stringify the parameters.
     *
     * @param param raw parameter
     * @param more  more raw parameters if any
     * @return substituted query
     */
    public String apply(Object param, Object... more) {
        if (!hasParameter()) {
            return originalQuery;
        }

        int len = more == null ? 0 : more.length + 1;
        StringBuilder builder = new StringBuilder();
        int index = 0;
        for (int[] part : parts) {
            builder.append(originalQuery.substring(part[0], part[1]));
            if (index > 0) {
                param = index < len ? more[index - 1] : null;
            }
            builder.append(ClickHouseValues.convertToSqlExpression(param));
            index++;
        }

        if (lastPart != null) {
            builder.append(originalQuery.substring(lastPart[0], lastPart[1]));
        }
        return builder.toString();
    }

    /**
     * Applies raw parameters to the query.
     * {@link ClickHouseValues#convertToSqlExpression(Object)} will be used to
     * stringify the parameters.
     *
     * @param values raw parameters
     * @return substituted query
     */
    public String apply(Object[] values) {
        if (!hasParameter()) {
            return originalQuery;
        }

        int len = values == null ? 0 : values.length;
        StringBuilder builder = new StringBuilder();
        int index = 0;
        for (int[] part : parts) {
            builder.append(originalQuery.substring(part[0], part[1]));
            builder.append(
                    index < len ? ClickHouseValues.convertToSqlExpression(values[index]) : ClickHouseValues.NULL_EXPR);
            index++;
        }

        if (lastPart != null) {
            builder.append(originalQuery.substring(lastPart[0], lastPart[1]));
        }
        return builder.toString();
    }

    /**
     * Applies stringified parameters to the query.
     *
     * @param param stringified parameter
     * @param more  more stringified parameters if any
     * @return substituted query
     */
    public String apply(String param, String... more) {
        if (!hasParameter()) {
            return originalQuery;
        }

        int len = more == null ? 0 : more.length + 1;
        StringBuilder builder = new StringBuilder();
        int index = 0;
        for (int[] part : parts) {
            builder.append(originalQuery.substring(part[0], part[1]));
            if (index > 0) {
                param = index < len ? more[index - 1] : ClickHouseValues.NULL_EXPR;
            }
            builder.append(param);
            index++;
        }

        if (lastPart != null) {
            builder.append(originalQuery.substring(lastPart[0], lastPart[1]));
        }
        return builder.toString();
    }

    /**
     * Applies stringified parameters to the query.
     *
     * @param values stringified parameters
     * @return substituted query
     */
    public String apply(String[] values) {
        if (!hasParameter()) {
            return originalQuery;
        }

        int len = values == null ? 0 : values.length;
        StringBuilder builder = new StringBuilder();
        int index = 0;
        for (int[] part : parts) {
            builder.append(originalQuery.substring(part[0], part[1]));
            builder.append(index < len ? values[index] : ClickHouseValues.NULL_EXPR);
            index++;
        }

        if (lastPart != null) {
            builder.append(originalQuery.substring(lastPart[0], lastPart[1]));
        }
        return builder.toString();
    }

    /**
     * Gets named parameters.
     *
     * @return list of named parameters
     */
    public List<String> getNamedParameters() {
        return names.length == 0 ? Collections.emptyList() : Arrays.asList(names);
    }

    /**
     * Gets original query.
     *
     * @return original query
     */
    public String getOriginalQuery() {
        return originalQuery;
    }

    /**
     * Gets query parts. Each part is composed of a snippet taken from
     * {@link #getOriginalQuery()}, followed by a parameter name, which might be
     * null.
     *
     * @return query parts
     */
    public List<String[]> getQueryParts() {
        List<String[]> queryParts = new ArrayList<>(parts.size() + 1);
        for (int[] part : parts) {
            queryParts.add(new String[] { originalQuery.substring(part[0], part[1]), names[part[2]] });
        }

        if (lastPart != null) {
            queryParts.add(new String[] { originalQuery.substring(lastPart[0], lastPart[1]), null });
        }

        return queryParts;
    }

    /**
     * Checks if the query has at least one named parameter or not.
     *
     * @return true if there's at least one named parameter; false otherwise
     */
    public boolean hasParameter() {
        return names.length > 0;
    }
}
