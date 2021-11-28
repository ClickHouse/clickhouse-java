package com.clickhouse.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A parameterized query is a parsed query with parameters being extracted for
 * substitution.
 * <p>
 * Here parameter is define in the format of {@code :<name>[(<type>)]}. It
 * starts with colon, followed by name, and then optionally type within
 * brackets. For example: in query "select :no as no, :name(String) as name",
 * both {@code no} and {@code name} are parameters. Moreover, type of the last
 * parameter is {@code String}.
 */
public class ClickHouseParameterizedQuery implements Serializable {
    private static final long serialVersionUID = 8108887349618342152L;

    /**
     * A part of query.
     */
    protected static class QueryPart implements Serializable {
        protected final String part;
        protected final int paramIndex;
        protected final String paramName;
        protected final ClickHouseColumn paramType;

        protected QueryPart(String part, int paramIndex, String paramName, String paramType) {
            this.part = part;
            this.paramIndex = paramIndex;
            this.paramName = paramName != null ? paramName : String.valueOf(paramIndex);
            // what should be default? ClickHouseAnyValue(simply convert object to string)?
            this.paramType = paramType != null ? ClickHouseColumn.of("", paramType) : null;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + paramIndex;
            result = prime * result + ((paramName == null) ? 0 : paramName.hashCode());
            result = prime * result + ((paramType == null) ? 0 : paramType.hashCode());
            result = prime * result + ((part == null) ? 0 : part.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            QueryPart other = (QueryPart) obj;
            return paramIndex == other.paramIndex && Objects.equals(paramName, other.paramName)
                    && Objects.equals(paramType, other.paramType) && Objects.equals(part, other.part);
        }
    }

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
        if (len < 2 || params == null || params.isEmpty()) {
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

    protected final String originalQuery;

    private final List<QueryPart> parts;
    private final Set<String> names;
    private final String lastPart;

    /**
     * Default constructor.
     *
     * @param query non-blank query
     */
    protected ClickHouseParameterizedQuery(String query) {
        originalQuery = ClickHouseChecker.nonBlank(query, "query");

        parts = new LinkedList<>();
        names = new LinkedHashSet<>();
        lastPart = parse();
    }

    /**
     * Adds part of query and the following parameter.
     *
     * @param part       part of the query, between previous and current parameter
     * @param paramIndex zero-based index of the parameter
     * @param paramType  type of the parameter, could be null
     */
    protected void addPart(String part, int paramIndex, String paramType) {
        addPart(part, paramIndex, null, paramType);
    }

    /**
     * Adds part of query and the following parameter.
     *
     * @param part       part of the query, between previous and current parameter
     * @param paramIndex zero-based index of the parameter
     * @param paramName  name of the parameter, null means
     *                   {@code String.valueOf(paramIndex)}
     * @param paramType  type of the parameter, could be null
     */

    protected void addPart(String part, int paramIndex, String paramName, String paramType) {
        if (paramName == null) {
            paramName = String.valueOf(paramIndex);
        }
        parts.add(new QueryPart(part, paramIndex, paramName, paramType));
        names.add(paramName);
    }

    /**
     * Gets immutable list of query parts.
     *
     * @return immutable list of query parts
     */
    protected List<QueryPart> getParts() {
        return Collections.unmodifiableList(parts);
    }

    /**
     * Parses the query given in constructor.
     *
     * @return remaining part(right after the last parameter) after parsing, could
     *         be null
     */
    protected String parse() {
        int paramIndex = 0;
        int partIndex = 0;
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
                        String part = partIndex != i ? originalQuery.substring(partIndex, i) : "";
                        String paramName = null;
                        String paramType = null;
                        StringBuilder builder = new StringBuilder().append(nextCh);
                        for (i = i + 2; i < len; i++) {
                            char c = originalQuery.charAt(i);
                            if (Character.isJavaIdentifierPart(c)) {
                                builder.append(c);
                            } else {
                                if (c == '(') {
                                    int idx = ClickHouseUtils.skipBrackets(originalQuery, i, len, c);
                                    paramType = originalQuery.substring(i + 1, idx - 1);
                                    i = idx;
                                }
                                break;
                            }
                        }

                        partIndex = i--;

                        if (builder.length() > 0) {
                            paramName = builder.toString();
                            if (names.add(paramName)) {
                                paramIndex++;
                            }
                        }

                        parts.add(new QueryPart(part, paramIndex, paramName, paramType));
                    }
                }
            }
        }

        return partIndex < len ? originalQuery.substring(partIndex, len) : null;
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
        for (QueryPart p : parts) {
            builder.append(p.part);
            builder.append(params.getOrDefault(p.paramName, ClickHouseValues.NULL_EXPR));
        }

        if (lastPart != null) {
            builder.append(lastPart);
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
        for (QueryPart p : parts) {
            builder.append(p.part);
            builder.append(hasMore ? it.next() : ClickHouseValues.NULL_EXPR);
            hasMore = hasMore && it.hasNext();
        }

        if (lastPart != null) {
            builder.append(lastPart);
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
        for (QueryPart p : parts) {
            builder.append(p.part);
            if (index > 0) {
                param = index < len ? more[index - 1] : null;
            }
            builder.append(ClickHouseValues.convertToSqlExpression(param));
            index++;
        }

        if (lastPart != null) {
            builder.append(lastPart);
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
        for (QueryPart p : parts) {
            builder.append(p.part);
            builder.append(
                    index < len ? ClickHouseValues.convertToSqlExpression(values[index]) : ClickHouseValues.NULL_EXPR);
            index++;
        }

        if (lastPart != null) {
            builder.append(lastPart);
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
        for (QueryPart p : parts) {
            builder.append(p.part);
            if (index > 0) {
                param = index < len ? more[index - 1] : ClickHouseValues.NULL_EXPR;
            }
            builder.append(param);
            index++;
        }

        if (lastPart != null) {
            builder.append(lastPart);
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
        for (QueryPart p : parts) {
            builder.append(p.part);
            builder.append(index < len ? values[index] : ClickHouseValues.NULL_EXPR);
            index++;
        }

        if (lastPart != null) {
            builder.append(lastPart);
        }
        return builder.toString();
    }

    /**
     * Gets named parameters.
     *
     * @return list of named parameters
     */
    public List<String> getNamedParameters() {
        return names.isEmpty() ? Collections.emptyList() : Arrays.asList(names.toArray(new String[0]));
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
        for (QueryPart p : parts) {
            queryParts.add(new String[] { p.part, p.paramName });
        }

        if (lastPart != null) {
            queryParts.add(new String[] { lastPart, null });
        }

        return queryParts;
    }

    /**
     * Checks if the query has at least one parameter or not.
     *
     * @return true if there's at least one parameter; false otherwise
     */
    public boolean hasParameter() {
        return !names.isEmpty();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((lastPart == null) ? 0 : lastPart.hashCode());
        result = prime * result + names.hashCode();
        result = prime * result + originalQuery.hashCode();
        result = prime * result + parts.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseParameterizedQuery other = (ClickHouseParameterizedQuery) obj;
        return Objects.equals(lastPart, other.lastPart) && names.equals(other.names)
                && originalQuery.equals(other.originalQuery) && parts.equals(other.parts);
    }
}
