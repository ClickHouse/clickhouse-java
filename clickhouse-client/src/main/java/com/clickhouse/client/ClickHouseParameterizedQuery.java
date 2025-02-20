package com.clickhouse.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;

import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * A parameterized query is a parsed query with parameters being extracted for
 * substitution.
 * <p>
 * Here parameter is define in the format of {@code :<name>[(<type>)]}. It
 * starts with colon, immediately followed by name, and then optionally type
 * within brackets. For example: in query "select :no as no, :name(String) as
 * name", we have two parameters: {@code no} and {@code name}. Moreover, type of
 * the last parameter is {@code String}.
 */
@Deprecated
public class ClickHouseParameterizedQuery implements Serializable {
    private static final long serialVersionUID = 8108887349618342152L;

    /**
     * A part of query.
     */
    protected static class QueryPart implements Serializable {
        public final String part;
        public final int paramIndex;
        public final String paramName;
        public final ClickHouseColumn paramType;

        protected QueryPart(ClickHouseConfig config, String part, int paramIndex, String paramName, String paramType,
                Map<String, ClickHouseValue> map) {
            this.part = part;
            this.paramIndex = paramIndex;
            this.paramName = paramName != null ? paramName : String.valueOf(paramIndex);
            if (paramType != null) {
                this.paramType = ClickHouseColumn.of("", paramType);
                map.put(paramName, this.paramType.newValue(config));
            } else {
                this.paramType = null;
                map.putIfAbsent(paramName, null);
            }
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
     * @param params mapping between parameter name and correspoding SQL
     *               expression(NOT raw value)
     * @return substituted SQL, or the given sql if one of {@code sql} and
     *         {@code params} is null or empty
     */
    public static String apply(String sql, Map<String, String> params) {
        StringBuilder builder = new StringBuilder();
        apply(builder, sql, params);
        return builder.toString();
    }

    /**
     * Substitute named parameters in given SQL.
     *
     * @param builder non-null string builder
     * @param sql     SQL containing named parameters
     * @param params  mapping between parameter name and correspoding SQL
     *                expression(NOT raw value)
     */
    public static void apply(StringBuilder builder, String sql, Map<String, String> params) {
        int len = sql == null ? 0 : sql.length();
        if (len < 2 || params == null || params.isEmpty()) {
            builder.append(sql);
            return;
        }

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

                        builder.append(params.getOrDefault(sb.toString(), ClickHouseValues.NULL_EXPR));
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
    }

    /**
     * Creates an instance by parsing the given query.
     *
     * @param config non-null config
     * @param query  non-empty SQL query
     * @return parameterized query
     */
    public static ClickHouseParameterizedQuery of(ClickHouseConfig config, String query) {
        // cache if query.length() is greater than 1024?
        return new ClickHouseParameterizedQuery(config, query);
    }

    protected final ClickHouseConfig config;
    protected final String originalQuery;

    private final List<QueryPart> parts;
    private final Map<String, ClickHouseValue> names;
    private final String lastPart;

    /**
     * Default constructor.
     *
     * @param config non-null config
     * @param query  non-blank query
     */
    protected ClickHouseParameterizedQuery(ClickHouseConfig config, String query) {
        this.config = ClickHouseChecker.nonNull(config, ClickHouseConfig.TYPE_NAME);
        originalQuery = ClickHouseChecker.nonBlank(query, "query");

        parts = new LinkedList<>();
        names = new LinkedHashMap<>();
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

        parts.add(new QueryPart(config, part, paramIndex, paramName, paramType, names));
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
                            if (!names.containsKey(paramName)) {
                                paramIndex++;
                            }
                        }

                        parts.add(new QueryPart(config, part, paramIndex, paramName, paramType, names));
                    }
                }
            }
        }

        return partIndex < len ? originalQuery.substring(partIndex, len) : null;
    }

    /**
     * Appends last part of the query if it exists.
     *
     * @param builder non-null string builder
     * @return the builder
     */
    protected StringBuilder appendLastPartIfExists(StringBuilder builder) {
        if (lastPart != null) {
            builder.append(lastPart);
        }
        return builder;
    }

    /**
     * Converts given raw value to SQL expression.
     *
     * @param paramName name of the parameter
     * @param value     raw value, could be null
     * @return non-null SQL expression
     */
    protected String toSqlExpression(String paramName, Object value) {
        ClickHouseValue template = names.get(paramName);
        return template != null ? template.update(value).toSqlExpression()
                : ClickHouseValues.convertToSqlExpression(value);
    }

    /**
     * Applies stringified parameters to the given string builder.
     *
     * @param builder non-null string builder
     * @param params  stringified parameters
     */
    public void apply(StringBuilder builder, Map<String, String> params) {
        if (!hasParameter()) {
            builder.append(originalQuery);
            return;
        }

        if (params == null) {
            params = Collections.emptyMap();
        }

        for (QueryPart p : parts) {
            builder.append(p.part);
            builder.append(params.getOrDefault(p.paramName, ClickHouseValues.NULL_EXPR));
        }

        appendLastPartIfExists(builder);
    }

    /**
     * Applies stringified parameters to the given string builder.
     *
     * @param builder non-null string builder
     * @param params  stringified parameters
     */
    public void apply(StringBuilder builder, Collection<String> params) {
        if (params == null || params.isEmpty()) {
            apply(builder, Collections.emptyMap());
            return;
        }

        Map<String, String> map = null;
        Iterator<String> it = params.iterator();
        if (it.hasNext()) {
            map = new HashMap<>();
            for (String n : names.keySet()) {
                String v = it.next();
                if (v != null) {
                    map.put(n, v);
                }
                if (!it.hasNext()) {
                    break;
                }
            }
        }
        apply(builder, map);
    }

    /**
     * Applies raw parameters to the given string builder.
     * {@link ClickHouseValues#convertToSqlExpression(Object)} will be used to
     * stringify the parameters.
     *
     * @param builder non-null string builder
     * @param param   raw parameter
     * @param more    more raw parameters if any
     */
    public void apply(StringBuilder builder, Object param, Object... more) {
        if (!hasParameter()) {
            builder.append(originalQuery);
            return;
        }

        int len = more == null ? 0 : more.length;
        Map<String, String> map = new HashMap<>();
        int index = -1;
        for (Entry<String, ClickHouseValue> e : names.entrySet()) {
            ClickHouseValue v = e.getValue();
            if (index < 0) {
                map.put(e.getKey(),
                        v != null ? v.update(param).toSqlExpression() : ClickHouseValues.convertToSqlExpression(param));
            } else if (index < len) {
                map.put(e.getKey(), v != null ? v.update(more[index]).toSqlExpression() // NOSONAR
                        : ClickHouseValues.convertToSqlExpression(more[index])); // NOSONAR
            } else {
                break;
            }
            index++;
        }

        apply(builder, map);
    }

    /**
     * Applies raw parameters to the given string builder.
     * {@link ClickHouseValues#convertToSqlExpression(Object)} will be used to
     * stringify the parameters.
     *
     * @param builder non-null string builder
     * @param values  raw parameters
     */
    public void apply(StringBuilder builder, Object[] values) {
        int len = values == null ? 0 : values.length;
        if (len == 0) {
            apply(builder, Collections.emptyMap());
            return;
        }

        Map<String, String> map = new HashMap<>();
        int index = 0;
        for (Entry<String, ClickHouseValue> e : names.entrySet()) {
            ClickHouseValue v = e.getValue();
            if (index < len) {
                map.put(e.getKey(), v != null ? v.update(values[index]).toSqlExpression()
                        : ClickHouseValues.convertToSqlExpression(values[index]));
            } else {
                break;
            }
            index++;
        }

        apply(builder, map);
    }

    /**
     * Applies stringified parameters to the given string builder.
     *
     * @param builder non-null string builder
     * @param param   stringified parameter
     * @param more    more stringified parameters if any
     */
    public void apply(StringBuilder builder, String param, String... more) {
        if (!hasParameter()) {
            builder.append(originalQuery);
            return;
        }

        int len = more == null ? 0 : more.length;
        Map<String, String> map = new HashMap<>();
        int index = -1;
        for (String n : names.keySet()) {
            if (index < 0) {
                map.put(n, param);
            } else if (index < len) {
                map.put(n, more[index]); // NOSONAR
            } else {
                break;
            }
            index++;
        }

        apply(builder, map);
    }

    /**
     * Applies stringified parameters to the given string builder.
     *
     * @param builder non-null string builder
     * @param values  stringified parameters
     */
    public void apply(StringBuilder builder, String[] values) {
        int len = values == null ? 0 : values.length;
        if (len == 0) {
            apply(builder, Collections.emptyMap());
            return;
        }

        Map<String, String> map = new HashMap<>();
        int index = 0;
        for (String n : names.keySet()) {
            if (index < len) {
                map.put(n, values[index]);
            } else {
                break;
            }
            index++;
        }

        apply(builder, map);
    }

    /**
     * Gets named parameters.
     *
     * @return list of named parameters
     */
    public List<String> getParameters() {
        if (names.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> list = new ArrayList<>(names.size());
        for (String n : names.keySet()) {
            list.add(n);
        }
        return Collections.unmodifiableList(list);
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
     * Gets parameter templates for converting value to SQL expression.
     *
     * @return parameter templates
     */
    public ClickHouseValue[] getParameterTemplates() {
        int i = 0;
        ClickHouseValue[] templates = new ClickHouseValue[names.size()];
        for (ClickHouseValue v : names.values()) {
            templates[i++] = v;
        }
        return templates;
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
