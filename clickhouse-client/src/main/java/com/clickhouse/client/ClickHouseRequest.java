package com.clickhouse.client;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
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
import java.util.concurrent.CompletableFuture;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseConfigOption;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.client.exception.ClickHouseException;

/**
 * Request object holding references to {@link ClickHouseClient},
 * {@link ClickHouseNode}, format, sql, options and settings etc. for execution.
 */
@SuppressWarnings("squid:S119")
public class ClickHouseRequest<SelfT extends ClickHouseRequest<SelfT>> implements Serializable {
    private static final String TYPE_EXTERNAL_TABLE = "ExternalTable";

    /**
     * Mutation request.
     */
    public static class Mutation extends ClickHouseRequest<Mutation> {
        protected Mutation(ClickHouseRequest<?> request, boolean sealed) {
            super(request.getClient(), request.server, sealed);

            this.options.putAll(request.options);
            this.settings.putAll(request.settings);

            this.sessionId = request.sessionId;
        }

        @Override
        protected String getQuery() {
            if (input != null && sql != null) {
                return new StringBuilder().append(sql).append(" FORMAT ").append(getFormat().name()).toString();
            }

            return super.getQuery();
        }

        @Override
        public Mutation format(ClickHouseFormat format) {
            if (!ClickHouseChecker.nonNull(format, "format").supportsInput()) {
                throw new IllegalArgumentException("Only input format is allowed for mutation.");
            }

            return super.format(format);
        }

        /**
         * Loads data from given file which may or may not be compressed.
         *
         * @param file absolute or relative path of the file, file extension will be
         *             used to determine if it's compressed or not
         * @return mutation request
         */
        public Mutation data(String file) {
            return data(file, ClickHouseCompression.fromFileName(file));
        }

        /**
         * Loads compressed data from given file.
         *
         * @param file        absolute or relative path of the file
         * @param compression compression algorithm, {@link ClickHouseCompression#NONE}
         *                    means no compression
         * @return mutation request
         */
        @SuppressWarnings("squid:S2095")
        public Mutation data(String file, ClickHouseCompression compression) {
            checkSealed();

            FileInputStream fileInput = null;

            try {
                fileInput = new FileInputStream(file);
            } catch (FileNotFoundException e) {
                throw new IllegalArgumentException(e);
            }

            if (compression != null && compression != ClickHouseCompression.NONE) {
                // TODO create input stream
            } else {
                this.input = fileInput;
            }

            return this;
        }

        /**
         * Loads data from input stream.
         *
         * @param input input stream
         * @return mutation requets
         */
        public Mutation data(InputStream input) {
            checkSealed();

            this.input = input;

            return this;
        }

        /**
         * Sends mutation requets for execution. Same as
         * {@code client.execute(request.seal())}.
         *
         * @return future to get response
         * @throws ClickHouseException when error occurred
         */
        public CompletableFuture<ClickHouseResponse> send() throws ClickHouseException {
            return getClient().execute(isSealed() ? this : seal());
        }

        @Override
        public Mutation table(String table, String queryId) {
            checkSealed();

            this.queryId = queryId;

            String sql = "INSERT INTO " + ClickHouseChecker.nonBlank(table, "table");
            if (!sql.equals(this.sql)) {
                this.sql = sql;
                this.preparedQuery = null;
                resetCache();
            }

            return this;
        }

        @Override
        public Mutation seal() {
            Mutation req = this;

            if (!isSealed()) {
                // no idea which node we'll connect to until now
                req = new Mutation(this, true);
                req.externalTables.addAll(externalTables);
                req.options.putAll(options);
                req.settings.putAll(settings);

                req.namedParameters.putAll(namedParameters);

                req.input = input;
                req.queryId = queryId;
                req.sessionId = sessionId;
                req.sql = sql;

                req.preparedQuery = preparedQuery;
            }

            return req;
        }
    }

    private static final long serialVersionUID = 4990313525960702287L;

    private final boolean sealed;

    private transient ClickHouseClient client;

    protected final ClickHouseConfig clientConfig;
    protected final Function<ClickHouseNodeSelector, ClickHouseNode> server;
    protected final transient List<ClickHouseExternalTable> externalTables;
    protected final Map<ClickHouseConfigOption, Serializable> options;
    protected final Map<String, Serializable> settings;

    protected final Map<String, String> namedParameters;

    protected transient InputStream input;
    protected String queryId;
    protected String sessionId;
    protected String sql;
    protected ClickHouseParameterizedQuery preparedQuery;

    // cache
    protected transient ClickHouseConfig config;
    protected transient List<String> statements;

    @SuppressWarnings("unchecked")
    protected ClickHouseRequest(ClickHouseClient client, Function<ClickHouseNodeSelector, ClickHouseNode> server,
            boolean sealed) {
        if (client == null || server == null) {
            throw new IllegalArgumentException("Non-null client and server are required");
        }

        this.client = client;
        this.clientConfig = client.getConfig();
        this.server = (Function<ClickHouseNodeSelector, ClickHouseNode> & Serializable) server::apply;
        this.sealed = sealed;

        this.externalTables = new LinkedList<>();
        this.options = new HashMap<>();
        this.settings = new LinkedHashMap<>();

        this.namedParameters = new HashMap<>();
    }

    protected void checkSealed() {
        if (sealed) {
            throw new IllegalStateException("Sealed request is immutable");
        }
    }

    protected ClickHouseClient getClient() {
        if (client == null) {
            client = ClickHouseClient.builder().nodeSelector(clientConfig.getNodeSelector()).build();
        }

        return client;
    }

    protected ClickHouseParameterizedQuery getPreparedQuery() {
        if (preparedQuery == null) {
            preparedQuery = ClickHouseParameterizedQuery.of(getQuery());
        }

        return preparedQuery;
    }

    /**
     * Gets query, either set by {@code query()} or {@code table()}.
     *
     * @return sql query
     */
    protected String getQuery() {
        return this.sql;
    }

    protected void resetCache() {
        if (config != null) {
            config = null;
        }

        if (statements != null) {
            statements = null;
        }
    }

    /**
     * Creates a copy of this request object.
     *
     * @return copy of this request
     */
    public ClickHouseRequest<SelfT> copy() {
        ClickHouseRequest<SelfT> req = new ClickHouseRequest<>(getClient(), server, false);
        req.externalTables.addAll(externalTables);
        req.options.putAll(options);
        req.settings.putAll(settings);
        req.namedParameters.putAll(namedParameters);
        req.input = input;
        req.queryId = queryId;
        req.sessionId = sessionId;
        req.sql = sql;
        req.preparedQuery = preparedQuery;
        return req;
    }

    /**
     * Checks if the request is sealed(immutable).
     *
     * @return true if the request is sealed; false otherwise
     */
    public boolean isSealed() {
        return this.sealed;
    }

    /**
     * Checks if the request contains any input stream.
     *
     * @return true if there's input stream; false otherwise
     */
    public boolean hasInputStream() {
        return this.input != null || !this.externalTables.isEmpty();
    }

    /**
     * Depending on the {@link java.util.function.Function} passed to the
     * constructor, this method may return different node for each call.
     * 
     * @return node defined by {@link java.util.function.Function}
     */
    public final ClickHouseNode getServer() {
        return this.server.apply(getConfig().getNodeSelector());
    }

    /**
     * Gets merged configuration.
     *
     * @return merged configuration
     */
    public ClickHouseConfig getConfig() {
        if (config == null) {
            if (options.isEmpty()) {
                config = clientConfig;
            } else {
                Map<ClickHouseConfigOption, Serializable> merged = new HashMap<>();
                merged.putAll(clientConfig.getAllOptions());
                merged.putAll(options);
                config = new ClickHouseConfig(merged, clientConfig.getDefaultCredentials(),
                        clientConfig.getNodeSelector(), clientConfig.getMetricRegistry().orElse(null));
            }
        }

        return config;
    }

    /**
     * Gets input stream.
     *
     * @return input stream
     */
    public Optional<InputStream> getInputStream() {
        return Optional.ofNullable(input);
    }

    /**
     * Gets compression used for communication between server and client. Same as
     * {@code getConfig().getCompression()}.
     *
     * @return compression used for communication between server and client
     */
    public ClickHouseCompression getCompression() {
        return getConfig().getCompression();
    }

    /**
     * Gets immutable list of external tables.
     *
     * @return immutable list of external tables
     */
    public List<ClickHouseExternalTable> getExternalTables() {
        return Collections.unmodifiableList(externalTables);
    }

    /**
     * Gets data format used for communication between server and client.
     *
     * @return data format used for communication between server and client
     */
    public ClickHouseFormat getFormat() {
        return getConfig().getFormat();
    }

    /**
     * Gets query id.
     *
     * @return query id
     */
    public Optional<String> getQueryId() {
        return ClickHouseChecker.isNullOrEmpty(queryId) ? Optional.empty() : Optional.of(queryId);
    }

    /**
     * Gets immutable settings.
     *
     * @return immutable settings
     */
    public Map<String, Object> getSettings() {
        return Collections.unmodifiableMap(settings);
    }

    /**
     * Gets session id.
     *
     * @return session id
     */
    public Optional<String> getSessionId() {
        return ClickHouseChecker.isNullOrEmpty(sessionId) ? Optional.empty() : Optional.of(sessionId);
    }

    /**
     * Gets list of SQL statements. Same as {@code getStatements(true)}.
     *
     * @return list of SQL statements
     */
    public List<String> getStatements() {
        return getStatements(true);
    }

    /**
     * Gets list of SQL statements.
     *
     * @param withSettings true to treat settings as SQL statement; false otherwise
     * @return list of SQL statements
     */
    public List<String> getStatements(boolean withSettings) {
        if (statements == null) {
            statements = new ArrayList<>();

            if (withSettings) {
                for (Entry<String, Serializable> entry : settings.entrySet()) {
                    Serializable value = entry.getValue();
                    StringBuilder sb = new StringBuilder().append("SET ").append(entry.getKey()).append('=');
                    if (value instanceof String) {
                        sb.append('\'').append(value).append('\'');
                    } else if (value instanceof Boolean) {
                        sb.append((boolean) value ? 1 : 0);
                    } else {
                        sb.append(value);
                    }
                    statements.add(sb.toString());
                }
            }

            String stmt = getQuery();
            if (!ClickHouseChecker.isNullOrEmpty(stmt)) {
                statements.add(preparedQuery == null ? ClickHouseParameterizedQuery.apply(stmt, namedParameters)
                        : preparedQuery.apply(namedParameters));
            }
        }

        return Collections.unmodifiableList(statements);
    }

    /**
     * Sets preferred compression algorithm to be used between server and client.
     *
     * @param compression compression used in transportation, null or
     *                    {@link ClickHouseCompression#NONE} means no compression
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT compression(ClickHouseCompression compression) {
        checkSealed();

        if (compression == null) {
            compression = ClickHouseCompression.NONE;
        }

        Object oldValue = options.put(ClickHouseClientOption.COMPRESSION, compression);
        if (oldValue == null || !oldValue.equals(compression)) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Adds an external table.
     *
     * @param table non-null external table
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT addExternal(ClickHouseExternalTable table) {
        checkSealed();

        if (externalTables.add(ClickHouseChecker.nonNull(table, TYPE_EXTERNAL_TABLE))) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Sets one or more external tables.
     *
     * @param table non-null external table
     * @param more  more external tables
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT external(ClickHouseExternalTable table, ClickHouseExternalTable... more) {
        checkSealed();

        externalTables.clear();
        externalTables.add(ClickHouseChecker.nonNull(table, TYPE_EXTERNAL_TABLE));
        if (more != null) {
            for (ClickHouseExternalTable e : more) {
                externalTables.add(ClickHouseChecker.nonNull(e, TYPE_EXTERNAL_TABLE));
            }
        }

        return (SelfT) this;
    }

    /**
     * Sets external tables.
     *
     * @param tables non-null external tables
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT external(Collection<ClickHouseExternalTable> tables) {
        checkSealed();

        externalTables.clear();
        if (tables != null) {
            for (ClickHouseExternalTable e : tables) {
                externalTables.add(ClickHouseChecker.nonNull(e, TYPE_EXTERNAL_TABLE));
            }
        }

        return (SelfT) this;
    }

    /**
     * Sets format to be used for communication between server and client.
     *
     * @param format non-null format
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT format(ClickHouseFormat format) {
        checkSealed();

        Object oldValue = options.put(ClickHouseClientOption.FORMAT, ClickHouseChecker.nonNull(format, "format"));
        if (oldValue == null || !oldValue.equals(format)) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Sets an option. {@code option} is for configuring client's behaviour, while
     * {@code setting} is for server.
     *
     * @param option option
     * @param value  value
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT option(ClickHouseConfigOption option, Serializable value) {
        checkSealed();

        Object oldValue = options.put(ClickHouseChecker.nonNull(option, "option"),
                ClickHouseChecker.nonNull(value, "value"));
        if (oldValue == null || !oldValue.equals(value)) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Sets all options. {@code option} is for configuring client's behaviour, while
     * {@code setting} is for server.
     *
     * @param options options
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT options(Map<ClickHouseConfigOption, Serializable> options) {
        checkSealed();

        this.options.clear();
        if (options != null) {
            this.options.putAll(options);
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Sets all options. {@code option} is for configuring client's behaviour, while
     * {@code setting} is for server.
     *
     * @param options options
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT options(Properties options) {
        checkSealed();

        this.options.clear();
        if (options != null) {
            for (Entry<Object, Object> e : options.entrySet()) {
                Object key = e.getKey();
                Object value = e.getValue();
                if (key == null || value == null) {
                    continue;
                }

                ClickHouseClientOption o = ClickHouseClientOption.fromKey(key.toString());
                if (o != null) {
                    this.options.put(o, ClickHouseConfigOption.fromString(value.toString(), o.getValueType()));
                }
            }
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Sets stringified parameters. Be aware of SQL injection risk as mentioned in
     * {@link #params(String, String...)}.
     *
     * @param values stringified parameters
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT params(Collection<String> values) {
        checkSealed();

        namedParameters.clear();

        if (values != null && !values.isEmpty()) {
            List<String> names = getPreparedQuery().getNamedParameters();
            int size = names.size();
            int index = 0;
            for (String v : values) {
                namedParameters.put(names.get(index), v);
                if (++index >= size) {
                    break;
                }
            }
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Sets parameters wrapped by {@link ClickHouseValue}. Safer but a bit slower
     * than {@link #params(String, String...)}. Consider to reuse ClickHouseValue
     * object and its update methods for less overhead in batch processing.
     *
     * @param value parameter
     * @param more  more parameters
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT params(ClickHouseValue value, ClickHouseValue... more) {
        checkSealed();

        namedParameters.clear();

        if (value != null) { // it doesn't make sense to pass null as first parameter
            List<String> names = getPreparedQuery().getNamedParameters();
            int size = names.size();
            int index = 0;

            namedParameters.put(names.get(index++), value.toSqlExpression());

            if (more != null && more.length > 0) {
                for (ClickHouseValue v : more) {
                    if (index >= size) {
                        break;
                    }
                    namedParameters.put(names.get(index++),
                            v != null ? v.toSqlExpression() : ClickHouseValues.NULL_EXPR);
                }
            }
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Sets parameters wrapped by {@link ClickHouseValue}. Safer but a bit slower
     * than {@link #params(String, String...)}. Consider to reuse ClickHouseValue
     * object and its update methods for less overhead in batch processing.
     *
     * @param values parameters
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT params(ClickHouseValue[] values) {
        checkSealed();

        namedParameters.clear();

        if (values != null && values.length > 0) {
            List<String> names = getPreparedQuery().getNamedParameters();
            int size = names.size();
            int index = 0;
            for (ClickHouseValue v : values) {
                namedParameters.put(names.get(index), v != null ? v.toSqlExpression() : ClickHouseValues.NULL_EXPR);
                if (++index >= size) {
                    break;
                }
            }
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Sets stringified parameters which are used to substitude named parameters in
     * SQL query without further transformation and validation. Keep in mind that
     * stringified parameter is a SQL expression, meaning it could be a
     * sub-query(SQL injection) in addition to value like number and string.
     *
     * @param value stringified parameter
     * @param more  more stringified parameters
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT params(String value, String... more) {
        checkSealed();

        namedParameters.clear();

        List<String> names = getPreparedQuery().getNamedParameters();
        int size = names.size();
        int index = 0;
        namedParameters.put(names.get(index++), value);

        if (more != null && more.length > 0) {
            for (String v : more) {
                if (index >= size) {
                    break;
                }
                namedParameters.put(names.get(index++), v);
            }
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Sets stringified parameters which are used to substitude named parameters in
     * SQL query without further transformation and validation. Keep in mind that
     * stringified parameter is a SQL expression, meaning it could be a
     * sub-query(SQL injection) in addition to value like number and string.
     *
     * @param values stringified parameters
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT params(String[] values) {
        checkSealed();

        namedParameters.clear();

        if (values != null && values.length > 0) {
            List<String> names = getPreparedQuery().getNamedParameters();
            int size = names.size();
            int index = 0;
            for (String v : values) {
                namedParameters.put(names.get(index), v);
                if (++index >= size) {
                    break;
                }
            }
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Set raw parameters, which will later be stringified using
     * {@link ClickHouseValues#convertToSqlExpression(Object)}. Although it is
     * convenient to use, it's NOT recommended in most cases except for a few
     * parameters and/or testing.
     *
     * @param value raw parameter
     * @param more  more raw parameters
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT params(Object value, Object... more) {
        checkSealed();

        namedParameters.clear();

        List<String> names = getPreparedQuery().getNamedParameters();
        int size = names.size();
        int index = 0;
        namedParameters.put(names.get(index++), ClickHouseValues.convertToSqlExpression(value));

        if (more != null && more.length > 0) {
            for (Object v : more) {
                if (index >= size) {
                    break;
                }
                namedParameters.put(names.get(index++), ClickHouseValues.convertToSqlExpression(v));
            }
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Set raw parameters, which will later be stringified using
     * {@link ClickHouseValues#convertToSqlExpression(Object)}. Although it is
     * convenient to use, it's NOT recommended in most cases except for a few
     * parameters and/or testing.
     *
     * @param values raw parameters
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT params(Object[] values) {
        checkSealed();

        namedParameters.clear();

        if (values != null && values.length > 0) {
            List<String> names = getPreparedQuery().getNamedParameters();
            int size = names.size();
            int index = 0;
            for (Object v : values) {
                namedParameters.put(names.get(index), ClickHouseValues.convertToSqlExpression(v));
                if (++index >= size) {
                    break;
                }
            }
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Sets named parameters. Be aware of SQL injection risk as mentioned in
     * {@link #params(String, String...)}.
     *
     * @param namedParams named parameters
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT params(Map<String, String> namedParams) {
        checkSealed();

        namedParameters.clear();

        if (namedParams != null) {
            namedParameters.putAll(namedParams);
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Sets parameterized query. Same as {@code query(query, null)}.
     *
     * @param query non-null parameterized query
     * @return the request itself
     */
    public SelfT query(ClickHouseParameterizedQuery query) {
        return query(query, null);
    }

    /**
     * Sets query. Same as {@code query(sql, null)}.
     *
     * @param sql non-empty query
     * @return the request itself
     */
    public SelfT query(String sql) {
        return query(sql, null);
    }

    /**
     * Sets parameterized query and optinally query id.
     *
     * @param query   non-null parameterized query
     * @param queryId query id, null means no query id
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT query(ClickHouseParameterizedQuery query, String queryId) {
        checkSealed();

        if (!ClickHouseChecker.nonNull(query, "query").equals(this.preparedQuery)) {
            this.preparedQuery = query;
            this.sql = query.getOriginalQuery();
            resetCache();
        }

        this.queryId = queryId;

        return (SelfT) this;
    }

    /**
     * Sets query and optinally query id.
     *
     * @param sql     non-empty query
     * @param queryId query id, null means no query id
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT query(String sql, String queryId) {
        checkSealed();

        if (!ClickHouseChecker.nonBlank(sql, "sql").equals(this.sql)) {
            this.sql = sql;
            this.preparedQuery = null;
            resetCache();
        }

        this.queryId = queryId;

        return (SelfT) this;
    }

    /**
     * Clears session configuration including session id, whether to validate the id
     * and session timeout.
     *
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT clearSession() {
        checkSealed();

        boolean changed = this.sessionId != null;
        this.sessionId = null;

        Object oldValue = null;
        oldValue = options.remove(ClickHouseClientOption.SESSION_CHECK);
        changed = changed || oldValue != null;

        oldValue = options.remove(ClickHouseClientOption.SESSION_TIMEOUT);
        changed = changed || oldValue != null;

        if (changed) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Sets current session using custom id. Same as
     * {@code session(sessionId, null, null)}.
     *
     * @param sessionId session id, null means no session
     * @return the request itself
     */
    public SelfT session(String sessionId) {
        return session(sessionId, null, null);
    }

    /**
     * Sets session. Same as {@code session(sessionId, check, null)}.
     *
     * @param sessionId session id, null means no session
     * @param check     whether the server should check if the session id exists or
     *                  not
     * @return the request itself
     */
    public SelfT session(String sessionId, Boolean check) {
        return session(sessionId, check, null);
    }

    /**
     * Sets current session. Same as {@code session(sessionId, null, timeout)}.
     *
     * @param sessionId session id, null means no session
     * @param timeout   timeout in milliseconds
     * @return the request itself
     */
    public SelfT session(String sessionId, Integer timeout) {
        return session(sessionId, null, timeout);
    }

    /**
     * Sets current session.
     *
     * @param sessionId session id, null means no session
     * @param check     whether the server should check if the session id exists or
     *                  not
     * @param timeout   timeout in milliseconds
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT session(String sessionId, Boolean check, Integer timeout) {
        checkSealed();

        boolean changed = !Objects.equals(this.sessionId, sessionId);
        this.sessionId = sessionId;

        Object oldValue = null;
        if (check != null) {
            oldValue = options.put(ClickHouseClientOption.SESSION_CHECK, check);
            changed = oldValue == null || !oldValue.equals(check);
        }

        if (timeout != null) {
            oldValue = options.put(ClickHouseClientOption.SESSION_TIMEOUT, timeout);
            changed = changed || oldValue == null || !oldValue.equals(timeout);
        }

        if (changed) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Sets a setting. See
     * https://clickhouse.tech/docs/en/operations/settings/settings/ for more
     * information.
     *
     * @param setting non-empty setting to set
     * @param value   value of the setting
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT set(String setting, Serializable value) {
        checkSealed();

        Serializable oldValue = settings.put(ClickHouseChecker.nonBlank(setting, "setting"),
                ClickHouseChecker.nonNull(value, "value"));
        if (oldValue == null || !oldValue.equals(value)) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Sets a setting. See
     * https://clickhouse.tech/docs/en/operations/settings/settings/ for more
     * information.
     *
     * @param setting non-empty setting to set
     * @param value   value of the setting
     * @return the request itself
     */
    public SelfT set(String setting, String value) {
        checkSealed();

        return set(setting, (Serializable) ClickHouseUtils.escape(value, '\''));
    }

    /**
     * Sets target table. Same as {@code table(table, null)}.
     *
     * @param table non-empty table name
     * @return the request itself
     */
    public SelfT table(String table) {
        return table(table, null);
    }

    /**
     * Sets target table and optionally query id. This will generate a query like
     * {@code SELECT * FROM [table]} and override the one set by
     * {@link #query(String, String)}.
     *
     * @param table   non-empty table name
     * @param queryId query id, null means no query id
     * @return the request itself
     */
    public SelfT table(String table, String queryId) {
        return query("SELECT * FROM " + ClickHouseChecker.nonBlank(table, "table"), queryId);
    }

    /**
     * Changes current database.
     *
     * @param database non-empty database name
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT use(String database) {
        checkSealed();

        Object oldValue = options.put(ClickHouseClientOption.DATABASE,
                ClickHouseChecker.nonBlank(database, "database"));
        if (oldValue == null || !oldValue.equals(database)) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Removes an external table.
     *
     * @param external non-null external table
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT removeExternal(ClickHouseExternalTable external) {
        checkSealed();

        if (externalTables.remove(ClickHouseChecker.nonNull(external, TYPE_EXTERNAL_TABLE))) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Removes an external table by name.
     *
     * @param name non-empty external table name
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT removeExternal(String name) {
        checkSealed();

        if (!ClickHouseChecker.isNullOrEmpty(name)) {
            boolean removed = false;
            Iterator<ClickHouseExternalTable> i = externalTables.iterator();
            while (i.hasNext()) {
                ClickHouseExternalTable e = i.next();
                if (name.equals(e.getName())) {
                    i.remove();
                    removed = true;
                }
            }

            if (removed) {
                resetCache();
            }
        }

        return (SelfT) this;
    }

    /**
     * Removes an option.
     *
     * @param option option to be removed
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT removeOption(ClickHouseConfigOption option) {
        checkSealed();

        if (options.remove(ClickHouseChecker.nonNull(option, "option")) != null) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Removes a setting.
     *
     * @param setting name of the setting
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT removeSetting(String setting) {
        checkSealed();

        if (settings.remove(ClickHouseChecker.nonBlank(setting, "setting")) != null) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Resets the request to start all over.
     *
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT reset() {
        checkSealed();

        this.externalTables.clear();
        this.options.clear();
        this.settings.clear();

        this.namedParameters.clear();

        this.input = null;
        this.sql = null;
        this.preparedQuery = null;
        this.queryId = null;
        this.sessionId = null;

        resetCache();

        return (SelfT) this;
    }

    /**
     * Creates a sealed request, which is an immutable copy of the current request.
     *
     * @return sealed request, an immutable copy of the current request
     */
    public ClickHouseRequest<SelfT> seal() {
        ClickHouseRequest<SelfT> req = this;

        if (!isSealed()) {
            // no idea which node we'll connect to until now
            req = new ClickHouseRequest<>(client, getServer(), true);
            req.externalTables.addAll(externalTables);
            req.options.putAll(options);
            req.settings.putAll(settings);

            req.namedParameters.putAll(namedParameters);

            req.input = input;
            req.queryId = queryId;
            req.sessionId = sessionId;
            req.sql = sql;
            req.preparedQuery = preparedQuery;
        }

        return req;
    }

    /**
     * Creates a new request for mutation.
     *
     * @return request for mutation
     */
    public Mutation write() {
        checkSealed();

        return new Mutation(this, false);
    }

    /**
     * Executes the request. Same as {@code client.execute(request.seal())}.
     * 
     * @return future to get response
     * @throws ClickHouseException when error occurred preparing for the execution
     */
    public CompletableFuture<ClickHouseResponse> execute() throws ClickHouseException {
        return client.execute(isSealed() ? this : seal());
    }
}
