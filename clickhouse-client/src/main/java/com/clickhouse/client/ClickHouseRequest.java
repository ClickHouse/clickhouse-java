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
import java.util.concurrent.CompletionException;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseConfigChangeListener;
import com.clickhouse.client.config.ClickHouseOption;
import com.clickhouse.client.data.ClickHouseExternalTable;

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
        }

        @Override
        protected String getQuery() {
            if (input != null && sql != null) {
                return new StringBuilder().append(sql).append(" FORMAT ").append(getInputFormat().name()).toString();
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

            final ClickHouseRequest<?> self = this;
            final String fileName = ClickHouseChecker.nonEmpty(file, "File");
            this.input = changeProperty(PROP_DATA, this.input, ClickHouseDeferredValue.of(() -> {
                try {
                    return ClickHouseInputStream.of(new FileInputStream(fileName), 123, compression);
                } catch (FileNotFoundException e) {
                    throw new IllegalArgumentException(e);
                }
            }));
            return this;
        }

        /**
         * Loads data from input stream.
         *
         * @param input input stream
         * @return mutation requets
         */
        public Mutation data(InputStream input) {
            return data(ClickHouseInputStream.of(input));
        }

        /**
         * Loads data from input stream.
         *
         * @param input input stream
         * @return mutation requets
         */
        public Mutation data(ClickHouseInputStream input) {
            checkSealed();

            this.input = changeProperty(PROP_DATA, this.input,
                    ClickHouseDeferredValue.of(input, ClickHouseInputStream.class));

            return this;
        }

        /**
         * Loads data from input stream.
         *
         * @param input input stream
         * @return mutation requets
         */
        public Mutation data(ClickHouseDeferredValue<ClickHouseInputStream> input) {
            checkSealed();

            this.input = changeProperty(PROP_DATA, this.input, input);

            return this;
        }

        /**
         * Sends mutation requets for execution. Same as
         * {@code client.execute(request.seal())}.
         *
         * @return non-null future to get response
         * @throws CompletionException when error occurred
         */
        public CompletableFuture<ClickHouseResponse> send() {
            return execute();
        }

        /**
         * Synchronous version of {@link #send()}.
         *
         * @return non-null response
         * @throws ClickHouseException when error occurred during execution
         */
        public ClickHouseResponse sendAndWait() throws ClickHouseException {
            return executeAndWait();
        }

        @Override
        public Mutation table(String table, String queryId) {
            checkSealed();
            super.query("INSERT INTO " + ClickHouseChecker.nonBlank(table, "table"), queryId);
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
                req.sql = sql;

                req.preparedQuery = preparedQuery;
            }

            return req;
        }
    }

    private static final long serialVersionUID = 4990313525960702287L;

    static final String PROP_DATA = "data";
    static final String PROP_PREPARED_QUERY = "preparedQuery";
    static final String PROP_QUERY = "query";
    static final String PROP_QUERY_ID = "queryId";

    private final boolean sealed;

    private transient ClickHouseClient client;

    protected final ClickHouseConfig clientConfig;
    protected final Function<ClickHouseNodeSelector, ClickHouseNode> server;
    protected final transient List<ClickHouseExternalTable> externalTables;
    protected final Map<ClickHouseOption, Serializable> options;
    protected final Map<String, Serializable> settings;

    protected final Map<String, String> namedParameters;

    protected transient ClickHouseDeferredValue<ClickHouseInputStream> input;
    protected String queryId;
    protected String sql;
    protected ClickHouseParameterizedQuery preparedQuery;

    protected transient ClickHouseConfigChangeListener<ClickHouseRequest<?>> changeListener;

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

    protected <T> T changeProperty(String property, T oldValue, T newValue) {
        if (changeListener != null && !Objects.equals(oldValue, newValue)) {
            changeListener.propertyChanged(this, property, oldValue, newValue);
        }
        return newValue;
    }

    protected void checkSealed() {
        if (sealed) {
            throw new IllegalStateException("Sealed request is immutable");
        }
    }

    protected ClickHouseClient getClient() {
        if (client == null) {
            client = ClickHouseClient.builder().config(clientConfig).build();
        }

        return client;
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
     * Gets request configuration.
     *
     * @return request configuration
     */
    public ClickHouseConfig getConfig() {
        if (config == null) {
            if (options.isEmpty()) {
                config = clientConfig;
            } else {
                Map<ClickHouseOption, Serializable> merged = new HashMap<>();
                merged.putAll(clientConfig.getAllOptions());
                merged.putAll(options);
                config = new ClickHouseConfig(merged, clientConfig.getDefaultCredentials(),
                        clientConfig.getNodeSelector(), clientConfig.getMetricRegistry().orElse(null));
            }
        }

        return config;
    }

    /**
     * Sets change listener.
     *
     * @param listener change listener which may or may not be null
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public final SelfT setChangeListener(ClickHouseConfigChangeListener<ClickHouseRequest<?>> listener) {
        this.changeListener = listener;
        return (SelfT) this;
    }

    /**
     * Gets input stream.
     *
     * @return input stream
     */
    public Optional<ClickHouseInputStream> getInputStream() {
        return input != null ? input.getOptional() : Optional.empty();
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
     * Gets data format used for input(e.g. writing data into server).
     *
     * @return data format for input
     */
    public ClickHouseFormat getInputFormat() {
        ClickHouseFormat format = getFormat();
        return options.containsKey(ClickHouseClientOption.FORMAT) ? format : format.defaultInputFormat();
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
     * Gets prepared query, which is a loosely parsed query with the origianl query
     * and list of parameters.
     *
     * @return prepared query
     */
    public ClickHouseParameterizedQuery getPreparedQuery() {
        if (preparedQuery == null) {
            preparedQuery = changeProperty(PROP_PREPARED_QUERY, preparedQuery,
                    ClickHouseParameterizedQuery.of(getConfig(), getQuery()));
        }

        return preparedQuery;
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
        String sessionId = (String) getConfig().getOption(ClickHouseClientOption.SESSION_ID);
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
                StringBuilder builder = new StringBuilder();
                if (preparedQuery == null) {
                    ClickHouseParameterizedQuery.apply(builder, stmt, namedParameters);
                } else {
                    preparedQuery.apply(builder, namedParameters);
                }
                statements.add(builder.toString());
            }
        }

        return Collections.unmodifiableList(statements);
    }

    /**
     * Enable or disable compression of server response. Pay attention that
     * {@link ClickHouseClientOption#COMPRESS_ALGORITHM} and
     * {@link ClickHouseClientOption#COMPRESS_LEVEL} will be used.
     *
     * @param enable true to enable compression of server response; false otherwise
     * @return the request itself
     */
    public SelfT compressServerResponse(boolean enable) {
        return compressServerResponse(enable, null,
                (int) ClickHouseClientOption.COMPRESS_LEVEL.getEffectiveDefaultValue());
    }

    /**
     * Enable or disable compression of server response. Pay attention that
     * {@link ClickHouseClientOption#COMPRESS_LEVEL} will be used.
     *
     * @param enable            true to enable compression of server response; false
     *                          otherwise
     * @param compressAlgorithm compression algorithm, null is treated as
     *                          {@link ClickHouseCompression#NONE} or
     *                          {@link ClickHouseClientOption#COMPRESS_ALGORITHM}
     *                          depending on whether enabled
     * @return the request itself
     */
    public SelfT compressServerResponse(boolean enable, ClickHouseCompression compressAlgorithm) {
        return compressServerResponse(enable, compressAlgorithm,
                (int) ClickHouseClientOption.COMPRESS_LEVEL.getEffectiveDefaultValue());
    }

    /**
     * Enable or disable compression of server response.
     *
     * @param enable            true to enable compression of server response; false
     *                          otherwise
     * @param compressAlgorithm compression algorithm, null is treated as
     *                          {@link ClickHouseCompression#NONE} or
     *                          {@link ClickHouseClientOption#COMPRESS_ALGORITHM}
     *                          depending on whether enabled
     * @param compressLevel     compression level
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT compressServerResponse(boolean enable, ClickHouseCompression compressAlgorithm, int compressLevel) {
        checkSealed();

        if (compressAlgorithm == null) {
            compressAlgorithm = enable
                    ? (ClickHouseCompression) ClickHouseClientOption.COMPRESS_ALGORITHM.getEffectiveDefaultValue()
                    : ClickHouseCompression.NONE;
        }

        if (compressLevel < 0) {
            compressLevel = 0;
        } else if (compressLevel > 9) {
            compressLevel = 9;
        }

        option(ClickHouseClientOption.COMPRESS, enable);
        option(ClickHouseClientOption.COMPRESS_ALGORITHM, compressAlgorithm);
        option(ClickHouseClientOption.COMPRESS_LEVEL, compressLevel);

        return (SelfT) this;
    }

    /**
     * Enable or disable compression of client request. Pay attention that
     * {@link ClickHouseClientOption#DECOMPRESS_ALGORITHM} and
     * {@link ClickHouseClientOption#DECOMPRESS_LEVEL} will be used.
     *
     * @param enable true to enable compression of client request; false otherwise
     * @return the request itself
     */
    public SelfT decompressClientRequest(boolean enable) {
        return decompressClientRequest(enable, null,
                (int) ClickHouseClientOption.DECOMPRESS_LEVEL.getEffectiveDefaultValue());
    }

    /**
     * Enable or disable compression of client request. Pay attention that
     * {@link ClickHouseClientOption#DECOMPRESS_LEVEL} will be used.
     *
     * @param enable            true to enable compression of client request; false
     *                          otherwise
     * @param compressAlgorithm compression algorithm, null is treated as
     *                          {@link ClickHouseCompression#NONE} or
     *                          {@link ClickHouseClientOption#DECOMPRESS_ALGORITHM}
     *                          depending on whether enabled
     * @return the request itself
     */
    public SelfT decompressClientRequest(boolean enable, ClickHouseCompression compressAlgorithm) {
        return decompressClientRequest(enable, compressAlgorithm,
                (int) ClickHouseClientOption.DECOMPRESS_LEVEL.getEffectiveDefaultValue());
    }

    /**
     * Enable or disable compression of client request.
     *
     * @param enable            true to enable compression of client request; false
     *                          otherwise
     * @param compressAlgorithm compression algorithm, null is treated as
     *                          {@link ClickHouseCompression#NONE}
     * @param compressLevel     compression level
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT decompressClientRequest(boolean enable, ClickHouseCompression compressAlgorithm, int compressLevel) {
        checkSealed();

        if (compressAlgorithm == null) {
            compressAlgorithm = enable
                    ? (ClickHouseCompression) ClickHouseClientOption.DECOMPRESS_ALGORITHM.getEffectiveDefaultValue()
                    : ClickHouseCompression.NONE;
        }

        if (compressLevel < 0) {
            compressLevel = 0;
        } else if (compressLevel > 9) {
            compressLevel = 9;
        }

        option(ClickHouseClientOption.DECOMPRESS, enable);
        option(ClickHouseClientOption.DECOMPRESS_ALGORITHM, compressAlgorithm);
        option(ClickHouseClientOption.DECOMPRESS_LEVEL, compressLevel);

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
     * @param format preferred format, {@code null} means reset to default
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT format(ClickHouseFormat format) {
        checkSealed();
        option(ClickHouseClientOption.FORMAT, format);
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
    public SelfT option(ClickHouseOption option, Serializable value) {
        checkSealed();

        if (value == null) {
            return removeOption(option);
        }

        Serializable oldValue = options.put(ClickHouseChecker.nonNull(option, "Option"), value);
        if (oldValue == null || !oldValue.equals(value)) {
            if (changeListener != null) {
                changeListener.optionChanged(this, option, oldValue, value);
            }
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
    public SelfT options(Map<ClickHouseOption, Serializable> options) {
        checkSealed();

        if (changeListener == null) {
            this.options.clear();
            if (options != null) {
                this.options.putAll(options);
            }
            resetCache();
        } else {
            Map<ClickHouseOption, Serializable> m = new HashMap<>();
            m.putAll(this.options);
            if (options != null) {
                for (Entry<ClickHouseOption, Serializable> e : options.entrySet()) {
                    option(e.getKey(), e.getValue());
                    m.remove(e.getKey());
                }
            }
            for (ClickHouseOption o : m.keySet()) {
                removeOption(o);
            }
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
    public SelfT options(Properties options) {
        checkSealed();

        Map<ClickHouseOption, Serializable> m = new HashMap<>();
        m.putAll(this.options);

        if (options != null) {
            for (Entry<Object, Object> e : options.entrySet()) {
                Object key = e.getKey();
                Object value = e.getValue();
                if (key == null || value == null) {
                    continue;
                }

                ClickHouseClientOption o = ClickHouseClientOption.fromKey(key.toString());
                if (o != null) {
                    option(o, ClickHouseOption.fromString(value.toString(), o.getValueType()));
                    m.remove(o);
                }
            }
        }

        for (ClickHouseOption o : m.keySet()) {
            removeOption(o);
        }

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
            List<String> names = getPreparedQuery().getParameters();
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
            List<String> names = getPreparedQuery().getParameters();
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
            List<String> names = getPreparedQuery().getParameters();
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

        List<String> names = getPreparedQuery().getParameters();
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
            List<String> names = getPreparedQuery().getParameters();
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

        List<String> names = getPreparedQuery().getParameters();
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
            List<String> names = getPreparedQuery().getParameters();
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

        if (!ClickHouseChecker.nonNull(query, PROP_QUERY).equals(this.preparedQuery)) {
            this.preparedQuery = changeProperty(PROP_PREPARED_QUERY, this.preparedQuery, query);
            this.sql = changeProperty(PROP_QUERY, this.sql, query.getOriginalQuery());
            resetCache();
        }

        this.queryId = changeProperty(PROP_QUERY_ID, this.queryId, queryId);

        return (SelfT) this;
    }

    /**
     * Sets query and optinally query id.
     *
     * @param query   non-empty query
     * @param queryId query id, null means no query id
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT query(String query, String queryId) {
        checkSealed();

        if (!ClickHouseChecker.nonBlank(query, PROP_QUERY).equals(this.sql)) {
            this.sql = changeProperty(PROP_QUERY, this.sql, query);
            this.preparedQuery = changeProperty(PROP_PREPARED_QUERY, this.preparedQuery, null);
            resetCache();
        }

        this.queryId = changeProperty(PROP_QUERY_ID, this.queryId, queryId);

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

        removeOption(ClickHouseClientOption.SESSION_ID);
        removeOption(ClickHouseClientOption.SESSION_CHECK);
        removeOption(ClickHouseClientOption.SESSION_TIMEOUT);

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

        option(ClickHouseClientOption.SESSION_ID, sessionId);
        option(ClickHouseClientOption.SESSION_CHECK, check);
        option(ClickHouseClientOption.SESSION_TIMEOUT, timeout);

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

        if (value == null) {
            return removeSetting(setting);
        }

        Serializable oldValue = settings.put(ClickHouseChecker.nonBlank(setting, "setting"), value);
        if (oldValue == null || !oldValue.equals(value)) {
            if (changeListener != null) {
                changeListener.settingChanged(this, setting, oldValue, value);
            }
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
     * @param database database name, {@code null} means reset to default
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT use(String database) {
        checkSealed();
        option(ClickHouseClientOption.DATABASE, database);
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
    public SelfT removeOption(ClickHouseOption option) {
        checkSealed();

        Serializable oldValue = options.remove(ClickHouseChecker.nonNull(option, "option"));
        if (oldValue != null) {
            if (changeListener != null) {
                changeListener.optionChanged(this, option, oldValue, null);
            }
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

        Serializable oldValue = settings.remove(ClickHouseChecker.nonBlank(setting, "setting"));
        if (oldValue != null) {
            if (changeListener != null) {
                changeListener.settingChanged(this, setting, oldValue, null);
            }
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
        if (changeListener == null) {
            this.options.clear();
            this.settings.clear();
        } else {
            for (ClickHouseOption o : this.options.keySet().toArray(new ClickHouseOption[0])) {
                removeOption(o);
            }
            for (String s : this.settings.keySet().toArray(new String[0])) {
                removeSetting(s);
            }
            this.changeListener = null;
        }
        this.namedParameters.clear();

        this.input = changeProperty(PROP_DATA, this.input, null);
        this.sql = changeProperty(PROP_QUERY, this.sql, null);
        this.preparedQuery = changeProperty(PROP_PREPARED_QUERY, this.preparedQuery, null);
        this.queryId = changeProperty(PROP_QUERY_ID, this.queryId, null);

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
     * @return non-null future to get response
     * @throws CompletionException when error occurred during execution
     */
    public CompletableFuture<ClickHouseResponse> execute() {
        return getClient().execute(isSealed() ? this : seal());
    }

    /**
     * Synchronous version of {@link #execute()}.
     *
     * @return non-null response
     * @throws ClickHouseException when error occurred during execution
     */
    public ClickHouseResponse executeAndWait() throws ClickHouseException {
        return getClient().executeAndWait(isSealed() ? this : seal());
    }
}
