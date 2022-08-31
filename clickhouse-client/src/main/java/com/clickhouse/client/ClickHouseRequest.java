package com.clickhouse.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;
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

    private static final Set<String> SPECIAL_SETTINGS;

    static final String PARAM_SETTING = "setting";

    static {
        Set<String> set = new HashSet<>();
        set.add("query_id");
        set.add(ClickHouseClientOption.SESSION_ID.getKey());
        set.add(ClickHouseClientOption.SESSION_CHECK.getKey());
        set.add(ClickHouseClientOption.SESSION_TIMEOUT.getKey());
        SPECIAL_SETTINGS = Collections.unmodifiableSet(set);
    }

    /**
     * Mutation request.
     */
    public static class Mutation extends ClickHouseRequest<Mutation> {
        private ClickHouseWriter writer;

        protected Mutation(ClickHouseRequest<?> request, boolean sealed) {
            super(request.getClient(), request.server, request.serverRef, request.options, sealed);

            this.settings.putAll(request.settings);
            this.txRef.set(request.txRef.get());

            this.changeListener = request.changeListener;
            this.serverListener = request.serverListener;
        }

        @Override
        protected String getQuery() {
            if (input != null && sql != null) {
                int tmp = 0;
                int len = sql.length();
                int index = len;
                while ((tmp = ClickHouseUtils.skipContentsUntil(sql, tmp, sql.length(), "format", false)) < len) {
                    index = tmp;
                }

                StringBuilder builder = new StringBuilder();
                boolean spaces = false;
                for (; index < len; index++) {
                    char ch = sql.charAt(index);
                    if (ClickHouseUtils.isQuote(ch) || ClickHouseUtils.isOpenBracket(ch)) {
                        break;
                    } else if (Character.isWhitespace(ch)) {
                        if (builder.length() > 0) {
                            spaces = true;
                        }
                    } else if (index + 1 < len) {
                        char nextCh = sql.charAt(index + 1);
                        if (ch == '-' && nextCh == '-') {
                            index = ClickHouseUtils.skipSingleLineComment(sql, index + 2, len) - 1;
                        } else if (ch == '/' && nextCh == '*') {
                            index = ClickHouseUtils.skipMultiLineComment(sql, index + 2, len) - 1;
                        } else if (ch == '\\') {
                            index++;
                        } else {
                            if (spaces) {
                                break;
                            } else {
                                builder.append(ch);
                            }
                        }
                    } else {
                        if (spaces) {
                            break;
                        } else {
                            builder.append(ch);
                        }
                    }
                }

                return builder.length() > 0 && index == len ? sql
                        : new StringBuilder().append(sql).append("\n FORMAT ").append(getInputFormat().name())
                                .toString();
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
         * Sets custom writer for streaming. This will create a piped stream between the
         * writer and ClickHouse server.
         *
         * @param writer writer
         * @return mutation request
         */
        public Mutation data(ClickHouseWriter writer) {
            checkSealed();

            this.writer = changeProperty(PROP_WRITER, this.writer, writer);
            return this;
        }

        /**
         * Loads data from given file which may or may not be compressed.
         *
         * @param file absolute or relative path of the file, file extension will be
         *             used to determine if it's compressed or not
         * @return mutation request
         */
        public Mutation data(ClickHouseFile file) {
            checkSealed();

            final ClickHouseRequest<?> self = this;
            if (ClickHouseChecker.nonNull(file, "File").hasFormat()) {
                format(file.getFormat());
            }
            decompressClientRequest(file.isCompressed(), file.getCompressionAlgorithm());
            this.input = changeProperty(PROP_DATA, this.input, ClickHouseDeferredValue
                    .of(() -> ClickHouseInputStream.of(file, self.getConfig().getReadBufferSize(), null)));
            return this;
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
            final ClickHouseFile wrappedFile = ClickHouseFile.of(file, compression, 0, null);
            if (wrappedFile.hasFormat()) {
                format(wrappedFile.getFormat());
            }
            this.input = changeProperty(PROP_DATA, this.input, ClickHouseDeferredValue
                    .of(() -> ClickHouseInputStream.of(wrappedFile, self.getConfig().getReadBufferSize(), null)));
            return this;
        }

        /**
         * Loads data from input stream.
         *
         * @param input input stream
         * @return mutation request
         */
        public Mutation data(InputStream input) {
            return data(ClickHouseInputStream.of(input));
        }

        /**
         * Loads data from input stream.
         *
         * @param input input stream
         * @return mutation request
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
         * @return mutation request
         */
        public Mutation data(ClickHouseDeferredValue<ClickHouseInputStream> input) {
            checkSealed();

            this.input = changeProperty(PROP_DATA, this.input, input);

            return this;
        }

        @Override
        public CompletableFuture<ClickHouseResponse> execute() {
            if (writer != null) {
                ClickHouseConfig c = getConfig();
                ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                        .createPipedOutputStream(c, null);
                data(stream.getInputStream());
                CompletableFuture<ClickHouseResponse> future = null;
                if (c.isAsync()) {
                    future = getClient().execute(this);
                }
                try (ClickHouseOutputStream out = stream) {
                    writer.write(out);
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
                if (future != null) {
                    return future;
                }
            }

            return getClient().execute(this);
        }

        @Override
        public ClickHouseResponse executeAndWait() throws ClickHouseException {
            if (writer != null) {
                ClickHouseConfig c = getConfig();
                ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                        .createPipedOutputStream(c, null);
                data(stream.getInputStream());
                CompletableFuture<ClickHouseResponse> future = null;
                if (c.isAsync()) {
                    future = getClient().execute(this);
                }
                try (ClickHouseOutputStream out = stream) {
                    writer.write(out);
                } catch (IOException e) {
                    throw ClickHouseException.of(e, getServer());
                }
                if (future != null) {
                    try {
                        return future.get(c.getSocketTimeout(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw ClickHouseException.forCancellation(e, getServer());
                    } catch (CancellationException e) {
                        throw ClickHouseException.forCancellation(e, getServer());
                    } catch (ExecutionException | TimeoutException | UncheckedIOException e) {
                        Throwable cause = e.getCause();
                        if (cause == null) {
                            cause = e;
                        }
                        throw cause instanceof ClickHouseException ? (ClickHouseException) cause
                                : ClickHouseException.of(cause, getServer());
                    } catch (RuntimeException e) { // unexpected
                        throw ClickHouseException.of(e, getServer());
                    }
                }
            }

            return getClient().executeAndWait(this);
        }

        /**
         * Sends mutation request for execution. Same as
         * {@code client.execute(request.seal())}.
         *
         * @return non-null future to get response
         * @throws CompletionException when error occurred
         * @deprecated will be removed in v0.3.3, please use {@link #execute()}
         *             instead
         */
        @Deprecated
        public CompletableFuture<ClickHouseResponse> send() {
            return execute();
        }

        /**
         * Synchronous version of {@link #send()}.
         *
         * @return non-null response
         * @throws ClickHouseException when error occurred during execution
         * @deprecated will be removed in v0.3.3, please use {@link #executeAndWait()}
         *             instead
         */
        public ClickHouseResponse sendAndWait() throws ClickHouseException {
            return executeAndWait();
        }

        @Override
        public Mutation table(String table, String queryId) {
            checkSealed();
            return super.query("INSERT INTO " + ClickHouseChecker.nonBlank(table, "table"), queryId);
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
                req.managerRef.set(managerRef.get());
                req.txRef.set(txRef.get());
            }

            return req;
        }
    }

    private static final long serialVersionUID = 4990313525960702287L;

    static final String PROP_DATA = "data";
    static final String PROP_MANAGER = "manager";
    static final String PROP_OUTPUT = "output";
    static final String PROP_PREPARED_QUERY = "preparedQuery";
    static final String PROP_QUERY = "query";
    static final String PROP_QUERY_ID = "queryId";
    static final String PROP_TRANSACTION = "transaction";
    static final String PROP_WRITER = "writer";

    private final boolean sealed;

    private final ClickHouseClient client;

    protected final AtomicReference<ClickHouseRequestManager> managerRef;
    protected final Function<ClickHouseNodeSelector, ClickHouseNode> server;
    protected final AtomicReference<ClickHouseNode> serverRef;
    protected final AtomicReference<ClickHouseTransaction> txRef;

    protected final List<ClickHouseExternalTable> externalTables;
    protected final Map<ClickHouseOption, Serializable> options;
    protected final Map<String, Serializable> settings;

    protected final Map<String, String> namedParameters;

    protected transient ClickHouseDeferredValue<ClickHouseInputStream> input;
    protected transient ClickHouseDeferredValue<ClickHouseOutputStream> output;
    protected String queryId;
    protected String sql;
    protected ClickHouseParameterizedQuery preparedQuery;

    protected transient ClickHouseConfigChangeListener<ClickHouseRequest<?>> changeListener;
    protected transient BiConsumer<ClickHouseNode, ClickHouseNode> serverListener;

    // cache
    protected transient ClickHouseConfig config;
    protected transient List<String> statements;

    @SuppressWarnings("squid:S1905")
    protected ClickHouseRequest(ClickHouseClient client, Function<ClickHouseNodeSelector, ClickHouseNode> server,
            AtomicReference<ClickHouseNode> ref, Map<ClickHouseOption, Serializable> options, boolean sealed) {
        if (client == null || server == null) {
            throw new IllegalArgumentException("Non-null client and server are required");
        }

        this.client = client;

        this.managerRef = new AtomicReference<>(null);
        this.server = (Function<ClickHouseNodeSelector, ClickHouseNode> & Serializable) server;
        this.serverRef = ref == null ? new AtomicReference<>(null) : ref;
        this.txRef = new AtomicReference<>(null);
        this.sealed = sealed;

        this.externalTables = new LinkedList<>();
        this.options = options != null ? new HashMap<>(options) : new HashMap<>();
        this.settings = new LinkedHashMap<>(client.getConfig().getCustomSettings());

        this.namedParameters = new HashMap<>();
    }

    protected <T> T changeProperty(String property, T oldValue, T newValue) {
        if (changeListener != null && !Objects.equals(oldValue, newValue)) {
            changeListener.propertyChanged(this, property, oldValue, newValue);
        }
        return newValue;
    }

    protected ClickHouseNode changeServer(ClickHouseNode currentServer, ClickHouseNode newServer) {
        if (!serverRef.compareAndSet(currentServer, newServer)) {
            newServer = getServer();
        } else if (serverListener != null) {
            serverListener.accept(currentServer, newServer);
        }
        return newServer;
    }

    protected void checkSealed() {
        if (sealed) {
            throw new IllegalStateException("Sealed request is immutable");
        }
    }

    /**
     * Gets client associated with the request.
     *
     * @return non-null client
     */
    protected ClickHouseClient getClient() {
        return client != null ? client : ClickHouseClient.newInstance(getServer().getProtocol());
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
     * Creates a copy of this request including listeners. Please pay attention that
     * the same node reference (returned from {@link #getServer()}) will be copied
     * to the new request as well, meaning failover will change node for both
     * requests.
     *
     * @return copy of this request
     */
    public ClickHouseRequest<SelfT> copy() {
        ClickHouseRequest<SelfT> req = new ClickHouseRequest<>(getClient(), server, serverRef, options, false);
        req.externalTables.addAll(externalTables);
        req.settings.putAll(settings);
        req.namedParameters.putAll(namedParameters);
        req.input = input;
        req.output = output;
        req.queryId = queryId;
        req.sql = sql;
        req.preparedQuery = preparedQuery;
        req.managerRef.set(managerRef.get());
        req.txRef.set(txRef.get());
        req.changeListener = changeListener;
        req.serverListener = serverListener;
        return req;
    }

    /**
     * Gets manager for the request, which defaults to
     * {@link ClickHouseRequestManager#getInstance()}.
     *
     * @return non-null request manager
     */
    public ClickHouseRequestManager getManager() {
        ClickHouseRequestManager m = managerRef.get();
        if (m == null) {
            m = ClickHouseRequestManager.getInstance();
            if (!managerRef.compareAndSet(null, m)) {
                m = managerRef.get();
            }
        }
        return m;
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
     * Checks if the request is bounded with a transaction.
     *
     * @return true if the request is bounded with a transaction; false otherwise
     */
    public boolean isTransactional() {
        return txRef.get() != null;
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
     * Checks if the response should be redirected to an output stream, which was
     * defined by one of {@code output(*)} methods.
     *
     * @return true if response should be redirected to an output stream; false
     *         otherwise
     */
    public boolean hasOutputStream() {
        return this.output != null;
    }

    /**
     * Gets the server currently connected to. The initial value was determined by
     * the {@link java.util.function.Function} passed to constructor, and it may be
     * changed over time when failover is enabled.
     * 
     * @return non-null node
     */
    public final ClickHouseNode getServer() {
        ClickHouseNode node = serverRef.get();
        if (node == null) {
            node = server.apply(getConfig().getNodeSelector());
            if (!serverRef.compareAndSet(null, node)) {
                node = serverRef.get();
            }
        }
        return node;
    }

    /**
     * Gets request configuration.
     *
     * @return request configuration
     */
    public ClickHouseConfig getConfig() {
        if (config == null) {
            ClickHouseConfig clientConfig = getClient().getConfig();
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

    public ClickHouseTransaction getTransaction() {
        return txRef.get();
    }

    public final ClickHouseConfigChangeListener<ClickHouseRequest<?>> getChangeListener() {
        return this.changeListener;
    }

    public final BiConsumer<ClickHouseNode, ClickHouseNode> getServerListener() {
        return this.serverListener;
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
     * Gets output stream.
     *
     * @return output stream
     */
    public Optional<ClickHouseOutputStream> getOutputStream() {
        return output != null ? output.getOptional() : Optional.empty();
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
        return getFormat().defaultInputFormat();
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
    public Map<String, Serializable> getSettings() {
        return Collections.unmodifiableMap(settings);
    }

    /**
     * Gets session id.
     *
     * @return session id
     */
    public Optional<String> getSessionId() {
        String sessionId = getConfig().getStrOption(ClickHouseClientOption.SESSION_ID);
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
     * {@link ClickHouseClientOption#COMPRESS_ALGORITHM} and
     * {@link ClickHouseClientOption#COMPRESS_LEVEL} will be used.
     *
     * @param compressAlgorithm compression algorihtm, null or
     *                          {@link ClickHouseCompression#NONE} means no
     *                          compression
     * @return the request itself
     */
    public SelfT compressServerResponse(ClickHouseCompression compressAlgorithm) {
        return compressServerResponse(compressAlgorithm != null && compressAlgorithm != ClickHouseCompression.NONE,
                compressAlgorithm, (int) ClickHouseClientOption.COMPRESS_LEVEL.getEffectiveDefaultValue());
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

        return option(ClickHouseClientOption.COMPRESS, enable)
                .option(ClickHouseClientOption.COMPRESS_ALGORITHM, compressAlgorithm)
                .option(ClickHouseClientOption.COMPRESS_LEVEL, compressLevel);
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
     * @param compressAlgorithm compression algorithm, null is treated as
     *                          {@link ClickHouseCompression#NONE} or
     *                          {@link ClickHouseClientOption#DECOMPRESS_ALGORITHM}
     *                          depending on whether enabled
     * @return the request itself
     */
    public SelfT decompressClientRequest(ClickHouseCompression compressAlgorithm) {
        return decompressClientRequest(compressAlgorithm != null && compressAlgorithm != ClickHouseCompression.NONE,
                compressAlgorithm, (int) ClickHouseClientOption.DECOMPRESS_LEVEL.getEffectiveDefaultValue());
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

        return option(ClickHouseClientOption.DECOMPRESS, enable)
                .option(ClickHouseClientOption.DECOMPRESS_ALGORITHM, compressAlgorithm)
                .option(ClickHouseClientOption.DECOMPRESS_LEVEL, compressLevel);
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
    public SelfT format(ClickHouseFormat format) {
        checkSealed();
        return option(ClickHouseClientOption.FORMAT, format);
    }

    /**
     * Sets request manager which is responsible for generating query ID and session
     * ID, as well as transaction creation.
     *
     * @param manager request manager
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT manager(ClickHouseRequestManager manager) {
        checkSealed();
        ClickHouseRequestManager current = managerRef.get();
        if (!Objects.equals(current, manager) && managerRef.compareAndSet(current, manager)) {
            changeProperty(PROP_MANAGER, current, manager);
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
    public SelfT option(ClickHouseOption option, Serializable value) {
        checkSealed();

        if (value == null) {
            return removeOption(option);
        } else if (option != null && option.getValueType() != value.getClass()) {
            value = ClickHouseOption.fromString(value.toString(), option.getValueType());
        }

        Serializable oldValue = options.put(ClickHouseChecker.nonNull(option, ClickHouseConfig.PARAM_OPTION), value);
        if (oldValue == null || !oldValue.equals(value)) {
            if (changeListener != null) {
                changeListener.optionChanged(this, option, oldValue, value);
            }
            if (option == ClickHouseClientOption.CUSTOM_SETTINGS) {
                for (Entry<String, String> entry : ClickHouseOption.toKeyValuePairs(value.toString()).entrySet()) {
                    set(entry.getKey(), entry.getValue());
                }
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
                for (Entry<ClickHouseOption, Serializable> e : options.entrySet()) {
                    option(e.getKey(), e.getValue());
                }
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
     * Sets output file, to which response will be redirected.
     *
     * @param file non-null output file
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT output(ClickHouseFile file) {
        checkSealed();

        final int bufferSize = getConfig().getWriteBufferSize();
        if (ClickHouseChecker.nonNull(file, "File").hasFormat()) {
            format(file.getFormat());
        }
        compressServerResponse(file.isCompressed(), file.getCompressionAlgorithm());
        this.output = changeProperty(PROP_OUTPUT, this.output, ClickHouseDeferredValue
                .of(() -> ClickHouseOutputStream.of(file, bufferSize, null)));

        return (SelfT) this;
    }

    /**
     * Sets output file, to which response will be redirected.
     *
     * @param file non-empty path to the file
     * @return the request itself
     */
    public SelfT output(String file) {
        return output(file, ClickHouseCompression.fromFileName(file));
    }

    /**
     * Sets compressed output file, to which response will be redirected.
     *
     * @param file        non-empty path to the file
     * @param compression compression algorithm, {@code null} or
     *                    {@link ClickHouseCompression#NONE} means no compression
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT output(String file, ClickHouseCompression compression) {
        checkSealed();

        final int bufferSize = getConfig().getWriteBufferSize();
        final ClickHouseFile wrappedFile = ClickHouseFile.of(file, compression, 0, null);
        if (wrappedFile.hasFormat()) {
            format(wrappedFile.getFormat());
        }
        this.output = changeProperty(PROP_OUTPUT, this.output, ClickHouseDeferredValue
                .of(() -> ClickHouseOutputStream.of(wrappedFile, bufferSize, null)));
        return (SelfT) this;
    }

    /**
     * Sets output stream, to which response will be redirected.
     *
     * @param output non-null output stream
     * @return the request itself
     */
    public SelfT output(OutputStream output) {
        return output(ClickHouseOutputStream.of(output));
    }

    /**
     * Sets output stream, to which response will be redirected.
     *
     * @param output non-null output stream
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT output(ClickHouseOutputStream output) {
        checkSealed();

        this.output = changeProperty(PROP_OUTPUT, this.output,
                ClickHouseDeferredValue.of(output, ClickHouseOutputStream.class));

        return (SelfT) this;
    }

    /**
     * Sets output stream, to which response will be redirected.
     *
     * @param output non-null output stream
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT output(ClickHouseDeferredValue<ClickHouseOutputStream> output) {
        checkSealed();

        this.output = changeProperty(PROP_OUTPUT, this.output, output);

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
        if (size > 0) {
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
     * Sets all server settings.
     *
     * @param settings settings
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT settings(Map<String, Serializable> settings) {
        checkSealed();

        if (changeListener == null) {
            this.settings.clear();
            if (settings != null) {
                for (Entry<String, Serializable> e : settings.entrySet()) {
                    set(e.getKey(), e.getValue());
                }
            }
            resetCache();
        } else {
            Map<String, Serializable> m = new HashMap<>();
            m.putAll(this.settings);
            if (options != null) {
                for (Entry<String, Serializable> e : settings.entrySet()) {
                    set(e.getKey(), e.getValue());
                    m.remove(e.getKey());
                }
            }
            for (String s : m.keySet()) {
                removeSetting(s);
            }
        }

        return (SelfT) this;
    }

    /**
     * Clears session configuration including session id, session check(whether to
     * validate the id), and session timeout. Transaction will be removed as well.
     *
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT clearSession() {
        checkSealed();

        removeOption(ClickHouseClientOption.SESSION_ID);
        removeOption(ClickHouseClientOption.SESSION_CHECK);
        removeOption(ClickHouseClientOption.SESSION_TIMEOUT);

        // assume the transaction is managed somewhere else
        ClickHouseTransaction tx = txRef.get();
        if (tx != null && txRef.compareAndSet(tx, null)) {
            changeProperty(PROP_TRANSACTION, tx, null);
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
     * @param timeout   timeout in seconds
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
     * @param timeout   timeout in seconds
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT session(String sessionId, Boolean check, Integer timeout) {
        checkSealed();

        ClickHouseTransaction tx = txRef.get();
        if (tx != null) {
            throw new IllegalArgumentException(ClickHouseUtils.format(
                    "Please complete %s (or clear session) before changing session to (id=%s, check=%s, timeout=%s)",
                    tx, sessionId, check, timeout));
        } else {
            option(ClickHouseClientOption.SESSION_ID, sessionId);
            option(ClickHouseClientOption.SESSION_CHECK, check);
            option(ClickHouseClientOption.SESSION_TIMEOUT, timeout);
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

        if (SPECIAL_SETTINGS.contains(ClickHouseChecker.nonBlank(setting, PARAM_SETTING))) {
            return option(ClickHouseClientOption.fromKey(setting), value);
        } else if (value == null) {
            return removeSetting(setting);
        }

        Serializable oldValue = settings.put(setting, value);
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
     * Sets thread-safe change listener. Please keep in mind that the same listener
     * might be shared by multiple requests.
     *
     * @param listener thread-safe change listener which may or may not be null
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public final SelfT setChangeListener(ClickHouseConfigChangeListener<ClickHouseRequest<?>> listener) {
        this.changeListener = listener;
        return (SelfT) this;
    }

    /**
     * Sets thread-safe server change listener. Please keep in mind that the same
     * listener might be shared by multiple requests.
     *
     * @param listener thread-safe server listener which may or may not be null
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public final SelfT setServerListener(BiConsumer<ClickHouseNode, ClickHouseNode> listener) {
        this.serverListener = listener;
        return (SelfT) this;
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
        return query("SELECT * FROM ".concat(ClickHouseChecker.nonBlank(table, "table")), queryId);
    }

    /**
     * Creates and starts a transaction. Same as {@code transaction(0)}.
     *
     * @return the request itself
     * @throws ClickHouseException when failed to start or reuse transaction
     */
    public SelfT transaction() throws ClickHouseException {
        return transaction(0);
    }

    /**
     * Creates and starts a transaction immediately. Please pay attention that
     * unlike other methods in this class, it will connect to ClickHouse server,
     * allocate session and start transaction right away.
     *
     * @param timeout transaction timeout in seconds, zero or negative means
     *                same as session timeout
     * @return the request itself
     * @throws ClickHouseException when failed to start or reuse transaction
     */
    @SuppressWarnings("unchecked")
    public SelfT transaction(int timeout) throws ClickHouseException {
        ClickHouseTransaction tx = txRef.get();
        if (tx != null && tx.getTimeout() == (timeout > 0 ? timeout : 0)) {
            return (SelfT) this;
        }
        return transaction(getManager().getOrStartTransaction(this, timeout));
    }

    /**
     * Sets transaction. Any existing transaction, regardless its state, will be
     * replaced by the given one.
     *
     * @param transaction transaction
     * @return the request itself
     * @throws ClickHouseException when failed to set transaction
     */
    @SuppressWarnings("unchecked")
    public SelfT transaction(ClickHouseTransaction transaction) throws ClickHouseException {
        checkSealed();

        try {
            txRef.updateAndGet(x -> {
                if (changeProperty(PROP_TRANSACTION, x, transaction) != null) {
                    final ClickHouseNode currentServer = getServer();
                    final ClickHouseNode txServer = transaction.getServer();
                    // there's no global transaction and ReplicateMergeTree is not supported
                    if (!currentServer.isSameEndpoint(txServer) && changeServer(currentServer, txServer) != txServer) {
                        throw new IllegalStateException(ClickHouseUtils
                                .format("Failed to change current server from %s to %s", currentServer, txServer));
                    }
                    // skip the check in session method
                    option(ClickHouseClientOption.SESSION_ID, transaction.getSessionId());
                    option(ClickHouseClientOption.SESSION_CHECK, true);
                    option(ClickHouseClientOption.SESSION_TIMEOUT,
                            transaction.getTimeout() < 1 ? null : transaction.getTimeout());
                    removeSetting(ClickHouseTransaction.SETTING_IMPLICIT_TRANSACTION);
                } else if (x != null) {
                    clearSession();
                }
                return transaction;
            });
        } catch (IllegalStateException e) {
            throw ClickHouseException.of(e.getMessage(), getServer());
        }
        return (SelfT) this;
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

        Serializable oldValue = options.remove(ClickHouseChecker.nonNull(option, ClickHouseConfig.PARAM_OPTION));
        if (oldValue != null) {
            if (changeListener != null) {
                changeListener.optionChanged(this, option, oldValue, null);
            }

            if (option == ClickHouseClientOption.CUSTOM_SETTINGS) {
                for (String key : ClickHouseOption.toKeyValuePairs(oldValue.toString()).keySet()) {
                    removeSetting(key);
                    if (SPECIAL_SETTINGS.contains(key)) {
                        removeOption(ClickHouseClientOption.fromKey(key));
                    }
                }
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

        Serializable oldValue = settings.remove(ClickHouseChecker.nonBlank(setting, PARAM_SETTING));
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
        this.namedParameters.clear();

        this.serverListener = null;

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
        }
        this.input = changeProperty(PROP_DATA, this.input, null);
        this.output = changeProperty(PROP_OUTPUT, this.output, null);
        this.sql = changeProperty(PROP_QUERY, this.sql, null);
        this.preparedQuery = changeProperty(PROP_PREPARED_QUERY, this.preparedQuery, null);
        this.queryId = changeProperty(PROP_QUERY_ID, this.queryId, null);

        ClickHouseRequestManager current = managerRef.get();
        if (current != null && managerRef.compareAndSet(current, null)) {
            changeProperty(PROP_MANAGER, current, null);
        }
        ClickHouseTransaction tx = txRef.get();
        if (tx != null && txRef.compareAndSet(tx, null)) {
            changeProperty(PROP_TRANSACTION, tx, null);
        }

        this.changeListener = null;

        resetCache();

        return (SelfT) this;
    }

    /**
     * Creates a sealed request, which is an immutable copy of the current request.
     * Listeners won't be copied to the sealed instance, because it's immutable.
     *
     * @return sealed request, an immutable copy of the current request
     */
    public ClickHouseRequest<SelfT> seal() {
        ClickHouseRequest<SelfT> req = this;

        if (!isSealed()) {
            // no idea which node we'll connect to until now
            req = new ClickHouseRequest<>(client, getServer(), serverRef, options, true);
            req.externalTables.addAll(externalTables);
            req.settings.putAll(settings);

            req.namedParameters.putAll(namedParameters);

            req.input = input;
            req.output = output;
            req.queryId = queryId;
            req.sql = sql;
            req.preparedQuery = preparedQuery;
            req.managerRef.set(managerRef.get());
            req.txRef.set(txRef.get());
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
        return getClient().execute(this);
    }

    /**
     * Synchronous version of {@link #execute()}.
     *
     * @return non-null response
     * @throws ClickHouseException when error occurred during execution
     */
    public ClickHouseResponse executeAndWait() throws ClickHouseException {
        return getClient().executeAndWait(this);
    }

    /**
     * Executes the request within an implicit transaction. New transaction will be
     * always created and started right before the query, and it will be committed
     * or rolled back afterwards automatically.
     * 
     * @return non-null response
     * @throws ClickHouseException when error occurred during execution
     */
    public ClickHouseResponse executeWithinTransaction() throws ClickHouseException {
        return executeWithinTransaction(false);
    }

    /**
     * Executes the request within an implicit transaction. When
     * {@code useImplicitTransaction} is set to {@code true}, it enforces the client
     * to use {@code implicit_transaction} setting which is only available in
     * ClickHouse 22.7+. Otherwise, new transaction will be always created and
     * started right before the query, and it will be committed or rolled back
     * afterwards automatically.
     *
     * @param useImplicitTransaction {@code true} to use native implicit transaction
     *                               requiring ClickHouse 22.7+ with minimum
     *                               overhead(no session on server side and no
     *                               additional objects on client side); false to
     *                               use auto-commit transaction
     * @return non-null response
     * @throws ClickHouseException when error occurred during execution
     * @deprecated will be removed in the future, once the minimum supported version
     *             of ClickHouse is 22.7 or above
     */
    @Deprecated
    public ClickHouseResponse executeWithinTransaction(boolean useImplicitTransaction) throws ClickHouseException {
        if (useImplicitTransaction) {
            return set(ClickHouseTransaction.SETTING_IMPLICIT_TRANSACTION, 1).transaction(null).executeAndWait();
        }

        ClickHouseTransaction tx = null;
        try {
            tx = getManager().createImplicitTransaction(this);
            // transaction will be committed only when the response is fully consumed
            return getClient().executeAndWait(transaction(tx));
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception ex) {
                    // ignore
                }
            }
            throw ClickHouseException.of(e, getServer());
        } finally {
            transaction(null);
        }
    }
}
