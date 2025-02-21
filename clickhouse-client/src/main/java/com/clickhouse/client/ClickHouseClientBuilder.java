package com.clickhouse.client;

import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.clickhouse.client.ClickHouseNode.Status;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

/**
 * Builder class for creating {@link ClickHouseClient}. Please use
 * {@link ClickHouseClient#builder()} for instantiation, and avoid
 * multi-threading as it's NOT thread-safe.
 */
@Deprecated
public class ClickHouseClientBuilder {
    /**
     * Dummy client which is only used by {@link Agent}.
     */
    static class DummyClient implements ClickHouseClient {
        static final ClickHouseConfig DEFAULT_CONFIG = new ClickHouseConfig();

        private final ClickHouseConfig config;

        DummyClient() {
            this(null);
        }

        DummyClient(ClickHouseConfig config) {
            this.config = config != null ? config : DEFAULT_CONFIG;
        }

        @Override
        public boolean accept(ClickHouseProtocol protocol) {
            return false;
        }

        @Override
        public CompletableFuture<ClickHouseResponse> execute(ClickHouseRequest<?> request) {
            CompletableFuture<ClickHouseResponse> future = new CompletableFuture<>();
            future.completeExceptionally(new ConnectException("No client available"));
            return future;
        }

        @Override
        public ClickHouseConfig getConfig() {
            return config;
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public boolean ping(ClickHouseNode server, int timeout) {
            return false;
        }
    }

    /**
     * Thread-safe wrapper of {@link ClickHouseClient} for collecting metrics and
     * fail-over.
     */
    static final class Agent implements ClickHouseClient {
        private static final Logger log = LoggerFactory.getLogger(Agent.class);

        private static final long INITIAL_REPEAT_DELAY = 100L;
        private static final long MAX_REPEAT_DELAY = 1000L;
        private static final long REPEAT_DELAY_BACKOFF = 100L;

        private final AtomicReference<ClickHouseClient> client;

        Agent(ClickHouseClient client, ClickHouseConfig config) {
            this.client = new AtomicReference<>(client != null ? client : new DummyClient(config));
        }

        ClickHouseClient getClient() {
            return client.get();
        }

        boolean changeClient(ClickHouseClient currentClient, ClickHouseClient newClient) {
            final boolean changed = client.compareAndSet(currentClient, newClient);
            try {
                if (changed) {
                    currentClient.close();
                } else {
                    newClient.close();
                }
            } catch (Exception e) {
                // ignore
            }
            return changed;
        }

        ClickHouseResponse failover(ClickHouseRequest<?> sealedRequest, ClickHouseException exception, int times) {
            for (int i = 1; i <= times; i++) {
                log.debug("Failover %d of %d due to: %s", i, times, exception.getCause(), null);
                ClickHouseNode current = sealedRequest.getServer();
                ClickHouseNodeManager manager = current.manager.get();
                if (manager == null) {
                    log.debug("Cancel failover for unmanaged node: %s", current);
                    break;
                }
                ClickHouseNode next = manager.suggestNode(current, exception);
                if (next == current) {
                    log.debug("Cancel failover for same node returned from %s", manager.getPolicy());
                    break;
                }
                current.update(Status.FAULTY);
                if (sealedRequest.isTransactional()) {
                    log.debug("Cancel failover for transactional context: %s", sealedRequest.getTransaction());
                    break;
                } else if ((next = sealedRequest.changeServer(current, next)) == current) {
                    log.debug("Cancel failover for no alternative of %s", current);
                    break;
                }

                log.info("Switching node from %s to %s due to: %s", current, next, exception.getCause(), null);
                final ClickHouseProtocol protocol = next.getProtocol();
                final ClickHouseClient currentClient = client.get();
                if (!currentClient.accept(protocol)) {
                    ClickHouseClient newClient = null;
                    try {
                        newClient = ClickHouseClient.builder().agent(false)
                                .config(new ClickHouseConfig(currentClient.getConfig(), next.config))
                                .nodeSelector(ClickHouseNodeSelector.of(protocol)).build();
                    } catch (Exception e) {
                        exception = ClickHouseException.of(new ConnectException("No client available for " + next),
                                sealedRequest.getServer());
                    } finally {
                        if (newClient != null) {
                            boolean changed = changeClient(currentClient, newClient);
                            log.info("Switching client from %s to %s: %s", currentClient, newClient, changed);
                            if (changed) {
                                sealedRequest.resetCache();
                            }
                        }
                    }

                    if (newClient == null) {
                        continue;
                    }
                }

                try {
                    return sendOnce(sealedRequest);
                } catch (Exception exp) {
                    exception = ClickHouseException.of(exp.getCause() != null ? exp.getCause() : exp,
                            sealedRequest.getServer());
                }
            }

            throw new CompletionException(exception);
        }

        /**
         * Repeats sending same request until success, timed out or running into a
         * different error.
         *
         * @param sealedRequest non-null sealed request
         * @param exception     non-null exception to start with
         * @param timeout       timeout in milliseconds, zero or negative numbers means
         *                      no repeat
         * @return non-null response
         * @throws CompletionException when error occurred or timed out
         */
        ClickHouseResponse repeat(ClickHouseRequest<?> sealedRequest, ClickHouseException exception, long timeout) {
            if (timeout > 0L) {
                final int errorCode = exception.getErrorCode();
                final long startTime = System.currentTimeMillis();

                long delay = INITIAL_REPEAT_DELAY;
                long elapsed = 0L;
                int count = 1;
                while (true) {
                    log.info("Repeating #%d (delay=%d, elapsed=%d, timeout=%d) due to: %s", count++, delay, elapsed,
                            timeout, exception.getMessage());
                    try {
                        return sendOnce(sealedRequest);
                    } catch (Exception exp) {
                        exception = ClickHouseException.of(exp.getCause() != null ? exp.getCause() : exp,
                                sealedRequest.getServer());
                    }

                    elapsed = System.currentTimeMillis() - startTime;
                    if (exception.getErrorCode() != errorCode || elapsed + delay >= timeout) {
                        log.warn("Stopped repeating(delay=%d, elapsed=%d, timeout=%d) for %s", delay, elapsed,
                                timeout, exception.getMessage());
                        break;
                    }

                    try {
                        Thread.sleep(delay);
                        elapsed += delay;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    if (delay >= MAX_REPEAT_DELAY) {
                        delay = MAX_REPEAT_DELAY;
                    } else {
                        delay += REPEAT_DELAY_BACKOFF;
                    }
                }
            }
            throw new CompletionException(exception);
        }

        ClickHouseResponse retry(ClickHouseRequest<?> sealedRequest, ClickHouseException exception, int times) {
            for (int i = 1; i <= times; i++) {
                log.debug("Retry %d of %d due to: %s", i, times, exception.getMessage());
                // TODO retry idempotent query
                if (exception.getErrorCode() == ClickHouseException.ERROR_NETWORK) {
                    log.info("Retry request on %s due to connection issue", sealedRequest.getServer());
                    try {
                        return sendOnce(sealedRequest);
                    } catch (Exception exp) {
                        exception = ClickHouseException.of(exp.getCause() != null ? exp.getCause() : exp,
                                sealedRequest.getServer());
                    }
                }
            }

            throw new CompletionException(exception);
        }

        ClickHouseResponse handle(ClickHouseRequest<?> sealedRequest, Throwable cause) {
            // in case there's any recoverable exception wrapped by UncheckedIOException
            if (cause instanceof UncheckedIOException && cause.getCause() != null) {
                cause = ((UncheckedIOException) cause).getCause();
            }

            ClickHouseConfig config = sealedRequest.getConfig();
            log.debug("Handling %s(failover=%d, retry=%d)", cause, config.getFailover(), config.getRetry());
            ClickHouseException ex = ClickHouseException.of(cause, sealedRequest.getServer());
            try {
                if (config.isRepeatOnSessionLock()
                        && ex.getErrorCode() == ClickHouseException.ERROR_SESSION_IS_LOCKED) {
                    // connection timeout is usually a small number(defaults to 5000 ms), making it
                    // better default compare to socket timeout and max execution time etc.
                    return repeat(sealedRequest, ex, config.getSessionTimeout() <= 0 ? config.getConnectionTimeout()
                            : TimeUnit.SECONDS.toMillis(config.getSessionTimeout()));
                }

                int times = sealedRequest.getConfig().getFailover();
                if (times > 0) {
                    return failover(sealedRequest, ex, times);
                }

                // different from failover: 1) retry on the same node; 2) never retry on timeout
                times = sealedRequest.getConfig().getRetry();
                if (times > 0) {
                    return retry(sealedRequest, ex, times);
                }

                throw new CompletionException(cause);
            } catch (CompletionException e) {
                throw e;
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }

        ClickHouseResponse sendOnce(ClickHouseRequest<?> sealedRequest) {
            try {
                return getClient().execute(sealedRequest).get(sealedRequest.getConfig().getSocketTimeout(),
                        TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CancellationException("Execution was interrupted");
            } catch (ExecutionException | TimeoutException e) {
                throw new CompletionException(e.getCause());
            }
        }

        ClickHouseResponse send(ClickHouseRequest<?> sealedRequest) {
            try {
                return sendOnce(sealedRequest);
            } catch (Exception e) {
                return handle(sealedRequest, e.getCause() != null ? e.getCause() : e);
            }
        }

        @Override
        public boolean accept(ClickHouseProtocol protocol) {
            return client.get().accept(protocol);
        }

        @Override
        public Class<? extends ClickHouseOption> getOptionClass() {
            return client.get().getOptionClass();
        }

        @Override
        public void init(ClickHouseConfig config) {
            client.get().init(config);
        }

        @Override
        public boolean ping(ClickHouseNode server, int timeout) {
            return client.get().ping(server, timeout);
        }

        @Override
        public CompletableFuture<ClickHouseResponse> execute(ClickHouseRequest<?> request) {
            final ClickHouseRequest<?> sealedRequest = request.seal();
            final ClickHouseNode server = sealedRequest.getServer();
            final ClickHouseProtocol protocol = server.getProtocol();
            final ClickHouseClient currentClient = client.get();
            if (!currentClient.accept(protocol)) {
                ClickHouseClient newClient = null;
                try {
                    newClient = ClickHouseClient.builder().agent(false)
                            .config(new ClickHouseConfig(currentClient.getConfig(), server.config))
                            .nodeSelector(ClickHouseNodeSelector.of(protocol)).build();
                } catch (IllegalStateException e) {
                    // let it fail on execution phase
                    log.debug("Failed to find client for %s", server);
                } finally {
                    if (newClient != null) {
                        boolean changed = changeClient(currentClient, newClient);
                        log.debug("Switching client from %s to %s: %s", currentClient, newClient, changed);
                        if (changed) {
                            sealedRequest.resetCache();
                        }
                    }
                }
            }
            return sealedRequest.getConfig().isAsync()
                    ? getClient().execute(sealedRequest)
                            .handle((r, t) -> t == null ? r
                                    : handle(sealedRequest, t.getCause() != null ? t.getCause() : t))
                    : CompletableFuture.completedFuture(send(sealedRequest));
        }

        @Override
        public ClickHouseConfig getConfig() {
            return client.get().getConfig();
        }

        @Override
        public void close() {
            client.get().close();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ClickHouseClientBuilder.class);

    static ServiceLoader<ClickHouseClient> loadClients() {
        return ServiceLoader.load(ClickHouseClient.class, ClickHouseClientBuilder.class.getClassLoader());
    }

    protected boolean agent;
    protected ClickHouseConfig config;

    protected ClickHouseCredentials credentials;
    protected Object metricRegistry;
    protected ClickHouseNodeSelector nodeSelector;

    protected final Map<ClickHouseOption, Serializable> options;

    /**
     * Default constructor.
     */
    protected ClickHouseClientBuilder() {
        agent = true;
        config = null;
        metricRegistry = null;
        nodeSelector = null;
        options = new HashMap<>();
    }

    /**
     * Resets client configuration to null.
     */
    protected void resetConfig() {
        if (config != null) {
            config = null;
        }
    }

    /**
     * Gets client configuration.
     *
     * @return non-null client configuration
     */
    public ClickHouseConfig getConfig() {
        if (config == null) {
            config = new ClickHouseConfig(options, credentials, nodeSelector, metricRegistry);
        }

        return config;
    }

    /**
     * Builds an instance of {@link ClickHouseClient}. This method will use
     * {@link java.util.ServiceLoader} to load a suitable implementation based on
     * preferred protocol(s), or just the first one if no preference given.
     * {@link ClickHouseClient#accept(ClickHouseProtocol)} will be invoked during
     * the process to test if the implementation is compatible with the preferred
     * protocol(s) or not. At the end of process, if a suitable implementation is
     * found, {@link ClickHouseClient#init(ClickHouseConfig)} will be invoked for
     * initialization.
     *
     * @return suitable client to handle preferred protocols
     * @throws IllegalStateException when no suitable client found in classpath
     */
    public ClickHouseClient build() {
        ClickHouseClient client = null;

        ClickHouseConfig conf = getConfig();
        int counter = 0;
        if (nodeSelector != null) {
            Throwable lastError = null;
            for (ClickHouseClient c : loadClients()) {
                try {
                    c.init(conf);
                    counter++;
                    if (nodeSelector == ClickHouseNodeSelector.EMPTY || nodeSelector.match(c)) {
                        client = c;
                        break;
                    }
                } catch (UnsupportedProtocolException e) {
                    if (nodeSelector.matchAnyOfPreferredProtocols(e.getProtocol())) {
                        lastError = e;
                    }
                } catch (Throwable e) {
                    log.warn("Skip client due to exception: " + e.getMessage(), e);
                }
            }

            if (client == null && lastError != null) {
                throw new ExceptionInInitializerError(lastError.getMessage());
            }
        }

        if (agent) {
            return new Agent(client, conf);
        } else if (client == null) {
            throw new IllegalStateException(
                    ClickHouseUtils.format("No suitable ClickHouse client(out of %d) found in classpath for %s.",
                            counter, nodeSelector));
        }
        return client;
    }

    /**
     * Sets whether agent should be used for advanced feature like failover and
     * retry.
     *
     * @param agent whether to use agent
     * @return this builder
     */
    public ClickHouseClientBuilder agent(boolean agent) {
        this.agent = agent;
        return this;
    }

    /**
     * Sets configuration.
     *
     * @param config non-null configuration
     * @return this builder
     */
    public ClickHouseClientBuilder config(ClickHouseConfig config) {
        this.config = config;

        this.credentials = config.getDefaultCredentials();
        this.metricRegistry = config.getMetricRegistry().orElse(null);
        this.nodeSelector = config.getNodeSelector();

        this.options.putAll(config.getAllOptions());

        return this;
    }

    /**
     * Adds an option, which is usually an Enum type that implements
     * {@link com.clickhouse.config.ClickHouseOption}.
     *
     * @param option non-null option
     * @param value  value
     * @return this builder
     */
    public ClickHouseClientBuilder option(ClickHouseOption option, Serializable value) {
        if (option == null || value == null) {
            throw new IllegalArgumentException("Non-null option and value are required");
        }
        Object oldValue = options.put(option, value);
        if (oldValue == null || !value.equals(oldValue)) {
            resetConfig();
        }

        return this;
    }

    /**
     * Removes an option.
     *
     * @param option non-null option
     * @return this builder
     */
    public ClickHouseClientBuilder removeOption(ClickHouseOption option) {
        Object value = options.remove(ClickHouseChecker.nonNull(option, "option"));
        if (value != null) {
            resetConfig();
        }

        return this;
    }

    /**
     * Removes all options.
     *
     * @return this builder
     */
    public ClickHouseClientBuilder clearOptions() {
        options.clear();
        resetConfig();

        return this;
    }

    /**
     * Sets options.
     *
     * @param options map containing all options
     * @return this builder
     */
    public ClickHouseClientBuilder options(Map<ClickHouseOption, Serializable> options) {
        if (options != null && !options.isEmpty()) {
            this.options.putAll(options);
            resetConfig();
        }

        return this;
    }

    /*
     * public ClickHouseClientBuilder addUserType(Object... userTypeMappers) {
     * resetConfig(); return this; }
     */

    /**
     * Sets default credentials, which will be used to connect to a
     * {@link ClickHouseNode} only when it has no credentials defined.
     *
     * @param credentials default credentials
     * @return this builder
     */
    public ClickHouseClientBuilder defaultCredentials(ClickHouseCredentials credentials) {
        if (!Objects.equals(this.credentials, credentials)) {
            this.credentials = credentials;
            resetConfig();
        }

        return this;
    }

    /*
     * public ClickHouseClientBuilder databaseChangeListener(@NonNull Object
     * listener) { resetConfig(); return this; }
     */

    /**
     * Sets node selector.
     *
     * @param nodeSelector non-null node selector
     * @return this builder
     */
    public ClickHouseClientBuilder nodeSelector(ClickHouseNodeSelector nodeSelector) {
        if (!ClickHouseChecker.nonNull(nodeSelector, "nodeSelector").equals(this.nodeSelector)) {
            this.nodeSelector = (nodeSelector.getPreferredProtocols().isEmpty() || nodeSelector.getPreferredProtocols()
                    .equals(Collections.singletonList(ClickHouseProtocol.ANY)))
                    && nodeSelector.getPreferredTags().isEmpty()
                            ? ClickHouseNodeSelector.EMPTY
                            : nodeSelector;
            resetConfig();
        }

        return this;
    }

    /**
     * Sets metric registry.
     *
     * @param metricRegistry metric registry, could be null
     * @return this builder
     */
    public ClickHouseClientBuilder metricRegistry(Object metricRegistry) {
        if (!Objects.equals(this.metricRegistry, metricRegistry)) {
            this.metricRegistry = metricRegistry;
            resetConfig();
        }

        return this;
    }
}
