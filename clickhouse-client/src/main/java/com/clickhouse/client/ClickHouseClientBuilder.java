package com.clickhouse.client;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import com.clickhouse.client.config.ClickHouseOption;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;
import com.clickhouse.client.ClickHouseNode.Status;
import com.clickhouse.client.config.ClickHouseDefaults;

/**
 * Builder class for creating {@link ClickHouseClient}. Please use
 * {@link ClickHouseClient#builder()} for instantiation, and avoid
 * multi-threading as it's NOT thread-safe.
 */
public class ClickHouseClientBuilder {
    /**
     * Dummy client which is only used {@link Agent}.
     */
    static class DummyClient implements ClickHouseClient {
        static final ClickHouseConfig CONFIG = new ClickHouseConfig();
        static final DummyClient INSTANCE = new DummyClient();

        @Override
        public boolean accept(ClickHouseProtocol protocol) {
            return true;
        }

        @Override
        public CompletableFuture<ClickHouseResponse> execute(ClickHouseRequest<?> request) {
            return CompletableFuture.completedFuture(ClickHouseResponse.EMPTY);
        }

        @Override
        public ClickHouseConfig getConfig() {
            return CONFIG;
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public boolean ping(ClickHouseNode server, int timeout) {
            return true;
        }
    }

    /**
     * Thread-safe wrapper of {@link ClickHouseClient} for collecting metrics and
     * fail-over.
     */
    static final class Agent implements ClickHouseClient {
        private static final Logger log = LoggerFactory.getLogger(Agent.class);

        private final AtomicReference<ClickHouseClient> client;

        Agent(ClickHouseClient client) {
            this.client = new AtomicReference<>(client != null ? client : DummyClient.INSTANCE);
        }

        ClickHouseClient getClient() {
            return client.get();
        }

        ClickHouseResponse failover(ClickHouseRequest<?> sealedRequest, Throwable cause, int times) {
            for (int i = 1; i <= times; i++) {
                log.debug("Failover %d of %d due to: %s", i, times, cause.getMessage());
                ClickHouseNode current = sealedRequest.getServer();
                ClickHouseNodeManager manager = current.manager.get();
                if (manager == null) {
                    break;
                }
                ClickHouseNode next = manager.suggestNode(current, cause);
                if (next == current) {
                    break;
                }
                current.update(Status.FAULTY);
                next = sealedRequest.changeServer(current, next);
                if (next == current) {
                    break;
                }

                log.info("Switching to %s due to connection issue with %s", next, current);
                final ClickHouseProtocol protocol = next.getProtocol();
                client.getAndUpdate(c -> {
                    if (!c.accept(protocol)) {
                        c.close();
                        return ClickHouseClient.newInstance(protocol);
                    }
                    return c;
                });
                try {
                    return sendOnce(sealedRequest);
                } catch (Exception exp) {
                    cause = exp.getCause();
                    if (cause == null) {
                        cause = exp;
                    }
                }
            }

            throw new CompletionException(cause);
        }

        ClickHouseResponse retry(ClickHouseRequest<?> sealedRequest, Throwable cause, int times) {
            for (int i = 1; i <= times; i++) {
                log.debug("Retry %d of %d due to: %s", i, times, cause.getMessage());
                // TODO retry idempotent query
                if (cause instanceof ClickHouseException
                        && ((ClickHouseException) cause).getErrorCode() == ClickHouseException.ERROR_NETWORK) {
                    log.info("Retry request on %s due to connection issue", sealedRequest.getServer());
                    try {
                        return sendOnce(sealedRequest);
                    } catch (Exception exp) {
                        cause = exp.getCause();
                        if (cause == null) {
                            cause = exp;
                        }
                    }
                }
            }

            throw new CompletionException(cause);
        }

        ClickHouseResponse handle(ClickHouseRequest<?> sealedRequest, Throwable cause) {
            try {
                int times = sealedRequest.getConfig().getFailover();
                if (times > 0) {
                    return failover(sealedRequest, cause, times);
                }

                // different from failover: 1) retry on the same node; 2) never retry on timeout
                times = sealedRequest.getConfig().getRetry();
                if (times > 0) {
                    return retry(sealedRequest, cause, times);
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
                return getClient().execute(sealedRequest).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CancellationException("Execution was interrupted");
            } catch (ExecutionException e) {
                throw new CompletionException(e.getCause());
            }
        }

        ClickHouseResponse send(ClickHouseRequest<?> sealedRequest) {
            try {
                return sendOnce(sealedRequest);
            } catch (CompletionException e) {
                return handle(sealedRequest, e.getCause());
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
            return sealedRequest.getConfig().isAsync()
                    ? getClient().execute(sealedRequest)
                            .handle((r, t) -> t == null ? r : handle(sealedRequest, t.getCause()))
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

    // expose method to change default thread pool in runtime? JMX?
    static final ExecutorService defaultExecutor;
    static final ScheduledExecutorService defaultScheduler;

    static {
        int maxSchedulers = (int) ClickHouseDefaults.MAX_SCHEDULER_THREADS.getEffectiveDefaultValue();
        int maxThreads = (int) ClickHouseDefaults.MAX_THREADS.getEffectiveDefaultValue();
        int maxRequests = (int) ClickHouseDefaults.MAX_REQUESTS.getEffectiveDefaultValue();
        long keepAliveTimeoutMs = (long) ClickHouseDefaults.THREAD_KEEPALIVE_TIMEOUT.getEffectiveDefaultValue();

        if (maxThreads <= 0) {
            maxThreads = Runtime.getRuntime().availableProcessors();
        }
        if (maxSchedulers <= 0) {
            maxSchedulers = Runtime.getRuntime().availableProcessors();
        } else if (maxSchedulers > maxThreads) {
            maxSchedulers = maxThreads;
        }

        if (maxRequests <= 0) {
            maxRequests = 0;
        }

        String prefix = "ClickHouseClientWorker";
        defaultExecutor = ClickHouseUtils.newThreadPool(prefix, maxThreads, maxThreads * 2, maxRequests,
                keepAliveTimeoutMs, false);
        prefix = "ClickHouseClientScheduler";
        defaultScheduler = maxSchedulers == 1 ? Executors
                .newSingleThreadScheduledExecutor(new ClickHouseThreadFactory(prefix))
                : Executors.newScheduledThreadPool(maxSchedulers, new ClickHouseThreadFactory(prefix));
    }

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

        boolean noSelector = nodeSelector == null || nodeSelector == ClickHouseNodeSelector.EMPTY;
        int counter = 0;
        ClickHouseConfig conf = getConfig();
        for (ClickHouseClient c : loadClients()) {
            c.init(conf);

            counter++;
            if (noSelector || nodeSelector.match(c)) {
                client = c;
                break;
            }
        }

        if (client == null) {
            throw new IllegalStateException(
                    ClickHouseUtils.format("No suitable ClickHouse client(out of %d) found in classpath.", counter));
        }

        return agent ? new Agent(client) : client;
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
     * {@link com.clickhouse.client.config.ClickHouseOption}.
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
            this.nodeSelector = nodeSelector;
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
