package com.clickhouse.client;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;

import com.clickhouse.client.config.ClickHouseOption;
import com.clickhouse.client.config.ClickHouseDefaults;

/**
 * Builder class for creating {@link ClickHouseClient}. Please use
 * {@link ClickHouseClient#builder()} for instantiation, and avoid
 * multi-threading as it's NOT thread-safe.
 */
public class ClickHouseClientBuilder {
    // expose method to change default thread pool in runtime? JMX?
    static final ExecutorService defaultExecutor;

    static {
        int maxThreads = (int) ClickHouseDefaults.MAX_THREADS.getEffectiveDefaultValue();
        int maxRequests = (int) ClickHouseDefaults.MAX_REQUESTS.getEffectiveDefaultValue();
        long keepAliveTimeoutMs = (long) ClickHouseDefaults.THREAD_KEEPALIVE_TIMEOUT.getEffectiveDefaultValue();

        if (maxThreads <= 0) {
            maxThreads = Runtime.getRuntime().availableProcessors();
        }
        if (maxRequests <= 0) {
            maxRequests = 0;
        }

        defaultExecutor = ClickHouseUtils.newThreadPool(ClickHouseClient.class.getSimpleName(), maxThreads,
                maxThreads * 2, maxRequests, keepAliveTimeoutMs);
    }

    protected ClickHouseConfig config;

    protected ClickHouseCredentials credentials;
    protected Object metricRegistry;
    protected ClickHouseNodeSelector nodeSelector;

    protected final Map<ClickHouseOption, Serializable> options;

    /**
     * Default constructor.
     */
    protected ClickHouseClientBuilder() {
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
        for (ClickHouseClient c : ServiceLoader.load(ClickHouseClient.class, getClass().getClassLoader())) {
            counter++;
            if (noSelector || nodeSelector.match(c)) {
                client = c;
                break;
            }
        }

        if (client == null) {
            throw new IllegalStateException(
                    ClickHouseUtils.format("No suitable ClickHouse client(out of %d) found in classpath.", counter));
        } else {
            client.init(getConfig());
        }

        return client;
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
        if (!ClickHouseChecker.nonNull(credentials, "credentials").equals(this.credentials)) {
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
