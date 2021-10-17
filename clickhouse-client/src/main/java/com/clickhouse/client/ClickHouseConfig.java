package com.clickhouse.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseConfigOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.config.ClickHouseSslMode;

/**
 * An immutable class holding client-specific options like
 * {@link ClickHouseCredentials} and {@link ClickHouseNodeSelector} etc.
 */
public class ClickHouseConfig implements Serializable {
    protected static final Map<ClickHouseConfigOption, Serializable> mergeOptions(List<ClickHouseConfig> list) {
        if (list == null || list.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<ClickHouseConfigOption, Serializable> options = new HashMap<>();
        List<ClickHouseConfig> cl = new ArrayList<>(list.size());
        for (ClickHouseConfig c : list) {
            if (c != null) {
                boolean duplicated = false;
                for (ClickHouseConfig conf : cl) {
                    if (conf == c) {
                        duplicated = true;
                        break;
                    }
                }

                if (duplicated) {
                    continue;
                }
                options.putAll(c.options);
                cl.add(c);
            }
        }

        return options;
    }

    protected static final ClickHouseCredentials mergeCredentials(List<ClickHouseConfig> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }

        ClickHouseCredentials credentials = null;
        for (ClickHouseConfig c : list) {
            if (c != null && c.credentials != null) {
                credentials = c.credentials;
                break;
            }
        }

        return credentials;
    }

    protected static final ClickHouseNodeSelector mergeNodeSelector(List<ClickHouseConfig> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }

        ClickHouseNodeSelector nodeSelector = null;
        for (ClickHouseConfig c : list) {
            if (c != null && c.nodeSelector != null) {
                nodeSelector = c.nodeSelector;
                break;
            }
        }

        return nodeSelector;
    }

    protected static final Object mergeMetricRegistry(List<ClickHouseConfig> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }

        Object metricRegistry = null;
        for (ClickHouseConfig c : list) {
            if (c != null && c.metricRegistry.isPresent()) {
                metricRegistry = c.metricRegistry.get();
                break;
            }
        }

        return metricRegistry;
    }

    private static final long serialVersionUID = 7794222888859182491L;

    // common options optimized for read
    private final boolean async;
    private final String clientName;
    private final ClickHouseCompression compression;
    private final int connectionTimeout;
    private final String database;
    private final ClickHouseFormat format;
    private final int maxBufferSize;
    private final int maxExecutionTime;
    private final int maxQueuedBuffers;
    private final int maxQueuedRequests;
    private final int maxResultRows;
    private final int maxThreads;
    private final boolean retry;
    private final boolean reuseValueWrapper;
    private final int socketTimeout;
    private final int sessionTimeout;
    private final boolean sessionCheck;
    private final boolean ssl;
    private final ClickHouseSslMode sslMode;
    private final String sslRootCert;
    private final String sslCert;
    private final String sslKey;
    private final boolean useObjectsInArray;
    private final boolean useServerTimeZone;
    private final String useTimeZone;
    private final boolean useServerTimeZoneForDate;

    // client specific options
    private final Map<ClickHouseConfigOption, Serializable> options;
    private final ClickHouseCredentials credentials;
    private final transient Optional<Object> metricRegistry;

    // node selector - pick only interested nodes from given list
    private final ClickHouseNodeSelector nodeSelector;

    /**
     * Construct a new configuration by consolidating given ones.
     *
     * @param configs list of configuration
     */
    public ClickHouseConfig(ClickHouseConfig... configs) {
        this(configs == null || configs.length == 0 ? Collections.emptyList() : Arrays.asList(configs));
    }

    /**
     * Construct a new configuration by consolidating given ones.
     *
     * @param configs list of configuration
     */
    public ClickHouseConfig(List<ClickHouseConfig> configs) {
        this(mergeOptions(configs), mergeCredentials(configs), mergeNodeSelector(configs),
                mergeMetricRegistry(configs));
    }

    /**
     * Default contructor.
     *
     * @param options        generic options
     * @param credentials    default credential
     * @param nodeSelector   node selector
     * @param metricRegistry metric registry
     */
    public ClickHouseConfig(Map<ClickHouseConfigOption, Serializable> options, ClickHouseCredentials credentials,
            ClickHouseNodeSelector nodeSelector, Object metricRegistry) {
        this.options = new HashMap<>();
        if (options != null) {
            this.options.putAll(options);
        }

        this.async = (boolean) getOption(ClickHouseClientOption.ASYNC, ClickHouseDefaults.ASYNC);
        this.clientName = (String) getOption(ClickHouseClientOption.CLIENT_NAME);
        this.compression = (ClickHouseCompression) getOption(ClickHouseClientOption.COMPRESSION,
                ClickHouseDefaults.COMPRESSION);
        this.connectionTimeout = (int) getOption(ClickHouseClientOption.CONNECTION_TIMEOUT);
        this.database = (String) getOption(ClickHouseClientOption.DATABASE, ClickHouseDefaults.DATABASE);
        this.format = (ClickHouseFormat) getOption(ClickHouseClientOption.FORMAT, ClickHouseDefaults.FORMAT);
        this.maxBufferSize = (int) getOption(ClickHouseClientOption.MAX_BUFFER_SIZE);
        this.maxExecutionTime = (int) getOption(ClickHouseClientOption.MAX_EXECUTION_TIME);
        this.maxQueuedBuffers = (int) getOption(ClickHouseClientOption.MAX_QUEUED_BUFFERS);
        this.maxQueuedRequests = (int) getOption(ClickHouseClientOption.MAX_QUEUED_REQUESTS);
        this.maxResultRows = (int) getOption(ClickHouseClientOption.MAX_RESULT_ROWS);
        this.maxThreads = (int) getOption(ClickHouseClientOption.MAX_THREADS_PER_CLIENT);
        this.retry = (boolean) getOption(ClickHouseClientOption.RETRY);
        this.reuseValueWrapper = (boolean) getOption(ClickHouseClientOption.REUSE_VALUE_WRAPPER);
        this.socketTimeout = (int) getOption(ClickHouseClientOption.SOCKET_TIMEOUT);
        this.sessionTimeout = (int) getOption(ClickHouseClientOption.SESSION_TIMEOUT);
        this.sessionCheck = (boolean) getOption(ClickHouseClientOption.SESSION_CHECK);
        this.ssl = (boolean) getOption(ClickHouseClientOption.SSL);
        this.sslMode = (ClickHouseSslMode) getOption(ClickHouseClientOption.SSL_MODE);
        this.sslRootCert = (String) getOption(ClickHouseClientOption.SSL_ROOT_CERTIFICATE);
        this.sslCert = (String) getOption(ClickHouseClientOption.SSL_CERTIFICATE);
        this.sslKey = (String) getOption(ClickHouseClientOption.SSL_KEY);
        this.useObjectsInArray = (boolean) getOption(ClickHouseClientOption.USE_OBJECTS_IN_ARRAYS);
        this.useServerTimeZone = (boolean) getOption(ClickHouseClientOption.USE_SERVER_TIME_ZONE);
        this.useServerTimeZoneForDate = (boolean) getOption(ClickHouseClientOption.USE_SERVER_TIME_ZONE_FOR_DATES);
        this.useTimeZone = (String) getOption(ClickHouseClientOption.USE_TIME_ZONE);

        if (credentials == null) {
            this.credentials = ClickHouseCredentials.fromUserAndPassword((String) getOption(ClickHouseDefaults.USER),
                    (String) getOption(ClickHouseDefaults.PASSWORD));
        } else {
            this.credentials = credentials;
        }
        this.metricRegistry = Optional.ofNullable(metricRegistry);
        this.nodeSelector = nodeSelector == null ? ClickHouseNodeSelector.EMPTY : nodeSelector;
    }

    public boolean isAsync() {
        return async;
    }

    public String getClientName() {
        return clientName;
    }

    public ClickHouseCompression getCompression() {
        return compression;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public String getDatabase() {
        return database;
    }

    public ClickHouseFormat getFormat() {
        return format;
    }

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    public int getMaxExecutionTime() {
        return maxExecutionTime;
    }

    public int getMaxQueuedBuffers() {
        return maxQueuedBuffers;
    }

    public int getMaxQueuedRequests() {
        return maxQueuedRequests;
    }

    public int getMaxResultRows() {
        return maxResultRows;
    }

    public int getMaxThreadsPerClient() {
        return maxThreads;
    }

    public boolean isRetry() {
        return retry;
    }

    public boolean isReuseValueWrapper() {
        return reuseValueWrapper;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public boolean isSessionCheck() {
        return sessionCheck;
    }

    public boolean isSsl() {
        return ssl;
    }

    public ClickHouseSslMode getSslMode() {
        return sslMode;
    }

    public String getSslRootCert() {
        return sslRootCert;
    }

    public String getSslCert() {
        return sslCert;
    }

    public String getSslKey() {
        return sslKey;
    }

    public boolean isUseObjectsInArray() {
        return useObjectsInArray;
    }

    public boolean isUseServerTimeZone() {
        return useServerTimeZone;
    }

    public String getUseTimeZone() {
        return useTimeZone;
    }

    public boolean isUseServerTimeZoneForDate() {
        return useServerTimeZoneForDate;
    }

    public ClickHouseCredentials getDefaultCredentials() {
        return this.credentials;
    }

    public Optional<Object> getMetricRegistry() {
        return this.metricRegistry;
    }

    public ClickHouseNodeSelector getNodeSelector() {
        return this.nodeSelector;
    }

    public List<ClickHouseProtocol> getPreferredProtocols() {
        return this.nodeSelector.getPreferredProtocols();
    }

    public Set<String> getPreferredTags() {
        return this.nodeSelector.getPreferredTags();
    }

    public Map<ClickHouseConfigOption, Serializable> getAllOptions() {
        return Collections.unmodifiableMap(this.options);
    }

    public Serializable getOption(ClickHouseConfigOption option) {
        return getOption(option, null);
    }

    public Serializable getOption(ClickHouseConfigOption option, ClickHouseDefaults defaultValue) {
        return this.options.getOrDefault(ClickHouseChecker.nonNull(option, "option"),
                defaultValue == null ? option.getEffectiveDefaultValue() : defaultValue.getEffectiveDefaultValue());
    }

    /**
     * Test whether a given option is configured or not.
     *
     * @param option option to test
     * @return true if the option is configured; false otherwise
     */
    public boolean hasOption(ClickHouseConfigOption option) {
        return option != null && this.options.containsKey(option);
    }
}
