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
import java.util.TimeZone;

import com.clickhouse.client.config.ClickHouseBufferingMode;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.config.ClickHouseSslMode;

/**
 * An immutable class holding client-specific options like
 * {@link ClickHouseCredentials} and {@link ClickHouseNodeSelector} etc.
 */
public class ClickHouseConfig implements Serializable {
    protected static final Map<ClickHouseOption, Serializable> mergeOptions(List<ClickHouseConfig> list) {
        if (list == null || list.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<ClickHouseOption, Serializable> options = new HashMap<>();
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
    private final boolean compressRequest;
    private final ClickHouseCompression compressAlgorithm;
    private final int compressLevel;
    private final boolean decompressResponse;
    private final ClickHouseCompression decompressAlgorithm;
    private final int decompressLevel;
    private final int connectionTimeout;
    private final String database;
    private final ClickHouseFormat format;
    private final int maxBufferSize;
    private final int bufferSize;
    private final int readBufferSize;
    private final int writeBufferSize;
    private final ClickHouseBufferingMode requestBuffering;
    private final ClickHouseBufferingMode responseBuffering;
    private final int maxExecutionTime;
    private final int maxQueuedBuffers;
    private final int maxQueuedRequests;
    private final long maxResultRows;
    private final int maxThreads;
    private final boolean retry;
    private final boolean reuseValueWrapper;
    private final boolean serverInfo;
    private final TimeZone serverTimeZone;
    private final ClickHouseVersion serverVersion;
    private final int sessionTimeout;
    private final boolean sessionCheck;
    private final int socketTimeout;
    private final boolean ssl;
    private final ClickHouseSslMode sslMode;
    private final String sslRootCert;
    private final String sslCert;
    private final String sslKey;
    private final boolean useBlockingQueue;
    private final boolean useObjectsInArray;
    private final boolean useServerTimeZone;
    private final boolean useServerTimeZoneForDates;
    private final TimeZone timeZoneForDate;
    private final TimeZone useTimeZone;

    // client specific options
    private final Map<ClickHouseOption, Serializable> options;
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
    public ClickHouseConfig(Map<ClickHouseOption, Serializable> options, ClickHouseCredentials credentials,
            ClickHouseNodeSelector nodeSelector, Object metricRegistry) {
        this.options = new HashMap<>();
        if (options != null) {
            this.options.putAll(options);
        }

        this.async = (boolean) getOption(ClickHouseClientOption.ASYNC, ClickHouseDefaults.ASYNC);
        this.clientName = (String) getOption(ClickHouseClientOption.CLIENT_NAME);
        this.compressRequest = (boolean) getOption(ClickHouseClientOption.DECOMPRESS);
        this.compressAlgorithm = (ClickHouseCompression) getOption(ClickHouseClientOption.DECOMPRESS_ALGORITHM);
        this.compressLevel = (int) getOption(ClickHouseClientOption.DECOMPRESS_LEVEL);
        this.decompressResponse = (boolean) getOption(ClickHouseClientOption.COMPRESS);
        this.decompressAlgorithm = (ClickHouseCompression) getOption(ClickHouseClientOption.COMPRESS_ALGORITHM);
        this.decompressLevel = (int) getOption(ClickHouseClientOption.COMPRESS_LEVEL);
        this.connectionTimeout = (int) getOption(ClickHouseClientOption.CONNECTION_TIMEOUT);
        this.database = (String) getOption(ClickHouseClientOption.DATABASE, ClickHouseDefaults.DATABASE);
        this.format = (ClickHouseFormat) getOption(ClickHouseClientOption.FORMAT, ClickHouseDefaults.FORMAT);
        this.maxBufferSize = ClickHouseUtils.getBufferSize((int) getOption(ClickHouseClientOption.MAX_BUFFER_SIZE), -1,
                -1);
        this.bufferSize = ClickHouseUtils.getBufferSize((int) getOption(ClickHouseClientOption.BUFFER_SIZE), -1,
                this.maxBufferSize);
        this.readBufferSize = ClickHouseUtils.getBufferSize(
                (int) getOption(ClickHouseClientOption.READ_BUFFER_SIZE), this.bufferSize, this.maxBufferSize);
        this.writeBufferSize = ClickHouseUtils.getBufferSize(
                (int) getOption(ClickHouseClientOption.WRITE_BUFFER_SIZE), this.bufferSize, this.maxBufferSize);
        this.requestBuffering = (ClickHouseBufferingMode) getOption(ClickHouseClientOption.REQUEST_BUFFERING,
                ClickHouseDefaults.BUFFERING);
        this.responseBuffering = (ClickHouseBufferingMode) getOption(ClickHouseClientOption.RESPONSE_BUFFERING,
                ClickHouseDefaults.BUFFERING);
        this.maxExecutionTime = (int) getOption(ClickHouseClientOption.MAX_EXECUTION_TIME);
        this.maxQueuedBuffers = (int) getOption(ClickHouseClientOption.MAX_QUEUED_BUFFERS);
        this.maxQueuedRequests = (int) getOption(ClickHouseClientOption.MAX_QUEUED_REQUESTS);
        this.maxResultRows = (long) getOption(ClickHouseClientOption.MAX_RESULT_ROWS);
        this.maxThreads = (int) getOption(ClickHouseClientOption.MAX_THREADS_PER_CLIENT);
        this.retry = (boolean) getOption(ClickHouseClientOption.RETRY);
        this.reuseValueWrapper = (boolean) getOption(ClickHouseClientOption.REUSE_VALUE_WRAPPER);
        this.serverInfo = !ClickHouseChecker.isNullOrBlank((String) getOption(ClickHouseClientOption.SERVER_TIME_ZONE))
                && !ClickHouseChecker.isNullOrBlank((String) getOption(ClickHouseClientOption.SERVER_VERSION));
        this.serverTimeZone = TimeZone.getTimeZone(
                (String) getOption(ClickHouseClientOption.SERVER_TIME_ZONE, ClickHouseDefaults.SERVER_TIME_ZONE));
        this.serverVersion = ClickHouseVersion
                .of((String) getOption(ClickHouseClientOption.SERVER_VERSION, ClickHouseDefaults.SERVER_VERSION));
        this.sessionTimeout = (int) getOption(ClickHouseClientOption.SESSION_TIMEOUT);
        this.sessionCheck = (boolean) getOption(ClickHouseClientOption.SESSION_CHECK);
        this.socketTimeout = (int) getOption(ClickHouseClientOption.SOCKET_TIMEOUT);
        this.ssl = (boolean) getOption(ClickHouseClientOption.SSL);
        this.sslMode = (ClickHouseSslMode) getOption(ClickHouseClientOption.SSL_MODE);
        this.sslRootCert = (String) getOption(ClickHouseClientOption.SSL_ROOT_CERTIFICATE);
        this.sslCert = (String) getOption(ClickHouseClientOption.SSL_CERTIFICATE);
        this.sslKey = (String) getOption(ClickHouseClientOption.SSL_KEY);
        this.useBlockingQueue = (boolean) getOption(ClickHouseClientOption.USE_BLOCKING_QUEUE);
        this.useObjectsInArray = (boolean) getOption(ClickHouseClientOption.USE_OBJECTS_IN_ARRAYS);
        this.useServerTimeZone = (boolean) getOption(ClickHouseClientOption.USE_SERVER_TIME_ZONE);
        this.useServerTimeZoneForDates = (boolean) getOption(ClickHouseClientOption.USE_SERVER_TIME_ZONE_FOR_DATES);

        String timeZone = (String) getOption(ClickHouseClientOption.USE_TIME_ZONE);
        TimeZone tz = ClickHouseChecker.isNullOrBlank(timeZone) ? TimeZone.getDefault()
                : TimeZone.getTimeZone(timeZone);
        this.useTimeZone = this.useServerTimeZone ? this.serverTimeZone : tz;
        this.timeZoneForDate = this.useServerTimeZoneForDates ? this.useTimeZone : null;

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

    /**
     * Checks if server response is compressed or not.
     *
     * @return true if server response is compressed; false otherwise
     */
    public boolean isResponseCompressed() {
        return decompressResponse;
    }

    /**
     * Gets server response compress algorithm. When {@link #isResponseCompressed()}
     * is {@code false}, this will return {@link ClickHouseCompression#NONE}.
     *
     * @return non-null compress algorithm
     */
    public ClickHouseCompression getResponseCompressAlgorithm() {
        return decompressResponse ? decompressAlgorithm : ClickHouseCompression.NONE;
    }

    /**
     * Gets input compress level. When {@link #isResponseCompressed()} is
     * {@code false}, this will return {@code 0}.
     *
     * @return compress level
     */
    public int getResponseCompressLevel() {
        return decompressResponse ? decompressLevel : 0;
    }

    /**
     * Checks if server response should be compressed or not.
     *
     * @return
     * @deprecated will be removed in v0.3.3, please use
     *             {@link #isResponseCompressed()} instead
     */
    @Deprecated
    public boolean isCompressServerResponse() {
        return decompressResponse;
    }

    /**
     * Gets compress algorithm for server response.
     *
     * @return compress algorithm for server response
     * @deprecated will be removed in v0.3.3, please use
     *             {@link #getResponseCompressAlgorithm()} instead
     */
    @Deprecated
    public ClickHouseCompression getCompressAlgorithmForServerResponse() {
        return decompressAlgorithm;
    }

    /**
     * Gets compress level for server response.
     *
     * @return compress level
     * @deprecated will be removed in v0.3.3, please use
     *             {@link #getResponseCompressLevel()} instead
     */
    @Deprecated
    public int getCompressLevelForServerResponse() {
        return decompressLevel;
    }

    /**
     * Checks if client's output, aka. client request, should be compressed or not.
     *
     * @return true if client request should be compressed; false otherwise
     */
    public boolean isRequestCompressed() {
        return compressRequest;
    }

    /**
     * Gets input compress algorithm. When {@link #isRequestCompressed()} is
     * {@code false}, this will return {@link ClickHouseCompression#NONE}.
     *
     * @return non-null compress algorithm
     */
    public ClickHouseCompression getRequestCompressAlgorithm() {
        return compressRequest ? compressAlgorithm : ClickHouseCompression.NONE;
    }

    /**
     * Gets input compress level. When {@link #isRequestCompressed()} is
     * {@code false}, this will return {@code 0}.
     *
     * @return compress level
     */
    public int getRequestCompressLevel() {
        return compressRequest ? compressLevel : 0;
    }

    /**
     * Checks if client request should be compressed or not.
     *
     * @return
     * @deprecated will be removed in v0.3.3, please use
     *             {@link #isRequestCompressed()} instead
     */
    @Deprecated
    public boolean isDecompressClientRequet() {
        return compressRequest;
    }

    /**
     * Gets compress algorithm for client request.
     *
     * @return compress algorithm for client request
     * @deprecated will be removed in v0.3.3, please use
     *             {@link #getRequestCompressAlgorithm()} instead
     */
    @Deprecated
    public ClickHouseCompression getDecompressAlgorithmForClientRequest() {
        return decompressAlgorithm;
    }

    /**
     * Gets compress level for client request.
     *
     * @return compress level
     * @deprecated will be removed in v0.3.3, please use
     *             {@link #getRequestCompressLevel()} instead
     */
    @Deprecated
    public int getDecompressLevelForClientRequest() {
        return decompressLevel;
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

    /**
     * Gets max buffer size in byte can be used for streaming.
     *
     * @return max buffer size in byte
     */
    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    /**
     * Gets default buffer size in byte for both read and write.
     *
     * @return default buffer size in byte
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Gets read buffer size in byte.
     *
     * @return read buffer size in byte
     */
    public int getReadBufferSize() {
        return readBufferSize;
    }

    /**
     * Gets write buffer size in byte.
     *
     * @return write buffer size in byte
     */
    public int getWriteBufferSize() {
        return writeBufferSize;
    }

    /**
     * Gets request buffering mode.
     *
     * @return request buffering mode
     */
    public ClickHouseBufferingMode getRequestBuffering() {
        return requestBuffering;
    }

    /**
     * Gets response buffering mode.
     *
     * @return response buffering mode
     */
    public ClickHouseBufferingMode getResponseBuffering() {
        return responseBuffering;
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

    public long getMaxResultRows() {
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

    /**
     * Checks whether we got all server information(e.g. timezone and version).
     *
     * @return true if we got all server information; false otherwise
     */
    public boolean hasServerInfo() {
        return serverInfo;
    }

    public TimeZone getServerTimeZone() {
        return serverTimeZone;
    }

    public ClickHouseVersion getServerVersion() {
        return serverVersion;
    }

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public boolean isSessionCheck() {
        return sessionCheck;
    }

    public int getSocketTimeout() {
        return socketTimeout;
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

    public boolean isUseBlockingQueue() {
        return useBlockingQueue;
    }

    public boolean isUseObjectsInArray() {
        return useObjectsInArray;
    }

    public boolean isUseServerTimeZone() {
        return useServerTimeZone;
    }

    public boolean isUseServerTimeZoneForDates() {
        return useServerTimeZoneForDates;
    }

    /**
     * Gets time zone for date values.
     *
     * @return time zone, could be null when {@code use_server_time_zone_for_date}
     *         is set to {@code false}.
     */
    public TimeZone getTimeZoneForDate() {
        return timeZoneForDate;
    }

    /**
     * Gets preferred time zone. When {@link #isUseServerTimeZone()} is
     * {@code true}, this returns same time zone as {@link #getServerTimeZone()}.
     *
     * @return non-null preferred time zone
     */
    public TimeZone getUseTimeZone() {
        return useTimeZone;
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

    public Map<ClickHouseOption, Serializable> getAllOptions() {
        return Collections.unmodifiableMap(this.options);
    }

    public Serializable getOption(ClickHouseOption option) {
        return getOption(option, null);
    }

    public Serializable getOption(ClickHouseOption option, ClickHouseDefaults defaultValue) {
        return this.options.getOrDefault(ClickHouseChecker.nonNull(option, "option"),
                defaultValue == null ? option.getEffectiveDefaultValue() : defaultValue.getEffectiveDefaultValue());
    }

    /**
     * Test whether a given option is configured or not.
     *
     * @param option option to test
     * @return true if the option is configured; false otherwise
     */
    public boolean hasOption(ClickHouseOption option) {
        return option != null && this.options.containsKey(option);
    }
}
