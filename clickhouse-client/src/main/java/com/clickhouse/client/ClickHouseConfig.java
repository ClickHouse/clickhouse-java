package com.clickhouse.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.Map.Entry;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.client.config.ClickHouseSslMode;
import com.clickhouse.config.ClickHouseBufferingMode;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.config.ClickHouseRenameMethod;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseVersion;

/**
 * An immutable class holding client-specific options like
 * {@link ClickHouseCredentials} and {@link ClickHouseNodeSelector} etc.
 */
@Deprecated
public class ClickHouseConfig implements ClickHouseDataConfig {
    static final class ClientOptions {
        static final ClientOptions INSTANCE = new ClientOptions();

        final Map<String, ClickHouseOption> customOptions;
        final Map<String, ClickHouseOption> sensitiveOptions;

        private ClientOptions() {
            Map<String, ClickHouseOption> m = new LinkedHashMap<>();
            Map<String, ClickHouseOption> s = new LinkedHashMap<>();
            for (ClickHouseOption o : ClickHouseClientOption.class.getEnumConstants()) {
                if (o.isSensitive()) {
                    s.put(o.getKey(), o);
                }
            }
            try {
                for (ClickHouseClient c : ClickHouseClientBuilder.loadClients()) {
                    Class<? extends ClickHouseOption> clazz = c.getOptionClass();
                    if (clazz == null || clazz == ClickHouseClientOption.class) {
                        continue;
                    }
                    for (ClickHouseOption o : clazz.getEnumConstants()) {
                        m.put(o.getKey(), o);
                        if (o.isSensitive()) {
                            s.put(o.getKey(), o);
                        }
                    }
                }
            } catch (Exception e) {
                // ignore
            }

            customOptions = m.isEmpty() ? Collections.emptyMap() : Collections.unmodifiableMap(m);
            sensitiveOptions = s.isEmpty() ? Collections.emptyMap() : Collections.unmodifiableMap(s);
        }
    }

    private static final long serialVersionUID = 7794222888859182491L;

    static final String PARAM_OPTION = "option";

    public static final String TYPE_NAME = "Config";

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

    /**
     * Converts given key-value pairs to a mutable map of corresponding
     * {@link ClickHouseOption}.
     *
     * @param props key-value pairs
     * @return non-null mutable map of client options
     */
    public static Map<ClickHouseOption, Serializable> toClientOptions(Map<?, ?> props) {
        Map<ClickHouseOption, Serializable> options = new HashMap<>();
        if (props != null && !props.isEmpty()) {
            Map<String, ClickHouseOption> customOptions = ClientOptions.INSTANCE.customOptions;
            for (Entry<?, ?> e : props.entrySet()) {
                if (e.getKey() == null || e.getValue() == null) {
                    continue;
                }

                String key = e.getKey().toString();
                // no need to enable option overidding for now
                ClickHouseOption o = ClickHouseClientOption.fromKey(key);
                if (o == null) {
                    o = customOptions.get(key);
                }

                if (o != null) {
                    options.put(o, ClickHouseOption.fromString(e.getValue().toString(), o.getValueType()));
                }
            }
        }

        return options;
    }

    // common options optimized for read
    private final boolean async;
    private final boolean autoDiscovery;
    private final Map<String, String> customSettings;
    private final String customSocketFactory;
    private final Map<String, String> customSocketFactoryOptions;
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
    private final int bufferQueueVariation;
    private final int readBufferSize;
    private final int writeBufferSize;
    private final int requestChunkSize;
    private final ClickHouseBufferingMode requestBuffering;
    private final ClickHouseBufferingMode responseBuffering;
    private final int maxExecutionTime;
    private final int maxMapperCache;
    private final int maxQueuedBuffers;
    private final int maxQueuedRequests;
    private final long maxResultRows;
    private final int maxThreads;
    private final String productName;
    private final int nodeCheckInterval;
    private final int failover;
    private final int retry;
    private final boolean repeatOnSessionLock;
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
    private final String keyStoreType;
    private final String trustStore;
    private final String trustStorePassword;
    private final int transactionTimeout;
    private final boolean widenUnsignedTypes;
    private final boolean useBinaryString;
    private final boolean useBlockingQueue;
    private final boolean useCompilation;
    private final boolean useObjectsInArray;
    private final boolean useServerTimeZone;
    private final boolean useServerTimeZoneForDates;
    private final TimeZone timeZoneForDate;
    private final TimeZone useTimeZone;
    private final ClickHouseProxyType proxyType;
    private final String proxyHost;
    private final int proxyPort;
    private final String proxyUserName;
    private final char[] proxyPassword;
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
     * Constructs a new configuration by consolidating given ones.
     *
     * @param configs list of configuration
     */
    public ClickHouseConfig(List<ClickHouseConfig> configs) {
        this(mergeOptions(configs), mergeCredentials(configs), mergeNodeSelector(configs),
                mergeMetricRegistry(configs));
    }

    /**
     * Constructs a new configuration using given options.
     *
     * @param options generic options
     */
    public ClickHouseConfig(Map<ClickHouseOption, Serializable> options) {
        this(options, null, null, null);
    }

    /**
     * Constructs a new configuration using given arguments.
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
        this.autoDiscovery = getBoolOption(ClickHouseClientOption.AUTO_DISCOVERY);
        this.customSettings = ClickHouseOption.toKeyValuePairs(getStrOption(ClickHouseClientOption.CUSTOM_SETTINGS));
        this.customSocketFactory = getStrOption(ClickHouseClientOption.CUSTOM_SOCKET_FACTORY);
        this.customSocketFactoryOptions = ClickHouseOption
                .toKeyValuePairs(getStrOption(ClickHouseClientOption.CUSTOM_SOCKET_FACTORY_OPTIONS));
        this.clientName = getStrOption(ClickHouseClientOption.CLIENT_NAME);
        this.compressRequest = getBoolOption(ClickHouseClientOption.DECOMPRESS);
        this.compressAlgorithm = getOption(ClickHouseClientOption.DECOMPRESS_ALGORITHM, ClickHouseCompression.class);
        this.compressLevel = getIntOption(ClickHouseClientOption.DECOMPRESS_LEVEL);
        this.decompressResponse = getBoolOption(ClickHouseClientOption.COMPRESS);
        this.decompressAlgorithm = getOption(ClickHouseClientOption.COMPRESS_ALGORITHM, ClickHouseCompression.class);
        this.decompressLevel = getIntOption(ClickHouseClientOption.COMPRESS_LEVEL);
        this.connectionTimeout = getIntOption(ClickHouseClientOption.CONNECTION_TIMEOUT);
        this.database = (String) getOption(ClickHouseClientOption.DATABASE, ClickHouseDefaults.DATABASE);
        this.format = (ClickHouseFormat) getOption(ClickHouseClientOption.FORMAT, ClickHouseDefaults.FORMAT);
        this.maxBufferSize = getIntOption(ClickHouseClientOption.MAX_BUFFER_SIZE);
        int size = getIntOption(ClickHouseClientOption.BUFFER_SIZE);
        this.bufferSize = Math.min(size < 1? DEFAULT_BUFFER_SIZE: size, this.maxBufferSize);
        size = getIntOption(ClickHouseClientOption.READ_BUFFER_SIZE);
        this.readBufferSize = Math.min(size < 1? this.bufferSize : size, this.maxBufferSize);
        size = getIntOption(ClickHouseClientOption.WRITE_BUFFER_SIZE);
        this.writeBufferSize = Math.min(size < 1 ? this.bufferSize : size , this.maxBufferSize);
        this.bufferQueueVariation = getIntOption(ClickHouseClientOption.BUFFER_QUEUE_VARIATION);
        int chunkSize = getIntOption(ClickHouseClientOption.REQUEST_CHUNK_SIZE);
        this.requestChunkSize = chunkSize < 1 ? this.writeBufferSize : chunkSize;
        this.requestBuffering = (ClickHouseBufferingMode) getOption(ClickHouseClientOption.REQUEST_BUFFERING,
                ClickHouseDefaults.BUFFERING);
        this.responseBuffering = (ClickHouseBufferingMode) getOption(ClickHouseClientOption.RESPONSE_BUFFERING,
                ClickHouseDefaults.BUFFERING);
        this.maxExecutionTime = getIntOption(ClickHouseClientOption.MAX_EXECUTION_TIME);
        this.maxMapperCache = getIntOption(ClickHouseClientOption.MAX_MAPPER_CACHE);
        this.maxQueuedBuffers = getIntOption(ClickHouseClientOption.MAX_QUEUED_BUFFERS);
        this.maxQueuedRequests = getIntOption(ClickHouseClientOption.MAX_QUEUED_REQUESTS);
        this.maxResultRows = getLongOption(ClickHouseClientOption.MAX_RESULT_ROWS);
        this.maxThreads = getIntOption(ClickHouseClientOption.MAX_THREADS_PER_CLIENT);
        this.productName = getStrOption(ClickHouseClientOption.PRODUCT_NAME);
        this.nodeCheckInterval = getIntOption(ClickHouseClientOption.NODE_CHECK_INTERVAL);
        this.failover = getIntOption(ClickHouseClientOption.FAILOVER);
        this.retry = getIntOption(ClickHouseClientOption.RETRY);
        this.repeatOnSessionLock = getBoolOption(ClickHouseClientOption.REPEAT_ON_SESSION_LOCK);
        this.reuseValueWrapper = getBoolOption(ClickHouseClientOption.REUSE_VALUE_WRAPPER);
        this.serverInfo = !ClickHouseChecker.isNullOrBlank(getStrOption(ClickHouseClientOption.SERVER_TIME_ZONE))
                && !ClickHouseChecker.isNullOrBlank(getStrOption(ClickHouseClientOption.SERVER_VERSION));
        this.serverTimeZone = TimeZone.getTimeZone(
                (String) getOption(ClickHouseClientOption.SERVER_TIME_ZONE, ClickHouseDefaults.SERVER_TIME_ZONE));
        this.serverVersion = ClickHouseVersion
                .of((String) getOption(ClickHouseClientOption.SERVER_VERSION, ClickHouseDefaults.SERVER_VERSION));
        this.sessionTimeout = getIntOption(ClickHouseClientOption.SESSION_TIMEOUT);
        this.sessionCheck = getBoolOption(ClickHouseClientOption.SESSION_CHECK);
        this.socketTimeout = getIntOption(ClickHouseClientOption.SOCKET_TIMEOUT);
        this.ssl = getBoolOption(ClickHouseClientOption.SSL);
        this.sslMode = getOption(ClickHouseClientOption.SSL_MODE, ClickHouseSslMode.class);
        this.sslRootCert = getStrOption(ClickHouseClientOption.SSL_ROOT_CERTIFICATE);
        this.sslCert = getStrOption(ClickHouseClientOption.SSL_CERTIFICATE);
        this.sslKey = getStrOption(ClickHouseClientOption.SSL_KEY);
        this.keyStoreType = getStrOption(ClickHouseClientOption.KEY_STORE_TYPE);
        this.trustStore = getStrOption(ClickHouseClientOption.TRUST_STORE);
        this.trustStorePassword = getStrOption(ClickHouseClientOption.KEY_STORE_PASSWORD);
        this.transactionTimeout = getIntOption(ClickHouseClientOption.TRANSACTION_TIMEOUT);
        this.widenUnsignedTypes = getBoolOption(ClickHouseClientOption.WIDEN_UNSIGNED_TYPES);
        this.useBinaryString = getBoolOption(ClickHouseClientOption.USE_BINARY_STRING);
        this.useBlockingQueue = getBoolOption(ClickHouseClientOption.USE_BLOCKING_QUEUE);
        this.useCompilation = getBoolOption(ClickHouseClientOption.USE_COMPILATION);
        this.useObjectsInArray = getBoolOption(ClickHouseClientOption.USE_OBJECTS_IN_ARRAYS);
        this.useServerTimeZone = getBoolOption(ClickHouseClientOption.USE_SERVER_TIME_ZONE);
        this.useServerTimeZoneForDates = getBoolOption(ClickHouseClientOption.USE_SERVER_TIME_ZONE_FOR_DATES);

        String timeZone = getStrOption(ClickHouseClientOption.USE_TIME_ZONE);
        TimeZone tz = ClickHouseChecker.isNullOrBlank(timeZone) ? TimeZone.getDefault()
                : TimeZone.getTimeZone(timeZone);
        this.useTimeZone = this.useServerTimeZone ? this.serverTimeZone : tz;
        this.timeZoneForDate = this.useServerTimeZoneForDates ? this.useTimeZone : null;

        if (credentials == null) {
            this.credentials = ClickHouseCredentials.fromUserAndPassword(getStrOption(ClickHouseDefaults.USER),
                    getStrOption(ClickHouseDefaults.PASSWORD));
        } else {
            this.credentials = credentials;
        }
        this.metricRegistry = Optional.ofNullable(metricRegistry);
        this.nodeSelector = nodeSelector == null ? ClickHouseNodeSelector.EMPTY : nodeSelector;

        // select the type of proxy to use
        this.proxyType = getOption(ClickHouseClientOption.PROXY_TYPE, ClickHouseProxyType.class);
        this.proxyHost = getStrOption(ClickHouseClientOption.PROXY_HOST);
        this.proxyPort = getIntOption(ClickHouseClientOption.PROXY_PORT);
        this.proxyUserName = getStrOption(ClickHouseClientOption.PROXY_USERNAME);
        this.proxyPassword = getStrOption(ClickHouseClientOption.PROXY_PASSWORD).toCharArray();
    }

    @Override
    public boolean isAsync() {
        return async;
    }

    public boolean isAutoDiscovery() {
        return autoDiscovery;
    }

    public Map<String, String> getCustomSettings() {
        return customSettings;
    }

    public String getCustomSocketFactory() {
        return customSocketFactory;
    }

    public Map<String, String> getCustomSocketFactoryOptions() {
        return customSocketFactoryOptions;
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
     * {@code false}, this will return {@code -1}.
     *
     * @return compress level
     */
    public int getResponseCompressLevel() {
        return decompressResponse ? decompressLevel : -1;
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
     * {@code false}, this will return {@code -1}.
     *
     * @return compress level
     */
    public int getRequestCompressLevel() {
        return compressRequest ? compressLevel : -1;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public String getDatabase() {
        return database;
    }

    @Override
    public ClickHouseFormat getFormat() {
        return format;
    }

    public int getNodeCheckInterval() {
        return nodeCheckInterval;
    }

    @Override
    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    @Override
    public int getMaxMapperCache() {
        return maxMapperCache;
    }

    @Override
    public int getBufferSize() {
        return bufferSize;
    }

    @Override
    public int getBufferQueueVariation() {
        return bufferQueueVariation;
    }

    @Override
    public int getReadBufferSize() {
        return readBufferSize;
    }

    @Override
    public int getWriteBufferSize() {
        return writeBufferSize;
    }

    /**
     * Gets request chunk size.
     *
     * @return request chunk size
     */
    public int getRequestChunkSize() {
        return ClickHouseDataConfig.getBufferSize(requestChunkSize, getWriteBufferSize(), getMaxBufferSize());
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

    @Override
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

    public String getProductName() {
        return productName;
    }

    public int getFailover() {
        return failover;
    }

    public int getRetry() {
        return retry;
    }

    public boolean isRepeatOnSessionLock() {
        return repeatOnSessionLock;
    }

    @Override
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

    public String getKeyStoreType() {
        return keyStoreType;
    }

    public String getTrustStore() {
        return trustStore;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public int getTransactionTimeout() {
        return transactionTimeout < 1 ? sessionTimeout : transactionTimeout;
    }

    @Override
    public boolean isWidenUnsignedTypes() {
        return widenUnsignedTypes;
    }

    @Override
    public boolean isUseBinaryString() {
        return useBinaryString;
    }

    @Override
    public boolean isUseBlockingQueue() {
        return useBlockingQueue;
    }

    @Override
    public boolean isUseCompilation() {
        return useCompilation;
    }

    @Override
    public boolean isUseObjectsInArray() {
        return useObjectsInArray;
    }

    public ClickHouseProxyType getProxyType() {
        return proxyType;
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public int getProxyPort() {
        return proxyPort;
    }

    public String getProxyUserName() {
        return proxyUserName;
    }

    public char[] getProxyPassword() {
        return proxyPassword;
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
    @Override
    public TimeZone getTimeZoneForDate() {
        return timeZoneForDate;
    }

    /**
     * Gets preferred time zone. When {@link #isUseServerTimeZone()} is
     * {@code true}, this returns same time zone as {@link #getServerTimeZone()}.
     *
     * @return non-null preferred time zone
     */
    @Override
    public TimeZone getUseTimeZone() {
        return useTimeZone;
    }

    /**
     * Same as {@link ClickHouseClientOption#PRODUCT_VERSION}.
     *
     * @return non-empty semantic version
     */
    public final String getProductVersion() {
        return ClickHouseClientOption.PRODUCT_VERSION;
    }

    /**
     * Same as {@link ClickHouseClientOption#PRODUCT_REVISION}.
     *
     * @return non-empty revision
     */
    public final String getProductRevision() {
        return ClickHouseClientOption.PRODUCT_REVISION;
    }

    /**
     * Same as {@link ClickHouseClientOption#CLIENT_OS_INFO}.
     *
     * @return non-empty O/S information
     */
    public final String getClientOsInfo() {
        return ClickHouseClientOption.CLIENT_OS_INFO;
    }

    /**
     * Same as {@link ClickHouseClientOption#CLIENT_JVM_INFO}.
     *
     * @return non-empty JVM information
     */
    public final String getClientJvmInfo() {
        return ClickHouseClientOption.CLIENT_JVM_INFO;
    }

    /**
     * Same as {@link ClickHouseClientOption#CLIENT_USER}.
     *
     * @return non-empty user name
     */
    public final String getClientUser() {
        return ClickHouseClientOption.CLIENT_USER;
    }

    /**
     * Same as {@link ClickHouseClientOption#CLIENT_HOST}.
     *
     * @return non-empty host name
     */
    public final String getClientHost() {
        return ClickHouseClientOption.CLIENT_HOST;
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

    /**
     * Gets typed option value. {@link ClickHouseOption#getEffectiveDefaultValue}
     * will be called when the option is undefined.
     *
     * @param <T>       type of option value, must be serializable
     * @param option    non-null option to lookup
     * @param valueType non-null type of option value, must be serializable
     * @return typed value
     */
    @SuppressWarnings("unchecked")
    public <T extends Serializable> T getOption(ClickHouseOption option, Class<T> valueType) {
        if (ClickHouseChecker.nonNull(option, PARAM_OPTION).getValueType() != ClickHouseChecker.nonNull(valueType,
                "valueType")) {
            throw new IllegalArgumentException(
                    "Cannot convert value from type " + option.getValueType() + " to " + valueType);
        }

        T value = (T) options.get(option);
        return value != null ? value : (T) option.getEffectiveDefaultValue();
    }

    /**
     * Gets option value.
     *
     * @param option        non-null option to lookup
     * @param defaultConfig optional default config to retrieve default value
     * @return option value
     */
    public Serializable getOption(ClickHouseOption option, ClickHouseConfig defaultConfig) {
        return this.options.getOrDefault(ClickHouseChecker.nonNull(option, PARAM_OPTION),
                defaultConfig == null ? option.getEffectiveDefaultValue() : defaultConfig.getOption(option));
    }

    /**
     * Gets option value.
     *
     * @param option       non-null option to lookup
     * @param defaultValue optional default value
     * @return option value
     */
    public Serializable getOption(ClickHouseOption option, ClickHouseDefaults defaultValue) {
        return this.options.getOrDefault(ClickHouseChecker.nonNull(option, PARAM_OPTION),
                defaultValue == null ? option.getEffectiveDefaultValue() : defaultValue.getEffectiveDefaultValue());
    }

    /**
     * Shortcut of {@link #getOption(ClickHouseOption, ClickHouseDefaults)}.
     *
     * @param option non-null option to lookup
     * @return option value
     */
    public Serializable getOption(ClickHouseOption option) {
        return getOption(option, (ClickHouseDefaults) null);
    }

    /**
     * Shortcut of {@code getOption(option, Boolean.class)}.
     *
     * @param option non-null option to lookup
     * @return boolean value of the given option
     */
    public boolean getBoolOption(ClickHouseOption option) {
        return getOption(option, Boolean.class);
    }

    /**
     * Shortcut of {@code getOption(option, Integer.class)}.
     *
     * @param option non-null option to lookup
     * @return int value of the given option
     */
    public int getIntOption(ClickHouseOption option) {
        return getOption(option, Integer.class);
    }

    /**
     * Shortcut of {@code getOption(option, Long.class)}.
     *
     * @param option non-null option to lookup
     * @return long value of the given option
     */
    public long getLongOption(ClickHouseOption option) {
        return getOption(option, Long.class);
    }

    /**
     * Shortcut of {@code getOption(option, String.class)}.
     *
     * @param option non-null option to lookup
     * @return String value of the given option
     */
    public String getStrOption(ClickHouseOption option) {
        return getOption(option, String.class);
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

    @Override
    public int hashCode() {
        return Objects.hash(options, credentials, metricRegistry.orElse(null), nodeSelector);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseConfig other = (ClickHouseConfig) obj;
        return Objects.equals(options, other.options) && Objects.equals(credentials, other.credentials)
                && Objects.equals(metricRegistry.orElse(null), other.metricRegistry.orElse(null))
                && Objects.equals(nodeSelector, other.nodeSelector);
    }

    @Override
    public int getReadTimeout() {
        return getSocketTimeout();
    }

    @Override
    public int getWriteTimeout() {
        return getSocketTimeout();
    }

    @Override
    public ClickHouseBufferingMode getReadBufferingMode() {
        return getResponseBuffering();
    }

    @Override
    public ClickHouseBufferingMode getWriteBufferingMode() {
        return getRequestBuffering();
    }

    @Override
    public ClickHouseRenameMethod getColumnRenameMethod() {
        return (ClickHouseRenameMethod) getOption(ClickHouseClientOption.RENAME_RESPONSE_COLUMN);
    }
}
