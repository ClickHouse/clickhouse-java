package com.clickhouse.client.api;

import com.clickhouse.client.api.command.CommandResponse;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.NativeFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.data_formats.internal.MapBackedRecord;
import com.clickhouse.client.api.data_formats.internal.ProcessParser;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.enums.ProxyType;
import com.clickhouse.client.api.http.ClickHouseHttpProto;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ClientStatisticsHolder;
import com.clickhouse.client.api.internal.HttpAPIClientHelper;
import com.clickhouse.client.api.internal.MapUtils;
import com.clickhouse.client.api.internal.TableSchemaParser;
import com.clickhouse.client.api.internal.ValidationUtils;
import com.clickhouse.client.api.metadata.ColumnToMethodMatchingStrategy;
import com.clickhouse.client.api.metadata.DefaultColumnToMethodMatchingStrategy;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.client.api.serde.DataSerializationException;
import com.clickhouse.client.api.serde.POJOFieldDeserializer;
import com.clickhouse.client.api.serde.POJOFieldSerializer;
import com.clickhouse.client.api.serde.POJOSerDe;
import com.clickhouse.client.api.transport.Endpoint;
import com.clickhouse.client.api.transport.HttpEndpoint;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;
import com.google.common.collect.ImmutableList;
import net.jpountz.lz4.LZ4Factory;
import org.apache.hc.core5.concurrent.DefaultThreadFactory;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * <p>Client is the starting point for all interactions with ClickHouse. </p>
 *
 * <p>{@link Builder} is designed to construct a client object with required configuration:</p>
 * {@code
 *
 *  Client client = new Client.Builder()
 *        .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort())
 *        .addUsername("default")
 *        .build();
 *  }
 *
 *
 *
 * <p>When client object is created any operation method can be called on it:</p>
 * {@code
 *
 *  if (client.ping()) {
 *      QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
 *      try (QueryResponse response = client.query("SELECT * FROM " + table, settings).get(10, TimeUnit.SECONDS)) {
 *          ...
 *      }
 *  }
 * }
 *
 *
 *
 * <p>Client is thread-safe. It uses exclusive set of object to perform an operation.</p>
 *
 */
public class Client implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private HttpAPIClientHelper httpClientHelper = null;

    private final List<Endpoint> endpoints;
    private final Map<String, Object> configuration;

    private final Map<String, String> readOnlyConfig;

    private final POJOSerDe pojoSerDe;

    private final ExecutorService sharedOperationExecutor;

    private final boolean isSharedOpExecutorOwned;

    private final Map<String, ClientStatisticsHolder> globalClientStats = new ConcurrentHashMap<>();

    private final Map<String, TableSchema> tableSchemaCache = new ConcurrentHashMap<>();

    private final Map<String, Boolean> tableSchemaHasDefaults = new ConcurrentHashMap<>();

    private final Map<ClickHouseDataType, Class<?>> typeHintMapping;

    // Server context
    private String dbUser;
    private String serverVersion;
    private final Object metricsRegistry;
    private final int retries;
    private LZ4Factory lz4Factory = null;
    private final Supplier<String> queryIdGenerator;

    private Client(Collection<Endpoint> endpoints, Map<String,String> configuration,
                   ExecutorService sharedOperationExecutor, ColumnToMethodMatchingStrategy columnToMethodMatchingStrategy,
                   Object metricsRegistry, Supplier<String> queryIdGenerator) {
        this.configuration = ClientConfigProperties.parseConfigMap(configuration);
        this.readOnlyConfig = Collections.unmodifiableMap(configuration);
        this.metricsRegistry = metricsRegistry;
        this.queryIdGenerator = queryIdGenerator;

        // Serialization
        this.pojoSerDe = new POJOSerDe(columnToMethodMatchingStrategy);

        // Operation Execution
        boolean isAsyncEnabled = ClientConfigProperties.ASYNC_OPERATIONS.getOrDefault(this.configuration);

        if (isAsyncEnabled && sharedOperationExecutor == null) {
            this.isSharedOpExecutorOwned = true;
            this.sharedOperationExecutor = Executors.newCachedThreadPool(new DefaultThreadFactory("chc-operation"));
        } else {
            this.isSharedOpExecutorOwned = false;
            this.sharedOperationExecutor = sharedOperationExecutor;
        }

        // Transport
        ImmutableList.Builder<Endpoint> tmpEndpoints = ImmutableList.builder();
        boolean initSslContext = false;
        for (Endpoint ep : endpoints) {
            if (ep instanceof HttpEndpoint) {
                HttpEndpoint httpEndpoint = (HttpEndpoint) ep;
                if (httpEndpoint.isSecure()) {
                    initSslContext = true;
                }
                LOG.debug("Adding endpoint: {}", httpEndpoint);
                tmpEndpoints.add(httpEndpoint);
            } else {
                throw new ClientException("Unsupported endpoint type: " + ep.getClass().getName());
            }
        }

        this.endpoints = tmpEndpoints.build();

        String retry = configuration.get(ClientConfigProperties.RETRY_ON_FAILURE.getKey());
        this.retries = retry == null ? 0 : Integer.parseInt(retry);
        boolean useNativeCompression = !MapUtils.getFlag(configuration, ClientConfigProperties.DISABLE_NATIVE_COMPRESSION.getKey(), false);
        if (useNativeCompression) {
            this.lz4Factory = LZ4Factory.fastestInstance();
        } else {
            this.lz4Factory = LZ4Factory.fastestJavaInstance();
        }

        this.httpClientHelper = new HttpAPIClientHelper(this.configuration, metricsRegistry, initSslContext, lz4Factory);
        this.serverVersion = configuration.getOrDefault(ClientConfigProperties.SERVER_VERSION.getKey(), "unknown");
        this.dbUser = configuration.getOrDefault(ClientConfigProperties.USER.getKey(), ClientConfigProperties.USER.getDefObjVal());
        this.typeHintMapping = (Map<ClickHouseDataType, Class<?>>) this.configuration.get(ClientConfigProperties.TYPE_HINT_MAPPING.getKey());
    }

    /**
     * Loads essential information about a server. Should be called after client creation.
     *
     */
    public void loadServerInfo() {
        try (QueryResponse response = this.query("SELECT currentUser() AS user, timezone() AS timezone, version() AS version LIMIT 1").get()) {
            try (ClickHouseBinaryFormatReader reader = this.newBinaryFormatReader(response)) {
                if (reader.next() != null) {
                    String tmpDbUser = reader.getString("user");
                    if (tmpDbUser != null && !tmpDbUser.isEmpty()) {
                        this.dbUser = tmpDbUser;
                    }
                    this.configuration.put(ClientConfigProperties.SERVER_TIMEZONE.getKey(), reader.getString("timezone"));
                    serverVersion = reader.getString("version");
                }
            }
        } catch (Exception e) {
            throw new ClientException("Failed to get server info", e);
        }
    }

    /**
     * Returns default database name that will be used by operations if not specified.
     *
     * @return String - actual default database name.
     */
    public String getDefaultDatabase() {
        return (String) this.configuration.get(ClientConfigProperties.DATABASE.getKey());
    }


    /**
     * Frees the resources associated with the client.
     * <ul>
     *     <li>Shuts down the shared operation executor by calling {@code shutdownNow()}</li>
     * </ul>
     */
    @Override
    public void close() {
        if (isSharedOpExecutorOwned) {
            try {
                if (sharedOperationExecutor != null && !sharedOperationExecutor.isShutdown()) {
                    this.sharedOperationExecutor.shutdownNow();
                }
            } catch (Exception e) {
                LOG.error("Failed to close shared operation executor", e);
            }
        } else {
            LOG.debug("Skip closing operation executor because not owned by client");
        }

        if (httpClientHelper != null) {
            httpClientHelper.close();
        }
    }


    public static class Builder {
        private Set<Endpoint> endpoints;

        // Read-only configuration
        private Map<String, String> configuration;

        private ExecutorService sharedOperationExecutor = null;
        private ColumnToMethodMatchingStrategy columnToMethodMatchingStrategy;
        private Object metricRegistry = null;
        private Supplier<String> queryIdGenerator;

        public Builder() {
            this.endpoints = new HashSet<>();
            this.configuration = new HashMap<>();

            for (ClientConfigProperties p : ClientConfigProperties.values()) {
                if (p.getDefaultValue() != null) {
                    this.configuration.put(p.getKey(), p.getDefaultValue());
                }
            }

            allowBinaryReaderToReuseBuffers(false);
            columnToMethodMatchingStrategy = DefaultColumnToMethodMatchingStrategy.INSTANCE;
        }

        /**
         * Server address to which client may connect. If there are multiple endpoints then client will
         * connect to one of them.
         * Acceptable formats are:
         * <ul>
         *     <li>{@code http://localhost:8123}</li>
         *     <li>{@code https://localhost:8443}</li>
         *     <li>{@code http://localhost:8123/clickhouse} (with path for reverse proxy scenarios)</li>
         * </ul>
         *
         * @param endpoint - URL formatted string with protocol, host, port, and optional path.
         */
        public Builder addEndpoint(String endpoint) {
            try {
                URL endpointURL = new URL(endpoint);

                String protocolStr = endpointURL.getProtocol();
                if (!protocolStr.equalsIgnoreCase("https") &&
                    !protocolStr.equalsIgnoreCase("http")) {
                    throw new IllegalArgumentException("Only HTTP and HTTPS protocols are supported");
                }

                boolean secure = protocolStr.equalsIgnoreCase("https");
                String host = endpointURL.getHost();
                if (host == null || host.isEmpty()) {
                    throw new IllegalArgumentException("Host cannot be empty in endpoint: " + endpoint);
                }

                int port = endpointURL.getPort();
                if (port <= 0) {
                    throw new ValidationUtils.SettingsValidationException("port", "Valid port must be specified");
                }

                String path = endpointURL.getPath();
                if (path == null || path.isEmpty()) {
                    path = "/";
                }

                return addEndpoint(Protocol.HTTP, host, port, secure, path);
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("Endpoint should be a valid URL string, but was " + endpoint, e);
            }
        }

        /**
         * Server address to which client may connect. If there are multiple endpoints then client will
         * connect to one of them.
         *
         * @param protocol - Endpoint protocol
         * @param host - Endpoint host
         * @param port - Endpoint port
         */
        public Builder addEndpoint(Protocol protocol, String host, int port, boolean secure) {
            return addEndpoint(protocol, host, port, secure, "/");
        }

        public Builder addEndpoint(Protocol protocol, String host, int port, boolean secure, String basePath) {
            ValidationUtils.checkNonBlank(host, "host");
            ValidationUtils.checkNotNull(protocol, "protocol");
            ValidationUtils.checkRange(port, 1, ValidationUtils.TCP_PORT_NUMBER_MAX, "port");
            ValidationUtils.checkNotNull(basePath, "basePath");

            if (protocol == Protocol.HTTP) {
                HttpEndpoint httpEndpoint = new HttpEndpoint(host, port, secure, basePath);
                this.endpoints.add(httpEndpoint);
            } else {
                throw new IllegalArgumentException("Unsupported protocol: " + protocol);
            }
            return this;

        }


        /**
         * Sets a configuration option. This method can be used to set any configuration option.
         * There is no specific validation is done on the key or value.
         *
         * @param key - configuration option name
         * @param value - configuration option value
         */
        public Builder setOption(String key, String value) {
            this.configuration.put(key, value);
            if (key.equals(ClientConfigProperties.PRODUCT_NAME.getKey())) {
                setClientName(value);
            }
            if (key.equals(ClientConfigProperties.BEARERTOKEN_AUTH.getKey())) {
                useBearerTokenAuth(value);
            }
            return this;
        }

        /**
         * Username for authentication with server. Required for all operations.
         * Same username will be used for all endpoints.
         *
         * @param username - a valid username
         */
        public Builder setUsername(String username) {
            this.configuration.put(ClientConfigProperties.USER.getKey(), username);
            return this;
        }

        /**
         * Password for authentication with server. Required for all operations.
         * Same password will be used for all endpoints.
         *
         * @param password - plain text password
         */
        public Builder setPassword(String password) {
            this.configuration.put(ClientConfigProperties.PASSWORD.getKey(), password);
            return this;
        }

        /**
         * Access token for authentication with server. Required for all operations.
         * Same access token will be used for all endpoints.
         *
         * @param accessToken - plain text access token
         */
        public Builder setAccessToken(String accessToken) {
            this.configuration.put(ClientConfigProperties.ACCESS_TOKEN.getKey(), accessToken);
            return this;
        }

        /**
         * Makes client to use SSL Client Certificate to authenticate with server.
         * Client certificate should be set as well. {@link Builder#setClientCertificate(String)}
         * @param useSSLAuthentication
         * @return
         */
        public Builder useSSLAuthentication(boolean useSSLAuthentication) {
            this.configuration.put(ClientConfigProperties.SSL_AUTH.getKey(), String.valueOf(useSSLAuthentication));
            return this;
        }

        /**
         * Configures client to use build-in connection pool
         * @param enable - if connection pool should be enabled
         * @return
         */
        public Builder enableConnectionPool(boolean enable) {
            this.configuration.put(ClientConfigProperties.CONNECTION_POOL_ENABLED.getKey(), String.valueOf(enable));
            return this;
        }

        /**
         * Default connection timeout in milliseconds. Timeout is applied to establish a connection.
         *
         * @param timeout - connection timeout in milliseconds
         */
        public Builder setConnectTimeout(long timeout) {
            this.configuration.put(ClientConfigProperties.CONNECTION_TIMEOUT.getKey(), String.valueOf(timeout));
            return this;
        }

        /**
         * Default connection timeout in milliseconds. Timeout is applied to establish a connection.
         *
         * @param timeout - connection timeout value
         * @param unit - time unit
         */
        public Builder setConnectTimeout(long timeout, ChronoUnit unit) {
            return this.setConnectTimeout(Duration.of(timeout, unit).toMillis());
        }

        /**
         * Set timeout for waiting a free connection from a pool when all connections are leased.
         * This configuration is important when need to fail fast in high concurrent scenarios.
         * Default is 10 s.
         * @param timeout - connection timeout in milliseconds
         * @param unit - time unit
         */
        public Builder setConnectionRequestTimeout(long timeout, ChronoUnit unit) {
            this.configuration.put(ClientConfigProperties.CONNECTION_REQUEST_TIMEOUT.getKey(), String.valueOf(Duration.of(timeout, unit).toMillis()));
            return this;
        }

        /**
         * Sets the maximum number of connections that can be opened at the same time to a single server. Limit is not
         * a hard stop. It is done to prevent threads stuck inside a connection pool waiting for a connection.
         * Default is 10. It is recommended to set a higher value for a high concurrent applications. It will let
         * more threads to get a connection and execute a query.
         *
         * @param maxConnections - maximum number of connections
         */
        public Builder setMaxConnections(int maxConnections) {
            this.configuration.put(ClientConfigProperties.HTTP_MAX_OPEN_CONNECTIONS.getKey(), String.valueOf(maxConnections));
            return this;
        }

        /**
         * Sets how long any connection would be considered as active and able for a lease.
         * After this time connection will be marked for sweep and will not be returned from a pool.
         * Has more effect than keep-alive timeout.
         * @param timeout - time in unit
         * @param unit - time unit
         * @return
         */
        public Builder setConnectionTTL(long timeout, ChronoUnit unit) {
            this.configuration.put(ClientConfigProperties.CONNECTION_TTL.getKey(), String.valueOf(Duration.of(timeout, unit).toMillis()));
            return this;
        }

        /**
         * Sets keep alive timeout for a connection to override server value. If set to -1 then server value will be used.
         * Default is -1.
         * Doesn't override connection TTL value.
         * {@see Client#setConnectionTTL}
         * @param timeout - time in unit
         * @param unit - time unit
         * @return
         */
        public Builder setKeepAliveTimeout(long timeout, ChronoUnit unit) {
            this.configuration.put(ClientConfigProperties.HTTP_KEEP_ALIVE_TIMEOUT.getKey(), String.valueOf(Duration.of(timeout, unit).toMillis()));
            return this;
        }

        /**
         * Sets strategy of how connections are reuse.
         * Default is {@link ConnectionReuseStrategy#FIFO} to evenly distribute load between them.
         *
         * @param strategy - strategy for connection reuse
         * @return
         */
        public Builder setConnectionReuseStrategy(ConnectionReuseStrategy strategy) {
            this.configuration.put(ClientConfigProperties.CONNECTION_REUSE_STRATEGY.getKey(), strategy.name());
            return this;
        }

        // SOCKET SETTINGS

        /**
         * Default socket timeout in milliseconds. Timeout is applied to read and write operations.
         *
         * @param timeout - socket timeout in milliseconds
         */
        public Builder setSocketTimeout(long timeout) {
            this.configuration.put(ClientConfigProperties.SOCKET_OPERATION_TIMEOUT.getKey(), String.valueOf(timeout));
            return this;
        }

        /**
         * Default socket timeout in milliseconds. Timeout is applied to read and write operations.
         * Default is 0.
         *
         * @param timeout - socket timeout value
         * @param unit - time unit
         */
        public Builder setSocketTimeout(long timeout, ChronoUnit unit) {
            return this.setSocketTimeout(Duration.of(timeout, unit).toMillis());
        }

        /**
         * Default socket receive buffer size in bytes.
         *
         * @param size - socket receive buffer size in bytes
         */
        public Builder setSocketRcvbuf(long size) {
            this.configuration.put(ClientConfigProperties.SOCKET_RCVBUF_OPT.getKey(), String.valueOf(size));
            return this;
        }

        /**
         * Default socket send buffer size in bytes.
         *
         * @param size - socket send buffer size in bytes
         */
        public Builder setSocketSndbuf(long size) {
            this.configuration.put(ClientConfigProperties.SOCKET_SNDBUF_OPT.getKey(), String.valueOf(size));
            return this;
        }

        /**
         * Default socket reuse address option.
         *
         * @param value - socket reuse address option
         */
        public Builder setSocketReuseAddress(boolean value) {
            this.configuration.put(ClientConfigProperties.SOCKET_REUSEADDR_OPT.getKey(), String.valueOf(value));
            return this;
        }

        /**
         * Default socket keep alive option.If set to true socket will be kept alive
         * until terminated by one of the parties.
         *
         * @param value - socket keep alive option
         */
        public Builder setSocketKeepAlive(boolean value) {
            this.configuration.put(ClientConfigProperties.SOCKET_KEEPALIVE_OPT.getKey(), String.valueOf(value));
            return this;
        }

        /**
         * Default socket tcp_no_delay option. If set to true, disables Nagle's algorithm.
         *
         * @param value - socket tcp no delay option
         */
        public Builder setSocketTcpNodelay(boolean value) {
            this.configuration.put(ClientConfigProperties.SOCKET_TCP_NO_DELAY_OPT.getKey(), String.valueOf(value));
            return this;
        }

        /**
         * Default socket linger option. If set to true, socket will linger for the specified time.
         *
         * @param secondsToWait - socket linger time in seconds
         */
        public Builder setSocketLinger(int secondsToWait) {
            this.configuration.put(ClientConfigProperties.SOCKET_LINGER_OPT.getKey(), String.valueOf(secondsToWait));
            return this;
        }

        /**
         * Server response compression. If set to true server will compress the response.
         * Has most effect for read operations. Default is true.
         *
         * @param enabled - indicates if server response compression is enabled
         */
        public Builder compressServerResponse(boolean enabled) {
            this.configuration.put(ClientConfigProperties.COMPRESS_SERVER_RESPONSE.getKey(), String.valueOf(enabled));
            return this;
        }

        /**
         * Client request compression. If set to true client will compress the request.
         * Has most effect for write operations. Default is false.
         *
         * @param enabled - indicates if client request compression is enabled
         */
        public Builder compressClientRequest(boolean enabled) {
            this.configuration.put(ClientConfigProperties.COMPRESS_CLIENT_REQUEST.getKey(), String.valueOf(enabled));
            return this;
        }

        /**
         * Configures the client to use HTTP compression. In this case compression is controlled by
         * http headers. Client compression will set {@code Content-Encoding: lz4} header and server
         * compression will set {@code Accept-Encoding: lz4} header. Default is false.
         *
         * @param enabled - indicates if http compression is enabled
         * @return
         */
        public Builder useHttpCompression(boolean enabled) {
            this.configuration.put(ClientConfigProperties.USE_HTTP_COMPRESSION.getKey(), String.valueOf(enabled));
            return this;
        }

        /**
         * Tell client that compression will be handled by application.
         * @param enabled - indicates that feature is enabled.
         * @return
         */
        public Builder appCompressedData(boolean enabled) {
            this.configuration.put(ClientConfigProperties.APP_COMPRESSED_DATA.getKey(), String.valueOf(enabled));
            return this;
        }

        /**
         * Sets buffer size for uncompressed data in LZ4 compression.
         * For outgoing data it is the size of a buffer that will be compressed.
         * For incoming data it is the size of a buffer that will be decompressed.
         *
         * @param size - size of the buffer in bytes
         * @return
         */
        public Builder setLZ4UncompressedBufferSize(int size) {
            this.configuration.put(ClientConfigProperties.COMPRESSION_LZ4_UNCOMPRESSED_BUF_SIZE.getKey(), String.valueOf(size));
            return this;
        }

        /**
         * Disable native compression. If set to true then native compression will be disabled.
         * If from some reason the native compressor is not working then it can be disabled.
         * @param disable
         * @return
         */
        public Builder disableNativeCompression(boolean disable) {
            this.configuration.put(ClientConfigProperties.DISABLE_NATIVE_COMPRESSION.getKey(), String.valueOf(disable));
            return this;
        }

        /**
         * Sets the default database name that will be used by operations if not specified.
         * @param database - actual default database name.
         */
        public Builder setDefaultDatabase(String database) {
            this.configuration.put(ClientConfigProperties.DATABASE.getKey(), database);
            return this;
        }

        public Builder addProxy(ProxyType type, String host, int port) {
            ValidationUtils.checkNotNull(type, "type");
            ValidationUtils.checkNonBlank(host, "host");
            ValidationUtils.checkRange(port, 1, ValidationUtils.TCP_PORT_NUMBER_MAX, "port");

            this.configuration.put(ClientConfigProperties.PROXY_TYPE.getKey(), type.name());
            this.configuration.put(ClientConfigProperties.PROXY_HOST.getKey(), host);
            this.configuration.put(ClientConfigProperties.PROXY_PORT.getKey(), String.valueOf(port));
            return this;
        }

        public Builder setProxyCredentials(String user, String pass) {
            this.configuration.put("proxy_user", user);
            this.configuration.put("proxy_password", pass);
            return this;
        }

        /**
         * Sets the maximum time for operation to complete. By default, it is set to 3 hours.
         * @param timeout
         * @param timeUnit
         * @return
         */
        public Builder setExecutionTimeout(long timeout, ChronoUnit timeUnit) {
            this.configuration.put(ClientConfigProperties.MAX_EXECUTION_TIME.getKey(), String.valueOf(Duration.of(timeout, timeUnit).toMillis()));
            return this;
        }

        public Builder setHttpCookiesEnabled(boolean enabled) {
            //TODO: extract to settings string constants
            this.configuration.put("client.http.cookies_enabled", String.valueOf(enabled));
            return this;
        }

        /**
         * Defines path to the trust store file. It cannot be combined with
         * certificates. Either trust store or certificates should be used.
         * {@see setSSLTrustStorePassword} and {@see setSSLTrustStoreType}
         * @param path
         * @return
         */
        public Builder setSSLTrustStore(String path) {
            this.configuration.put(ClientConfigProperties.SSL_TRUST_STORE.getKey(), path);
            return this;
        }

        /**
         * Password for the SSL Trust Store.
         *
         * @param password
         * @return
         */
        public Builder setSSLTrustStorePassword(String password) {
            this.configuration.put(ClientConfigProperties.SSL_KEY_STORE_PASSWORD.getKey(), password);
            return this;
        }

        /**
         * Type of the SSL Trust Store. Usually JKS
         *
         * @param type
         * @return
         */
        public Builder setSSLTrustStoreType(String type) {
            this.configuration.put(ClientConfigProperties.SSL_KEYSTORE_TYPE.getKey(), type);
            return this;
        }

        /**
         * Defines path to the key store file. It cannot be combined with
         * certificates. Either key store or certificates should be used.
         *
         * {@see setSSLKeyStorePassword} and {@see setSSLKeyStoreType}
         * @param path
         * @return
         */
        public Builder setRootCertificate(String path) {
            this.configuration.put(ClientConfigProperties.CA_CERTIFICATE.getKey(), path);
            return this;
        }

        /**
         * Client certificate for mTLS.
         * @param path
         * @return
         */
        public Builder setClientCertificate(String path) {
            this.configuration.put(ClientConfigProperties.SSL_CERTIFICATE.getKey(), path);
            return this;
        }

        /**
         * Client key for mTLS.
         * @param path
         * @return
         */
        public Builder setClientKey(String path) {
            this.configuration.put(ClientConfigProperties.SSL_KEY.getKey(), path);
            return this;
        }

        /**
         * Configure client to use server timezone for date/datetime columns. Default is true.
         * If this options is selected then server timezone should be set as well.
         *
         * @param useServerTimeZone - if to use server timezone
         * @return
         */
        public Builder useServerTimeZone(boolean useServerTimeZone) {
            this.configuration.put(ClientConfigProperties.USE_SERVER_TIMEZONE.getKey(), String.valueOf(useServerTimeZone));
            return this;
        }

        /**
         * Configure client to use specified timezone. If set using server TimeZone should be
         * set to false
         *
         * @param timeZone
         * @return
         */
        public Builder useTimeZone(String timeZone) {
            this.configuration.put(ClientConfigProperties.USE_TIMEZONE.getKey(), timeZone);
            // switch using server timezone to false
            this.configuration.put(ClientConfigProperties.USE_SERVER_TIMEZONE.getKey(), String.valueOf(Boolean.FALSE));
            return this;
        }

        /**
         * Specify server timezone to use. If not set then UTC will be used.
         *
         * @param timeZone - server timezone
         * @return
         */
        public Builder setServerTimeZone(String timeZone) {
            this.configuration.put(ClientConfigProperties.SERVER_TIMEZONE.getKey(), timeZone);
            return this;
        }

        /**
         * Configures client to execute requests in a separate thread. By default, operations (query, insert)
         * are executed in the same thread as the caller.
         * It is possible to set a shared executor for all operations. See {@link #setSharedOperationExecutor(ExecutorService)}
         *
         * Note: Async operations a using executor what expects having a queue of tasks for a pool of executors.
         * The queue size limit is small it may quickly become a problem for scheduling new tasks.
         *
         * @param async - if to use async requests
         * @return
         */
        public Builder useAsyncRequests(boolean async) {
            this.configuration.put(ClientConfigProperties.ASYNC_OPERATIONS.getKey(), String.valueOf(async));
            return this;
        }

        /**
         * Sets an executor for running operations. If async operations are enabled and no executor is specified
         * client will create a default executor.
         * Executor will stay running after {@code Client#close() } is called. It is application responsibility to close
         * the executor.
         * @param executorService - executor service for async operations
         * @return
         */
        public Builder setSharedOperationExecutor(ExecutorService executorService) {
            this.sharedOperationExecutor = executorService;
            return this;
        }

        /**
         * Set size of a buffers that are used to read/write data from the server. It is mainly used to copy data from
         * a socket to application memory and visa-versa. Setting is applied for both read and write operations.
         * Default is 300,000 bytes.
         *
         * @param size - size in bytes
         * @return
         */
        public Builder setClientNetworkBufferSize(int size) {
            this.configuration.put(ClientConfigProperties.CLIENT_NETWORK_BUFFER_SIZE.getKey(), String.valueOf(size));
            return this;
        }


        /**
         * Sets list of causes that should be retried on.
         * Default {@code [NoHttpResponse, ConnectTimeout, ConnectionRequestTimeout, ServerRetryable]}
         * Use {@link ClientFaultCause#None} to disable retries.
         *
         * @param causes - list of causes
         * @return
         */
        public Builder retryOnFailures(ClientFaultCause ...causes) {
            StringJoiner joiner = new StringJoiner(VALUES_LIST_DELIMITER);
            for (ClientFaultCause cause : causes) {
                joiner.add(cause.name());
            }
            this.configuration.put(ClientConfigProperties.CLIENT_RETRY_ON_FAILURE.getKey(), joiner.toString());
            return this;
        }

        public Builder setMaxRetries(int maxRetries) {
            this.configuration.put(ClientConfigProperties.RETRY_ON_FAILURE.getKey(), String.valueOf(maxRetries));
            return this;
        }

        /**
         * Configures client to reuse allocated byte buffers for numbers. It affects how binary format reader is working.
         * If set to 'true' then {@link  Client#newBinaryFormatReader(QueryResponse)} will construct reader that will
         * reuse buffers for numbers. It improves performance for large datasets by reducing number of allocations
         * (therefore GC pressure).
         * Enabling this feature is safe because each reader suppose to be used by a single thread and readers are not reused.
         *
         * Default is false.
         * @param reuse - if to reuse buffers
         * @return
         */
        public Builder allowBinaryReaderToReuseBuffers(boolean reuse) {
            this.configuration.put(ClientConfigProperties.BINARY_READER_USE_PREALLOCATED_BUFFERS.getKey(), String.valueOf(reuse));
            return this;
        }

        /**
         * Defines list of headers that should be sent with each request. The Client will use a header value
         * defined in {@code headers} instead of any other.
         * Operation settings may override these headers.
         *
         * @see InsertSettings#httpHeaders(Map)
         * @see QuerySettings#httpHeaders(Map)
         * @see CommandSettings#httpHeaders(Map)
         * @param key - a name of the header.
         * @param value - a value of the header.
         * @return same instance of the builder
         */
        public Builder httpHeader(String key, String value) {
            this.configuration.put(ClientConfigProperties.httpHeader(key), value);
            return this;
        }

        /**
         * {@see #httpHeader(String, String)} but for multiple values.
         * @param key - name of the header
         * @param values - collection of values
         * @return same instance of the builder
         */
        public Builder httpHeader(String key, Collection<String> values) {
            this.configuration.put(ClientConfigProperties.httpHeader(key), ClientConfigProperties.commaSeparated(values));
            return this;
        }

        /**
         * {@see #httpHeader(String, String)} but for multiple headers.
         * @param headers - map of headers
         * @return same instance of the builder
         */
        public Builder httpHeaders(Map<String, String> headers) {
            headers.forEach(this::httpHeader);
            return this;
        }

        /**
         * Defines list of server settings that should be sent with each request. The Client will use a setting value
         * defined in {@code settings} instead of any other.
         * Operation settings may override these values.
         *
         * @see InsertSettings#serverSetting(String, String) (Map)
         * @see QuerySettings#serverSetting(String, String) (Map)
         * @see CommandSettings#serverSetting(String, String) (Map)
         * @param name - name of the setting without special prefix
         * @param value - value of the setting
         * @return same instance of the builder
         */
        public Builder serverSetting(String name, String value) {
            this.configuration.put(ClientConfigProperties.SERVER_SETTING_PREFIX + name, value);
            return this;
        }

        /**
         * {@see #serverSetting(String, String)} but for multiple values.
         * @param name - name of the setting without special prefix
         * @param values - collection of values
         * @return same instance of the builder
         */
        public Builder serverSetting(String name,  Collection<String> values) {
            this.configuration.put(ClientConfigProperties.SERVER_SETTING_PREFIX + name, ClientConfigProperties.commaSeparated(values));
            return this;
        }

        /**
         * Sets column to method matching strategy. It is used while registering POJO serializers and deserializers.
         * Default is {@link DefaultColumnToMethodMatchingStrategy}.
         *
         * @param strategy - matching strategy
         * @return same instance of the builder
         */
        public Builder columnToMethodMatchingStrategy(ColumnToMethodMatchingStrategy strategy) {
            this.columnToMethodMatchingStrategy = strategy;
            return this;
        }

        /**
         * Whether to use HTTP basic authentication. Default value is true.
         * Password that contain UTF8 characters may not be passed through http headers and BASIC authentication
         * is the only option here.
         * @param useBasicAuth - indicates if basic authentication should be used
         * @return same instance of the builder
         */
        public Builder useHTTPBasicAuth(boolean useBasicAuth) {
            this.configuration.put(ClientConfigProperties.HTTP_USE_BASIC_AUTH.getKey(), String.valueOf(useBasicAuth));
            return this;
        }

        /**
         * Sets additional information about calling application. This string will be passed to server as a client name.
         * In case of HTTP protocol it will be passed as a {@code User-Agent} header.
         * Warn: If custom value of User-Agent header is set it will override this value for HTTP transport
         * Client name is used by server to identify client application when investigating {@code system.query_log}. In case of HTTP
         * transport this value will be in the {@code system.query_log.http_user_agent} column. Currently only HTTP transport is used.
         *
         * @param clientName - client application display name.
         * @return same instance of the builder
         */
        public Builder setClientName(String clientName) {
            this.configuration.put(ClientConfigProperties.CLIENT_NAME.getKey(), clientName);
            return this;
        }

        /**
         * Sets client options from provided map. Values are copied as is
         * @param options - map of client options
         * @return same instance of the builder
         */
        public Builder setOptions(Map<String, String> options) {
            for (Map.Entry<String, String> entry : options.entrySet()) {
                setOption(entry.getKey(), entry.getValue());
            }
            return this;
        }

        /**
         * Specifies whether to use Bearer Authentication and what token to use.
         * The token will be sent as is, so it should be encoded before passing to this method.
         *
         * @param bearerToken - token to use
         * @return same instance of the builder
         */
        public Builder useBearerTokenAuth(String bearerToken) {
            // Most JWT libraries (https://jwt.io/libraries?language=Java) compact tokens in proper way
            this.httpHeader(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken);
            return this;
        }

        /**
         * Registers http client metrics with MeterRegistry.
         *
         * @param registry - metrics registry
         * @param name - name of metrics group
         * @return same instance of the builder
         */
        public Builder registerClientMetrics(Object registry, String name) {
            this.metricRegistry = registry;
            this.configuration.put(ClientConfigProperties.METRICS_GROUP_NAME.getKey(), name);
            return this;
        }

        /**
         * Sets server version that the client is interacting with.
         *
         * @param serverVersion - ClickHouse server version
         * @return same instance of the builder
         */
        public Builder setServerVersion(String serverVersion) {
            this.configuration.put(ClientConfigProperties.SERVER_VERSION.getKey(), serverVersion);
            return this;
        }

        /**
         * Defines mapping between ClickHouse data type and target Java type
         * Used by binary readers to convert values into desired Java type.
         * @param typeHintMapping - map between ClickHouse data type and Java class
         * @return this builder instance
         */
        public Builder typeHintMapping(Map<ClickHouseDataType, Class<?>> typeHintMapping) {
            this.configuration.put(ClientConfigProperties.TYPE_HINT_MAPPING.getKey(),
                    ClientConfigProperties.mapToString(typeHintMapping, (v) -> {
                        return ((Class<?>) v).getName();
                    }));
            return this;
        }


        /**
         * SNI SSL parameter that will be set for each outbound SSL socket.
         * SNI stands for Server Name Indication - an extension to the TLS protocol that allows multiple domains to share the same IP address.
         *
         * @param sni - SNI parameter
         * @return this builder instance
         */
        public Builder sslSocketSNI(String sni) {
            this.configuration.put(ClientConfigProperties.SSL_SOCKET_SNI.getKey(), sni);
            return this;
        }

        /**
         * Make sending statement parameters as HTTP Form data (in the body of a request).
         * Note: work only with Server side compression. If client compression is enabled it will be disabled
         * for query requests with parameters. It is because each parameter is sent as part of multipart content
         * what would require compressions of them separately.
         *
         * @param enable - if feature enabled
         * @return this builder instance
         */
        public Builder useHttpFormDataForQuery(boolean enable) {
            this.configuration.put(ClientConfigProperties.HTTP_SEND_PARAMS_IN_BODY.getKey(), String.valueOf(enable));
            return this;
        }

        /**
         * Sets query id generator. Will be used when operation settings (InsertSettings, QuerySettings) do not have query id set.
         * @param supplier
         * @return
         */
        public Builder setQueryIdGenerator(Supplier<String> supplier) {
            this.queryIdGenerator = supplier;
            return this;
        }

        public Client build() {
            // check if endpoint are empty. so can not initiate client
            if (this.endpoints.isEmpty()) {
                throw new IllegalArgumentException("At least one endpoint is required");
            }
            // check if username and password are empty. so can not initiate client?
            boolean useSslAuth = MapUtils.getFlag(this.configuration, ClientConfigProperties.SSL_AUTH.getKey());
            boolean hasAccessToken = this.configuration.containsKey(ClientConfigProperties.ACCESS_TOKEN.getKey());
            boolean hasUser = this.configuration.containsKey(ClientConfigProperties.USER.getKey());
            boolean hasPassword = this.configuration.containsKey(ClientConfigProperties.PASSWORD.getKey());
            boolean customHttpHeaders = this.configuration.containsKey(ClientConfigProperties.httpHeader(HttpHeaders.AUTHORIZATION));

            if (!(useSslAuth || hasAccessToken || hasUser || hasPassword || customHttpHeaders)) {
                throw new IllegalArgumentException("Username and password (or access token or SSL authentication or pre-define Authorization header) are required");
            }

            if (useSslAuth && (hasAccessToken || hasPassword)) {
                throw new IllegalArgumentException("Only one of password, access token or SSL authentication can be used per client.");
            }

            if (useSslAuth && !this.configuration.containsKey(ClientConfigProperties.SSL_CERTIFICATE.getKey())) {
                throw new IllegalArgumentException("SSL authentication requires a client certificate");
            }

            if (this.configuration.containsKey(ClientConfigProperties.SSL_TRUST_STORE.getKey()) &&
                this.configuration.containsKey(ClientConfigProperties.SSL_CERTIFICATE.getKey())) {
                throw new IllegalArgumentException("Trust store and certificates cannot be used together");
            }

            // Check timezone settings
            String useTimeZoneValue = this.configuration.get(ClientConfigProperties.USE_TIMEZONE.getKey());
            String serverTimeZoneValue = this.configuration.get(ClientConfigProperties.SERVER_TIMEZONE.getKey());
            boolean useServerTimeZone = MapUtils.getFlag(this.configuration, ClientConfigProperties.USE_SERVER_TIMEZONE.getKey());
            if (useTimeZoneValue != null) {
                if (useServerTimeZone) {
                    throw new IllegalArgumentException("USE_TIME_ZONE option cannot be used when using server timezone");
                }

                try {
                    LOG.debug("Using timezone: {} instead of server one", ZoneId.of(useTimeZoneValue));
                } catch (Exception e) {
                    throw new IllegalArgumentException("Invalid timezone value: " + useTimeZoneValue);
                }
            } else if (useServerTimeZone) {
                if (serverTimeZoneValue == null) {
                    serverTimeZoneValue = "UTC";
                }

                try {
                    LOG.debug("Using server timezone: {}", ZoneId.of(serverTimeZoneValue));
                } catch (Exception e) {
                    throw new IllegalArgumentException("Invalid server timezone value: " + serverTimeZoneValue);
                }
            } else {
                throw new IllegalArgumentException("Nor server timezone nor specific timezone is set");
            }

            return new Client(this.endpoints, this.configuration, this.sharedOperationExecutor,
                this.columnToMethodMatchingStrategy, this.metricRegistry, this.queryIdGenerator);
        }
    }

    /**
     * Pings the server to check if it is alive
     * @return true if the server is alive, false otherwise
     */
    public boolean ping() {
        return ping(getOperationTimeout());
    }

    /**
     * Pings the server to check if it is alive. Maximum timeout is 10 minutes.
     *
     * @param timeout timeout in milliseconds
     * @return true if the server is alive, false otherwise
     */
    public boolean ping(long timeout) {
        long startTime = System.nanoTime();
        try {
            CompletableFuture<QueryResponse> future = query("SELECT 1 FORMAT TabSeparated");
            try (QueryResponse response = timeout > 0 ? future.get(timeout, TimeUnit.MILLISECONDS) : future.get()) {
                return true;
            }
        } catch (Exception e) {
            LOG.debug("Failed to connect to the server (Duration: {})", durationSince(startTime), e);
            return false;
        }
    }

    /**
     * <p>Registers a POJO class and maps its fields to a table schema</p>
     * <p>Note: table schema will be stored in cache to be used while other operations. Cache key is
     *  {@link TableSchema schemaId}. Call this method
     * to update cache.</p>
     *
     * @param clazz - class of a POJO
     * @param schema - correlating table schema
     */
    public synchronized void register(Class<?> clazz, TableSchema schema) {
        String schemaKey;
        if (schema.getTableName() != null && schema.getQuery() == null) {
            schemaKey = schema.getTableName();
        } else if (schema.getQuery() != null && schema.getTableName() == null) {
            schemaKey = schema.getQuery();
        } else {
            throw new IllegalArgumentException("Table schema has both query and table name set. Only one is allowed.");
        }
        tableSchemaCache.put(schemaKey, schema);
        tableSchemaHasDefaults.put(schemaKey, schema.hasDefaults());

        pojoSerDe.registerClass(clazz, schema);
    }

    /**
     * <p>Sends write request to database. List of objects is converted into a most suitable format
     * then it is sent to a server. Members of the list must be pre-registered using
     * {@link #register(Class, TableSchema)} method:</p>
     *
     * <pre>{@code
     * client.register(SamplePOJO.class, tableSchema);
     * List<Object> input = new ArrayList<>();
     * // ... Insert some items into input list
     * Future<InsertResponse> response = client.insert(tableName, simplePOJOs, settings);
     * }
     * </pre>
     *
     * @param tableName - destination table name
     * @param data  - data stream to insert
     * @return {@code CompletableFuture<InsertResponse>} - a promise to insert response
     */
    public CompletableFuture<InsertResponse> insert(String tableName, List<?> data) {
        return insert(tableName, data, new InsertSettings());
    }

    /**
     * <p>Sends write request to database. List of objects is converted into a most suitable format
     * then it is sent to a server. Members of the list must be pre-registered using
     * {@link #register(Class, TableSchema)} method:</p>
     *
     * <pre>{@code
     * client.register(SamplePOJO.class, tableSchema);
     * List<Object> input = new ArrayList<>();
     * // ... Insert some items into input list
     * Future<InsertResponse> response = client.insert(tableName, simplePOJOs, settings);
     * }
     * </pre>
     *
     * @param tableName - destination table name
     * @param data  - data stream to insert
     * @param settings - insert operation settings
     * @throws IllegalArgumentException when data is empty or not registered
     * @return {@code CompletableFuture<InsertResponse>} - a promise to insert response
     */
    public CompletableFuture<InsertResponse> insert(String tableName, List<?> data, InsertSettings settings) {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Data cannot be empty");
        }
        //Add format to the settings
        if (settings == null) {
            settings = new InsertSettings();
        }

        final InsertSettings requestSettings = new InsertSettings(buildRequestSettings(settings.getAllSettings()));

        String operationId = registerOperationMetrics();
        requestSettings.setOperationId(operationId);
        globalClientStats.get(operationId).start(ClientMetrics.OP_DURATION);
        globalClientStats.get(operationId).start(ClientMetrics.OP_SERIALIZATION);


        boolean hasDefaults = this.tableSchemaHasDefaults.get(tableName);
        ClickHouseFormat format = hasDefaults? ClickHouseFormat.RowBinaryWithDefaults : ClickHouseFormat.RowBinary;
        requestSettings.setOption(ClientConfigProperties.INPUT_OUTPUT_FORMAT.getKey(), format);
        TableSchema tableSchema = tableSchemaCache.get(tableName);
        if (tableSchema == null) {
            throw new IllegalArgumentException("Table schema not found for table: " + tableName + ". Did you forget to register it?");
        }
        //Lookup the Serializer for the POJO
        Map<String, POJOFieldSerializer> classSerializers = pojoSerDe.getFieldSerializers(data.get(0).getClass(),
                tableSchema);
        List<POJOFieldSerializer> serializersForTable = new ArrayList<>();
        for (ClickHouseColumn column : tableSchema.getColumns()) {
            if (column.hasDefault() && column.getDefaultValue() != ClickHouseColumn.DefaultValue.DEFAULT ) {
                continue;
            }
            POJOFieldSerializer serializer = classSerializers.get(column.getColumnName());
            if (serializer == null) {
                throw new IllegalArgumentException("No serializer found for column '" + column.getColumnName() + "'. Did you forget to register it?");
            }
            serializersForTable.add(serializer);
        }


        Integer retry = (Integer) configuration.get(ClientConfigProperties.RETRY_ON_FAILURE.getKey());
        final int maxRetries = retry == null ? 0 : retry;

        requestSettings.setOption(ClientConfigProperties.INPUT_OUTPUT_FORMAT.getKey(), format);
        if (requestSettings.getQueryId() == null && queryIdGenerator != null) {
            requestSettings.setQueryId(queryIdGenerator.get());
        }
        Supplier<InsertResponse> supplier = () -> {
            long startTime = System.nanoTime();
            // Selecting some node
            Endpoint selectedEndpoint = getNextAliveNode();

            RuntimeException lastException = null;
            for (int i = 0; i <= maxRetries; i++) {
                // Execute request
                try (ClassicHttpResponse httpResponse =
                        httpClientHelper.executeRequest(selectedEndpoint, requestSettings.getAllSettings(),
                                out -> {
                                    out.write("INSERT INTO ".getBytes());
                                    out.write(tableName.getBytes());
                                    out.write(" \n FORMAT ".getBytes());
                                    out.write(format.name().getBytes());
                                    out.write(" \n".getBytes());
                                    for (Object obj : data) {

                                        for (POJOFieldSerializer serializer : serializersForTable) {
                                            try {
                                                serializer.serialize(obj, out);
                                            } catch (InvocationTargetException | IllegalAccessException | IOException e) {
                                                throw new DataSerializationException(obj, serializer, e);
                                            }
                                        }
                                    }
                                    out.close();
                                })) {


                    // Check response
                    if (httpResponse.getCode() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                        LOG.warn("Failed to get response. Server returned {}. Retrying. (Duration: {})", httpResponse.getCode(), durationSince(startTime));
                        selectedEndpoint = getNextAliveNode();
                        continue;
                    }

                    ClientStatisticsHolder clientStats = globalClientStats.remove(operationId);
                    OperationMetrics metrics = new OperationMetrics(clientStats);
                    String summary = HttpAPIClientHelper.getHeaderVal(httpResponse.getFirstHeader(ClickHouseHttpProto.HEADER_SRV_SUMMARY), "{}");
                    ProcessParser.parseSummary(summary, metrics);
                    String queryId =  HttpAPIClientHelper.getHeaderVal(httpResponse.getFirstHeader(ClickHouseHttpProto.HEADER_QUERY_ID), requestSettings.getQueryId(), String::valueOf);
                    metrics.operationComplete();
                    metrics.setQueryId(queryId);
                    return new InsertResponse(metrics, HttpAPIClientHelper.collectResponseHeaders(httpResponse));
                } catch (Exception e) {
                    String msg = requestExMsg("Insert", (i + 1), durationSince(startTime).toMillis(), requestSettings.getQueryId());
                    lastException = httpClientHelper.wrapException(msg, e, requestSettings.getQueryId());
                    if (httpClientHelper.shouldRetry(e, requestSettings.getAllSettings())) {
                        LOG.warn("Retrying.", e);
                        selectedEndpoint = getNextAliveNode();
                    } else {
                        throw lastException;
                    }
                }
            }

            String errMsg = requestExMsg("Insert", retries, durationSince(startTime).toMillis(), requestSettings.getQueryId());
            LOG.warn(errMsg);
            throw (lastException == null ? new ClientException(errMsg) : lastException);        };

        return runAsyncOperation(supplier, requestSettings.getAllSettings());

    }

    /**
     * <p>Sends write request to database. Input data is read from the input stream.</p>
     *
     * @param tableName - destination table name
     * @param data  - data stream to insert
     * @param format - format of the data in the stream
     * @return {@code CompletableFuture<InsertResponse>} - a promise to insert response
     */
    public CompletableFuture<InsertResponse> insert(String tableName, InputStream data, ClickHouseFormat format) {
        return insert(tableName, Collections.emptyList(), data, format, new InsertSettings());
    }

    /**
     * <p>Sends write request to database. Input data is read from the input stream.</p>
     *
     * @param tableName - destination table name
     * @param data  - data stream to insert
     * @param format - format of the data in the stream
     * @return {@code CompletableFuture<InsertResponse>} - a promise to insert response
     */
    public CompletableFuture<InsertResponse> insert(String tableName, List<String> columnNames, InputStream data, ClickHouseFormat format) {
        return insert(tableName, columnNames, data, format, new InsertSettings());
    }



    /**
     * Sends write request to database. Input data is read from the input stream.
     *
     * @param tableName - destination table name
     * @param data  - data stream to insert
     * @param format - format of the data in the stream
     * @param settings - insert operation settings
     * @return {@code CompletableFuture<InsertResponse>} - a promise to insert response
     */
    public CompletableFuture<InsertResponse> insert(String tableName,
                                                    InputStream data,
                                                    ClickHouseFormat format,
                                                    InsertSettings settings) {
        return insert(tableName, Collections.emptyList(), data, format, settings);
    }
    /**
     * Sends write request to database. Input data is read from the input stream.
     *
     * @param tableName - destination table name
     * @param columnNames - list of column names to insert data into. If null or empty, all columns will be used.
     * @param data  - data stream to insert
     * @param format - format of the data in the stream
     * @param settings - insert operation settings
     * @return {@code CompletableFuture<InsertResponse>} - a promise to insert response
     */
    public CompletableFuture<InsertResponse> insert(String tableName,
                                                    List<String> columnNames,
                                                    InputStream data,
                                                    ClickHouseFormat format,
                                                    InsertSettings settings) {

        final int writeBufferSize = settings.getInputStreamCopyBufferSize() <= 0 ?
                (int) configuration.get(ClientConfigProperties.CLIENT_NETWORK_BUFFER_SIZE.getKey()) :
                settings.getInputStreamCopyBufferSize();

        if (writeBufferSize <= 0) {
            throw new IllegalArgumentException("Buffer size must be greater than 0");
        }

        return insert(tableName, columnNames, new DataStreamWriter() {
                    @Override
                    public void onOutput(OutputStream out) throws IOException {
                        byte[] buffer = new byte[writeBufferSize];
                        int bytesRead;
                        while ((bytesRead = data.read(buffer)) > 0) {
                            out.write(buffer, 0, bytesRead);
                        }
                        out.close();
                    }

                    @Override
                    public void onRetry() throws IOException {
                        data.reset();
                    }
                },
                format, settings);
    }

    /**
     * Does an insert request to a server. Data is pushed when a {@link DataStreamWriter#onOutput(OutputStream)} is called.
     *
     * @param tableName - target table name
     * @param writer - {@link DataStreamWriter} implementation
     * @param format - source format
     * @param settings - operation settings
     * @return {@code CompletableFuture<InsertResponse>} - a promise to insert response
     */
    public CompletableFuture<InsertResponse> insert(String tableName,
                                                    DataStreamWriter writer,
                                                    ClickHouseFormat format,
                                                    InsertSettings settings) {
        return insert(tableName, Collections.emptyList(), writer, format, settings);
    }

    /**
     * Does an insert request to a server. Data is pushed when a {@link DataStreamWriter#onOutput(OutputStream)} is called.
     *
     * @param tableName - target table name
     * @param columnNames - list of column names to insert data into. If null or empty, all columns will be used.
     * @param writer - {@link DataStreamWriter} implementation
     * @param format - source format
     * @param settings - operation settings
     * @return {@code CompletableFuture<InsertResponse>} - a promise to insert response
     */
    public CompletableFuture<InsertResponse> insert(String tableName,
                                     List<String> columnNames,
                                     DataStreamWriter writer,
                                     ClickHouseFormat format,
                                     InsertSettings settings) {
        if (settings == null) {
            throw new  IllegalArgumentException("Settings cannot be null");
        }
        final InsertSettings requestSettings = new InsertSettings(buildRequestSettings(settings.getAllSettings()));
        requestSettings.setOption(ClientConfigProperties.INPUT_OUTPUT_FORMAT.getKey(), format);

        String operationId = requestSettings.getOperationId();
        ClientStatisticsHolder clientStats = null;
        if (operationId != null) {
            clientStats = globalClientStats.remove(operationId);
        }

        if (clientStats == null) {
            clientStats = new ClientStatisticsHolder();
        }
        clientStats.start(ClientMetrics.OP_DURATION);
        final ClientStatisticsHolder finalClientStats = clientStats;

        Supplier<InsertResponse> responseSupplier;

        final int writeBufferSize = requestSettings.getInputStreamCopyBufferSize() <= 0 ?
                (int) configuration.get(ClientConfigProperties.CLIENT_NETWORK_BUFFER_SIZE.getKey()) :
                requestSettings.getInputStreamCopyBufferSize();

        if (writeBufferSize <= 0) {
            throw new IllegalArgumentException("Buffer size must be greater than 0");
        }

        StringBuilder sqlStmt = new StringBuilder("INSERT INTO ").append(tableName);
        if (columnNames != null && !columnNames.isEmpty()) {
            sqlStmt.append(" (");
            for (String columnName : columnNames) {
                sqlStmt.append(columnName).append(", ");
            }
            sqlStmt.deleteCharAt(sqlStmt.length() - 2);
            sqlStmt.append(")");
        }
        sqlStmt.append(" FORMAT ").append(format.name());
        requestSettings.serverSetting(ClickHouseHttpProto.QPARAM_QUERY_STMT, sqlStmt.toString());
        if (requestSettings.getQueryId() == null && queryIdGenerator != null) {
            requestSettings.setQueryId(queryIdGenerator.get());
        }
        responseSupplier = () -> {
            long startTime = System.nanoTime();
            // Selecting some node
            Endpoint selectedEndpoint = getNextAliveNode();

            RuntimeException lastException = null;
            for (int i = 0; i <= retries; i++) {
                // Execute request
                try (ClassicHttpResponse httpResponse =
                             httpClientHelper.executeRequest(selectedEndpoint, requestSettings.getAllSettings(),
                                     out -> {
                                         writer.onOutput(out);
                                         out.close();
                                     })) {


                    // Check response
                    if (httpResponse.getCode() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                        LOG.warn("Failed to get response. Server returned {}. Retrying. (Duration: {})", httpResponse.getCode(), durationSince(startTime));
                        selectedEndpoint = getNextAliveNode();
                        continue;
                    }

                    OperationMetrics metrics = new OperationMetrics(finalClientStats);
                    String summary = HttpAPIClientHelper.getHeaderVal(httpResponse.getFirstHeader(ClickHouseHttpProto.HEADER_SRV_SUMMARY), "{}");
                    ProcessParser.parseSummary(summary, metrics);
                    String queryId =  HttpAPIClientHelper.getHeaderVal(httpResponse.getFirstHeader(ClickHouseHttpProto.HEADER_QUERY_ID), requestSettings.getQueryId(), String::valueOf);
                    metrics.operationComplete();
                    metrics.setQueryId(queryId);
                    return new InsertResponse(metrics, HttpAPIClientHelper.collectResponseHeaders(httpResponse));
                } catch (Exception e) {
                    String msg = requestExMsg("Insert", (i + 1), durationSince(startTime).toMillis(), requestSettings.getQueryId());
                    lastException = httpClientHelper.wrapException(msg, e, requestSettings.getQueryId());
                    if (httpClientHelper.shouldRetry(e, requestSettings.getAllSettings())) {
                        LOG.warn("Retrying.", e);
                        selectedEndpoint = getNextAliveNode();
                    } else {
                        throw lastException;
                    }
                }

                if (i < retries) {
                    try {
                        writer.onRetry();
                    } catch (IOException ioe) {
                        throw new ClientException("Failed to reset stream before next attempt", ioe);
                    }
                }
            }
            String errMsg = requestExMsg("Insert", retries, durationSince(startTime).toMillis(), requestSettings.getQueryId());
            LOG.warn(errMsg);
            throw (lastException == null ? new ClientException(errMsg) : lastException);
        };

        return runAsyncOperation(responseSupplier, requestSettings.getAllSettings());
    }

    /**
     * Sends SQL query to server. Default settings are applied.
     * @param sqlQuery - complete SQL query.
     * @return {@code CompletableFuture<QueryResponse>} - a promise to query response.
     */
    public CompletableFuture<QueryResponse> query(String sqlQuery) {
        return query(sqlQuery, null, null);
    }

    /**
     * <p>Sends SQL query to server.</p>
     * <b>Notes:</b>
     * <ul>
     * <li>Server response format can be specified thru `settings` or in SQL query.</li>
     * <li>If specified in both, the `sqlQuery` will take precedence.</li>
     * </ul>
     * @param sqlQuery - complete SQL query.
     * @param settings - query operation settings.
     * @return {@code CompletableFuture<QueryResponse>} - a promise to query response.
     */
    public CompletableFuture<QueryResponse> query(String sqlQuery, QuerySettings settings) {
        return query(sqlQuery, null, settings);
    }

    /**
     * <p>Sends SQL query to server with parameters. The map `queryParams` should contain keys that
     * match the placeholders in the SQL query.</p>
     * <p>For a parametrized query like:</p>
     * <pre>{@code
     * SELECT * FROM table WHERE int_column = {id:UInt8} and string_column = {phrase:String}
     * }
     * </pre>
     *
     * <p>Code to set the queryParams would be:</p>
     * <pre>{@code
     *      Map<String, Object> queryParams = new HashMap<>();
     *      queryParams.put("id", 1);
     *      queryParams.put("phrase", "hello");
     *      }
     * </pre>
     *
     * <b>Notes:</b>
     * <ul>
     * <li>Server response format can be specified through {@code settings} or in SQL query.</li>
     * <li>If specified in both, the {@code sqlQuery} will take precedence.</li>
     * </ul>
     *
     * @param sqlQuery - complete SQL query.
     * @param settings - query operation settings.
     * @param queryParams - query parameters that are sent to the server. (Optional)
     * @return {@code CompletableFuture<QueryResponse>} - a promise to query response.
     */
    public CompletableFuture<QueryResponse> query(String sqlQuery, Map<String, Object> queryParams, QuerySettings settings) {
        if (settings == null) {
            settings = new QuerySettings();
        }
        final QuerySettings requestSettings = new QuerySettings(buildRequestSettings(settings.getAllSettings()));

        if (requestSettings.getFormat() == null) {
            requestSettings.setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        }
        ClientStatisticsHolder clientStats = new ClientStatisticsHolder();
        clientStats.start(ClientMetrics.OP_DURATION);

        if (queryParams != null) {
            requestSettings.setOption(HttpAPIClientHelper.KEY_STATEMENT_PARAMS, queryParams);
        }

        if (requestSettings.getQueryId() == null && queryIdGenerator != null) {
            requestSettings.setQueryId(queryIdGenerator.get());
        }

        Supplier<QueryResponse> responseSupplier = () -> {
                long startTime = System.nanoTime();
                // Selecting some node
                Endpoint selectedEndpoint = getNextAliveNode();
                RuntimeException lastException = null;
                for (int i = 0; i <= retries; i++) {
                    ClassicHttpResponse httpResponse = null;
                    try {
                        boolean  useMultipart = ClientConfigProperties.HTTP_SEND_PARAMS_IN_BODY.getOrDefault(requestSettings.getAllSettings());
                        if (queryParams != null && useMultipart) {
                            httpResponse = httpClientHelper.executeMultiPartRequest(selectedEndpoint,
                                    requestSettings.getAllSettings(), sqlQuery);
                        } else {
                            httpResponse = httpClientHelper.executeRequest(selectedEndpoint,
                                    requestSettings.getAllSettings(),
                                    sqlQuery);
                        }
                        // Check response
                        if (httpResponse.getCode() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                            LOG.warn("Failed to get response. Server returned {}. Retrying. (Duration: {})", httpResponse.getCode(), durationSince(startTime));
                            selectedEndpoint = getNextAliveNode();
                            HttpAPIClientHelper.closeQuietly(httpResponse);
                            continue;
                        }

                        OperationMetrics metrics = new OperationMetrics(clientStats);
                        String summary = HttpAPIClientHelper.getHeaderVal(httpResponse
                                .getFirstHeader(ClickHouseHttpProto.HEADER_SRV_SUMMARY), "{}");
                        ProcessParser.parseSummary(summary, metrics);
                        String queryId = HttpAPIClientHelper.getHeaderVal(httpResponse
                                .getFirstHeader(ClickHouseHttpProto.HEADER_QUERY_ID), requestSettings.getQueryId());
                        metrics.setQueryId(queryId);
                        metrics.operationComplete();
                        Header formatHeader = httpResponse.getFirstHeader(ClickHouseHttpProto.HEADER_FORMAT);
                        ClickHouseFormat responseFormat = requestSettings.getFormat();
                        if (formatHeader != null) {
                            responseFormat = ClickHouseFormat.valueOf(formatHeader.getValue());
                        }

                        return new QueryResponse(httpResponse, responseFormat, requestSettings, metrics,
                                HttpAPIClientHelper.collectResponseHeaders(httpResponse));

                    } catch (Exception e) {
                        HttpAPIClientHelper.closeQuietly(httpResponse);
                        String msg = requestExMsg("Query", (i + 1), durationSince(startTime).toMillis(), requestSettings.getQueryId());
                        lastException = httpClientHelper.wrapException(msg, e, requestSettings.getQueryId());
                        if (httpClientHelper.shouldRetry(e, requestSettings.getAllSettings())) {
                            LOG.warn("Retrying.", e);
                            selectedEndpoint = getNextAliveNode();
                        } else {
                            throw lastException;
                        }
                    }
                }
                String errMsg = requestExMsg("Query", retries, durationSince(startTime).toMillis(), requestSettings.getQueryId());
                LOG.warn(errMsg);
                throw (lastException == null ? new ClientException(errMsg) : lastException);
            };

        return runAsyncOperation(responseSupplier, requestSettings.getAllSettings());
    }
    public CompletableFuture<QueryResponse> query(String sqlQuery, Map<String, Object> queryParams) {
        return query(sqlQuery, queryParams, null);
    }

    /**
     * <p>Queries data in one of descriptive format and creates a reader out of the response stream.</p>
     * <p>Format is selected internally so is ignored when passed in settings. If query contains format
     * statement then it may cause incompatibility error.</p>
     *
     * @param sqlQuery - SQL statement
     * @return - a promise to a result
     */
    public CompletableFuture<Records> queryRecords(String sqlQuery) {
        return queryRecords(sqlQuery, null, null);
    }

    /**
     * <p>Queries data in one of descriptive format and creates a reader out of the response stream.</p>
     * <p>Format is selected internally so is ignored when passed in settings. If query contains format
     * statement then it may cause incompatibility error.</p>
     *
     * @param sqlQuery - SQL statement
     * @param settings - operation settings
     * @return - a promise to a result
     */
    public CompletableFuture<Records> queryRecords(String sqlQuery, QuerySettings settings) {
        return queryRecords(sqlQuery, null, settings);
    }

    /**
     * <p>Queries data in one of descriptive format and creates a reader out of the response stream.</p>
     * <p>Format is selected internally so is ignored when passed in settings. If query contains format
     * statement then it may cause incompatibility error.</p>
     * See {@link #query(String, Map, QuerySettings)} for parametrized queries.
     * @param sqlQuery - SQL statement
     * @param params - sql parameters
     * @param settings - operation settings
     * @return - a promise to a result
     */
    public CompletableFuture<Records> queryRecords(String sqlQuery, Map<String, Object> params, QuerySettings settings) {
        if (settings == null) {
            settings = new QuerySettings();
        }
        final QuerySettings requestSettings = new QuerySettings(buildRequestSettings(settings.getAllSettings()));
        requestSettings.setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        requestSettings.waitEndOfQuery(true); // we rely on the summery

        return query(sqlQuery, params, requestSettings).thenApply(response -> {
            try {

                return new Records(response, newBinaryFormatReader(response));
            } catch (Exception e) {
                throw new ClientException("Failed to get query response", e);
            }
        });
    }

    public CompletableFuture<Records> queryRecords(String sqlQuery, Map<String, Object> params) {
        return queryRecords(sqlQuery, params, null);
    }

    /**
     * <p>Queries data in descriptive format and reads result to a collection.</p>
     * <p>Use this method for queries that would return only a few records only because client
     * will read whole dataset and convert it into a list of GenericRecord</p>
     *
     * See {@link #query(String, Map, QuerySettings)} for parametrized queries.
     * @param sqlQuery - SQL statement
     * @param params - query parameters
     * @param settings  - operation settings
     * @return - complete list of records
     */
    public List<GenericRecord> queryAll(String sqlQuery, Map<String, Object> params, QuerySettings settings) {
        if (settings == null) {
            settings = new QuerySettings();
        }
        try {
            int operationTimeout = getOperationTimeout();
            final QuerySettings requestSettings = new QuerySettings(buildRequestSettings(settings.getAllSettings()));
            requestSettings.setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
            requestSettings.waitEndOfQuery(true);

            CompletableFuture<QueryResponse> f = query(sqlQuery, params, requestSettings);
            try (QueryResponse response = operationTimeout == 0 ? f.get() : f.get(operationTimeout, TimeUnit.MILLISECONDS)) {
                List<GenericRecord> records = new ArrayList<>();
                RowBinaryWithNamesAndTypesFormatReader reader =
                        (RowBinaryWithNamesAndTypesFormatReader) newBinaryFormatReader(response);

                Map<String, Object> record;
                while (reader.readRecord((record = new LinkedHashMap<>()))) {
                    records.add(new MapBackedRecord(record, reader.getConvertions(), reader.getSchema()));
                }
                return records;
            }
        } catch (ExecutionException e) {
            throw new ClientException("Failed to get query response", e.getCause());
        } catch (Exception e) {
            throw new ClientException("Failed to get query response", e);
        }
    }

    public List<GenericRecord> queryAll(String sqlQuery, QuerySettings settings) {
        return queryAll(sqlQuery, null, settings);
    }

    public List<GenericRecord> queryAll(String sqlQuery, Map<String, Object> params) {
        return queryAll(sqlQuery, params, null);
    }

    public List<GenericRecord> queryAll(String sqlQuery) {
        return queryAll(sqlQuery, null, (QuerySettings) null);
    }

    public <T> List<T> queryAll(String sqlQuery, Class<T> clazz, TableSchema schema) {
        return queryAll(sqlQuery, clazz, schema, null);
    }

    /**
     * WARNING: Experimental API
     *
     * <p>Queries data and returns collection with whole result. Data is read directly to a DTO
     *  to save memory on intermediate structures. DTO will be instantiated with default constructor or
     *  by using allocator</p>
     * <p>{@code class} should be registered before calling this method using {@link #register(Class, TableSchema)}</p>
     * <p>Internally deserializer is compiled at the register stage. Compilation is done using ASM library by
     *  writing a bytecode</p>
     * <p>Note: this method will cache schema and it will use sql as a key for storage.</p>
     *
     *
     * @param sqlQuery - query to execute
     * @param clazz - class of the DTO
     * @param allocator - optional supplier to create new instances of the DTO.
     * @throws IllegalArgumentException when class is not registered or no setters found
     * @return List of POJOs filled with data
     * @param <T> type of POJO
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> queryAll(String sqlQuery, Class<T> clazz, TableSchema schema, Supplier<T> allocator) {
        Map<String, POJOFieldDeserializer> classDeserializers = pojoSerDe.getFieldDeserializers(clazz, schema);

        if (classDeserializers.isEmpty()) {
            throw new IllegalArgumentException("No deserializers found for the query and class '" + clazz + "'. Did you forget to register it?");
        }

        try {
            int operationTimeout = getOperationTimeout();
            QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
            CompletableFuture<QueryResponse> f = query(sqlQuery, settings);
            try (QueryResponse response = operationTimeout == 0 ? f.get() : f.get(operationTimeout, TimeUnit.MILLISECONDS)) {
                List<T> records = new ArrayList<>();
                RowBinaryWithNamesAndTypesFormatReader reader =
                        (RowBinaryWithNamesAndTypesFormatReader) newBinaryFormatReader(response);

                while (true) {

                    Object record;
                    try {
                        record = allocator == null ? clazz.getDeclaredConstructor().newInstance() : allocator.get();
                    } catch (NoSuchMethodException e) {
                        throw new ClientException("Failed to instantiate DTO to store data: no-args constructor is not defined");
                    }
                    if (reader.readToPOJO(classDeserializers, record)) {
                        records.add((T) record);
                    } else {
                        break;
                    }
                }

                return records;
            }
        } catch (ExecutionException e) {
            throw new ClientException("Failed to get query response", e.getCause());
        } catch (Exception e) {
            throw new ClientException("Failed to get query response", e);
        }
    }

    /**
     * <p>Fetches schema of a table and returns complete information about each column.
     * Information includes column name, type, default value, etc.</p>
     *
     * <p>See {@link #register(Class, TableSchema)}</p>
     *
     * @param table - table name
     * @return {@code TableSchema} - Schema of the table
     */
    public TableSchema getTableSchema(String table) {
        return getTableSchema(table, getDefaultDatabase());
    }

    /**
     * <p>Fetches schema of a table and returns complete information about each column.
     * Information includes column name, type, default value, etc.</p>
     * <p>See {@link #register(Class, TableSchema)}</p>
     *
     * @param table - table name
     * @param database - database name
     * @return {@code TableSchema} - Schema of the table
     */
    public TableSchema getTableSchema(String table, String database) {
        final String sql = "DESCRIBE TABLE " + table + " FORMAT " + ClickHouseFormat.TSKV.name();
        return getTableSchemaImpl(sql, table, null, database, null);
    }

    /**
     * <p>Creates table schema from a query.</p>
     * @param sql - SQL query which schema to return
     * @return table schema for the query
     */
    public TableSchema getTableSchemaFromQuery(String sql) {
        return getTableSchemaFromQuery(sql, null);
    }

    public TableSchema getTableSchemaFromQuery(String sql, Map<String, Object> params) {
        final String describeQuery = "DESC (" + sql + ") FORMAT " + ClickHouseFormat.TSKV.name();
        return getTableSchemaImpl(describeQuery, null, sql, getDefaultDatabase(), params);
    }

    private TableSchema getTableSchemaImpl(
            String describeQuery, String name, String originalQuery, String database, Map<String, Object> queryParams) {
        int operationTimeout = getOperationTimeout();

        QuerySettings settings = new QuerySettings().setDatabase(database);
        try (QueryResponse response = operationTimeout == 0
                ? query(describeQuery, queryParams, settings).get()
                : query(describeQuery, queryParams, settings).get(getOperationTimeout(), TimeUnit.SECONDS)) {
            return TableSchemaParser.readTSKV(response.getInputStream(), name, originalQuery, database);
        } catch (TimeoutException e) {
            throw new ClientException("Operation has likely timed out after " + getOperationTimeout() + " seconds.", e);
        } catch (ExecutionException e) {
            throw new ClientException("Failed to get table schema", e.getCause());
        } catch (ServerException e) {
            throw e;
        } catch (Exception e) {
            throw new ClientException("Failed to get table schema", e);
        }
    }

    /**
     * <p>Executes a SQL command and doesn't care response. Useful for DDL statements, like `CREATE`, `DROP`, `ALTER`.
     * Method however returns execution errors from a server or summary in case of successful execution. </p>
     *
     * @param sql      - SQL command
     * @param settings - execution settings
     * @return {@code CompletableFuture<CommandResponse>} - a promise to command response
     */
    public CompletableFuture<CommandResponse> execute(String sql, CommandSettings settings) {
        return query(sql, settings)
                .thenApplyAsync(response -> {
                    try {
                        return new CommandResponse(response);
                    } catch (Exception e) {
                        throw new ClientException("Failed to get command response", e);
                    }
                });
    }

    /**
     * <p>Executes a SQL command and doesn't care response. Useful for DDL statements, like `CREATE`, `DROP`, `ALTER`.
     * Method however returns execution errors from a server or summary in case of successful execution. </p>
     *
     * @param sql      - SQL command
     * @param params   - query parameters
     * @return {@code CompletableFuture<CommandResponse>} - a promise to command response
     */
    public CompletableFuture<CommandResponse> execute(String sql, Map<String, Object> params){
        return query(sql, params)
                .thenApplyAsync(response -> {
                    try {
                        return new CommandResponse(response);
                    } catch (Exception e) {
                        throw new ClientException("Failed to get command response", e);
                    }
                });
    }

    /**
     * <p>Executes a SQL command and doesn't care response. Useful for DDL statements, like `CREATE`, `DROP`, `ALTER`.
     * Method however returns execution errors from a server or summary in case of successful execution. </p>
     *
     * @param sql      - SQL command
     * @param params   - query parameters
     * @param settings  - execution settings
     * @return {@code CompletableFuture<CommandResponse>} - a promise to command response
     */
    public CompletableFuture<CommandResponse> execute(String sql, Map<String, Object> params, CommandSettings settings){
        return query(sql, params, settings)
                .thenApplyAsync(response -> {
                    try {
                        return new CommandResponse(response);
                    } catch (Exception e) {
                        throw new ClientException("Failed to get command response", e);
                    }
                });
    }

    /**
     * <p>Executes a SQL command and doesn't care response. Useful for DDL statements, like `CREATE`, `DROP`, `ALTER`.
     * Method however returns execution errors from a server or summary in case of successful execution. </p>
     *
     * @param sql - SQL command
     * @return {@code CompletableFuture<CommandResponse>} - a promise to command response
     */
    public CompletableFuture<CommandResponse> execute(String sql) {
        return query(sql)
                .thenApply(response -> {
                    try {
                        return new CommandResponse(response);
                    } catch (Exception e) {
                        throw new ClientException("Failed to get command response", e);
                    }
                });
    }

    /**
     * <p>Create an instance of {@link ClickHouseBinaryFormatReader} based on response. Table schema is option and only
     *  required for {@link ClickHouseFormat#RowBinaryWithNames}, {@link ClickHouseFormat#RowBinary}.
     *  Format {@link ClickHouseFormat#RowBinaryWithDefaults} is not supported for output (read operations).</p>
     * @param response
     * @param schema
     * @return
     */
    public ClickHouseBinaryFormatReader newBinaryFormatReader(QueryResponse response, TableSchema schema) {
        ClickHouseBinaryFormatReader reader = null;
        // Using caching buffer allocator is risky so this parameter is not exposed to the user
        boolean useCachingBufferAllocator = MapUtils.getFlag(configuration, "client_allow_binary_reader_to_reuse_buffers", false);
        BinaryStreamReader.ByteBufferAllocator byteBufferPool = useCachingBufferAllocator ?
                new BinaryStreamReader.CachingByteBufferAllocator() :
                new BinaryStreamReader.DefaultByteBufferAllocator();
        switch (response.getFormat()) {
            case Native:
                reader = new NativeFormatReader(response.getInputStream(), response.getSettings(),
                        byteBufferPool, typeHintMapping);
                break;
            case RowBinaryWithNamesAndTypes:
                reader = new RowBinaryWithNamesAndTypesFormatReader(response.getInputStream(), response.getSettings(), byteBufferPool, typeHintMapping);
                break;
            case RowBinaryWithNames:
                reader = new RowBinaryWithNamesFormatReader(response.getInputStream(), response.getSettings(), schema, byteBufferPool, typeHintMapping);
                break;
            case RowBinary:
                reader = new RowBinaryFormatReader(response.getInputStream(), response.getSettings(), schema,
                        byteBufferPool, typeHintMapping);
                break;
            default:
                throw new IllegalArgumentException("Binary readers doesn't support format: " + response.getFormat());
        }
        return reader;
    }

    public ClickHouseBinaryFormatReader newBinaryFormatReader(QueryResponse response) {
        return  newBinaryFormatReader(response, null);
    }

    private String registerOperationMetrics() {
        String operationId = UUID.randomUUID().toString();
        globalClientStats.put(operationId, new ClientStatisticsHolder());
        return operationId;
    }

    private <T> CompletableFuture<T> runAsyncOperation(Supplier<T> resultSupplier, Map<String, Object> requestSettings) {
        boolean isAsync = MapUtils.getFlag(requestSettings, configuration, ClientConfigProperties.ASYNC_OPERATIONS.getKey());
        if (isAsync) {
            return sharedOperationExecutor == null ? CompletableFuture.supplyAsync(resultSupplier) :
                    CompletableFuture.supplyAsync(resultSupplier, sharedOperationExecutor);
        }
        return CompletableFuture.completedFuture(resultSupplier.get());
    }

    @Override
    public String toString() {
        return "Client{" +
                "endpoints=" + endpoints +
                ",lz4factory" + lz4Factory +
                '}';
    }

    /**
     * Returns unmodifiable map of configuration options.
     * @return - configuration options
     */
    public Map<String, String> getConfiguration() {
        return readOnlyConfig;
    }

    /** Returns operation timeout in seconds */
    protected int getOperationTimeout() {
        return ClientConfigProperties.MAX_EXECUTION_TIME.getOrDefault(configuration);
    }

    /**
     * Returns unmodifiable set of endpoints.
     * @return - set of endpoints
     * @deprecated
     */
    @Deprecated
    public Set<String> getEndpoints() {
        return endpoints.stream().map(endpoint -> endpoint.getURI().toString()).collect(Collectors.toSet());
    }

    public String getUser() {
        return dbUser;
    }

    public String getServerVersion() {
        return this.serverVersion;
    }

    public String getServerTimeZone() {
        TimeZone tz = (TimeZone) this.configuration.get(ClientConfigProperties.SERVER_TIMEZONE.getKey());
        return tz == null ? null : tz.getID();
    }

    public String getClientVersion() {
        return clientVersion;
    }

    /**
     * Sets list of DB roles that should be applied to each query.
     *
     * @param dbRoles
     */
    public void setDBRoles(Collection<String> dbRoles) {
        List<String> tmp = new ArrayList<>(dbRoles);
        this.configuration.put(ClientConfigProperties.SESSION_DB_ROLES.getKey(), tmp);
        this.unmodifiableDbRolesView = ImmutableList.copyOf(tmp);
    }

    public void updateClientName(String name) {
        this.configuration.put(ClientConfigProperties.CLIENT_NAME.getKey(), name);
    }

    public static final String clientVersion =
            ClickHouseClientOption.readVersionFromResource("client-v2-version.properties");
    public static final String CLIENT_USER_AGENT = "clickhouse-java-v2/";

    private Collection<String> unmodifiableDbRolesView = Collections.emptyList();

    /**
     * Returns list of DB roles that should be applied to each query.
     *
     * @return List of DB roles
     */
    public Collection<String> getDBRoles() {
        return unmodifiableDbRolesView;
    }

    public void updateBearerToken(String bearer) {
        this.configuration.put(ClientConfigProperties.httpHeader(HttpHeaders.AUTHORIZATION), "Bearer " + bearer);
    }

    private Endpoint getNextAliveNode() {
        return endpoints.get(0);
    }

    public static final String VALUES_LIST_DELIMITER = ",";

    /**
     * Produces a merge of operation and client settings.
     * Operation settings override client settings
     * @param opSettings - operation settings
     * @return request settings - merged client and operation settings
     */
    private Map<String, Object> buildRequestSettings(Map<String, Object> opSettings) {
        Map<String, Object> requestSettings = new HashMap<>();
        requestSettings.putAll(configuration);
        requestSettings.putAll(opSettings);
        return requestSettings;
    }

    private Duration durationSince(long sinceNanos) {
        return Duration.ofNanos(System.nanoTime() - sinceNanos);
    }

    private String requestExMsg(String operation, int attempt, long operationDuration, String queryId) {
        return operation + " request failed (attempt: " + attempt +", duration: " + operationDuration + "ms, queryId: " + queryId + ")";
    }
}
