package com.clickhouse.client.api;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.command.CommandResponse;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.MapBackedRecord;
import com.clickhouse.client.api.data_formats.internal.ProcessParser;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.enums.ProxyType;
import com.clickhouse.client.api.insert.DataSerializationException;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.insert.POJOSerializer;
import com.clickhouse.client.api.insert.SerializerNotFoundException;
import com.clickhouse.client.api.internal.ClientStatisticsHolder;
import com.clickhouse.client.api.internal.ClientV1AdaptorHelper;
import com.clickhouse.client.api.internal.HttpAPIClientHelper;
import com.clickhouse.client.api.internal.SerializerUtils;
import com.clickhouse.client.api.internal.SettingsConverter;
import com.clickhouse.client.api.internal.TableSchemaParser;
import com.clickhouse.client.api.internal.ValidationUtils;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.http.ClickHouseHttpProto;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.format.BinaryStreamUtils;
import org.apache.hc.core5.concurrent.DefaultThreadFactory;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.NoHttpResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * <p>Client is the starting point for all interactions with ClickHouse. </p>
 *
 * <p>{@link Builder} is designed to construct a client object with required configuration:</p>
 *  <pre>{@code
 *
 *  Client client = new Client.Builder()
 *        .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort())
 *        .addUsername("default")
 *        .build();
 *        }
 *  </pre>
 *
 *
 * <p>When client object is created any operation method can be called on it:</p>
 * <pre>{@code
 *
 *  if (client.ping()) {
 *      QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
 *      Future<QueryResponse> response = client.query("SELECT * FROM " + table, settings);
 *      QueryResponse queryResponse = response.get();
 *  }
 *  }
 * </pre>
 *
 *
 * <p>Client is thread-safe. It uses exclusive set of object to perform an operation.</p>
 *
 */
public class Client implements AutoCloseable {
    private HttpAPIClientHelper httpClientHelper = null;

    private Set<String> endpoints;
    private Map<String, String> configuration;
    private List<ClickHouseNode> serverNodes = new ArrayList<>();
    private Map<Class<?>, List<POJOSerializer>> serializers; //Order is important to preserve for RowBinary
    private Map<Class<?>, Map<String, Method>> getterMethods;
    private Map<Class<?>, Boolean> hasDefaults; // Whether the POJO has defaults
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private ExecutorService sharedOperationExecutor;

    private Map<String, ClientStatisticsHolder> globalClientStats = new ConcurrentHashMap<>();

    private boolean useNewImplementation = false;


    private Client(Set<String> endpoints, Map<String,String> configuration, boolean useNewImplementation) {
        this.endpoints = endpoints;
        this.configuration = configuration;
        this.endpoints.forEach(endpoint -> {
            this.serverNodes.add(ClickHouseNode.of(endpoint, this.configuration));
        });
        this.serializers = new HashMap<>();
        this.getterMethods = new HashMap<>();
        this.hasDefaults = new HashMap<>();

        this.sharedOperationExecutor = Executors.newCachedThreadPool(new DefaultThreadFactory("chc-operation"));
        this.useNewImplementation = useNewImplementation;
        if (useNewImplementation) {
            this.httpClientHelper = new HttpAPIClientHelper(configuration);
        }
    }

    /**
     * Returns default database name that will be used by operations if not specified.
     *
     * @return String - actual default database name.
     */
    public String getDefaultDatabase() {
        return this.configuration.get("database");
    }


    /**
     * Frees the resources associated with the client.
     * <ul>
     *     <li>Shuts down the shared operation executor by calling {@code shutdownNow()}</li>
     * </ul>
     */
    @Override
    public void close() {
        try {
            if (!sharedOperationExecutor.isShutdown()) {
                this.sharedOperationExecutor.shutdownNow();
            }
        } catch (Exception e) {
            LOG.error("Failed to close shared operation executor", e);
        }
    }

    public static class Builder {
        private Set<String> endpoints;

        // Read-only configuration
        private Map<String, String> configuration;
        private boolean useNewImplementation = false;

        public Builder() {
            this.endpoints = new HashSet<>();
            this.configuration = new HashMap<String, String>();
            // TODO: set defaults configuration values
            this.setConnectTimeout(30, SECONDS)
                    .setSocketTimeout(2, SECONDS)
                    .setSocketRcvbuf(804800)
                    .setSocketSndbuf(804800)
                    .compressServerResponse(true)
                    .compressClientRequest(false);
        }

        /**
         * Server address to which client may connect. If there are multiple endpoints then client will
         * connect to one of them.
         * Acceptable formats are:
         * <ul>
         *     <li>{@code http://localhost:8123}</li>
         *     <li>{@code https://localhost:8443}</li>
         * </ul>
         *
         * @param endpoint - URL formatted string with protocol, host and port.
         */
        public Builder addEndpoint(String endpoint) {
            try {
                URL endpointURL = new java.net.URL(endpoint);
                if (!(endpointURL.getProtocol().equalsIgnoreCase("https") ||
                        endpointURL.getProtocol().equalsIgnoreCase("http"))) {
                    throw new IllegalArgumentException("Only HTTP and HTTPS protocols are supported");
                }
            } catch (java.net.MalformedURLException e) {
                throw new IllegalArgumentException("Endpoint should be a valid URL string", e);
            }
            if (endpoint.startsWith("https://")) {
                this.configuration.put(ClickHouseClientOption.SSL.getKey(), "true");
            }
            this.endpoints.add(endpoint);
            return this;
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
            ValidationUtils.checkNonBlank(host, "host");
            ValidationUtils.checkNotNull(protocol, "protocol");
            ValidationUtils.checkRange(port, 1, ValidationUtils.TCP_PORT_NUMBER_MAX, "port");
            if (secure) {
                // For some reason com.clickhouse.client.http.ApacheHttpConnectionImpl.newConnection checks only client config
                // for SSL, so we need to set it here. But it actually should be set for each node separately.
                this.configuration.put(ClickHouseClientOption.SSL.getKey(), "true");
            }
            String endpoint = String.format("%s%s://%s:%d", protocol.toString().toLowerCase(), secure ? "s": "", host, port);
            this.addEndpoint(endpoint);
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
            return this;
        }

        /**
         * Username for authentication with server. Required for all operations.
         * Same username will be used for all endpoints.
         *
         * @param username - a valid username
         */
        public Builder setUsername(String username) {
            this.configuration.put("user", username);
            return this;
        }

        /**
         * Password for authentication with server. Required for all operations.
         * Same password will be used for all endpoints.
         *
         * @param password - plain text password
         */
        public Builder setPassword(String password) {
            this.configuration.put("password", password);
            return this;
        }

        /**
         * Access token for authentication with server. Required for all operations.
         * Same access token will be used for all endpoints.
         *
         * @param accessToken - plain text access token
         */
        public Builder setAccessToken(String accessToken) {
            this.configuration.put("access_token", accessToken);
            return this;
        }

        // SOCKET SETTINGS

        /**
         * Default connection timeout in milliseconds. Timeout is applied to establish a connection.
         *
         * @param timeout - connection timeout in milliseconds
         */
        public Builder setConnectTimeout(long timeout) {
            this.configuration.put("connect_timeout", String.valueOf(timeout));
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
         * Default socket timeout in milliseconds. Timeout is applied to read and write operations.
         *
         * @param timeout - socket timeout in milliseconds
         */
        public Builder setSocketTimeout(long timeout) {
            this.configuration.put("socket_timeout", String.valueOf(timeout));
            return this;
        }

        /**
         * Default socket timeout in milliseconds. Timeout is applied to read and write operations.
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
            this.configuration.put("socket_rcvbuf", String.valueOf(size));
            return this;
        }

        /**
         * Default socket send buffer size in bytes.
         *
         * @param size - socket send buffer size in bytes
         */
        public Builder setSocketSndbuf(long size) {
            this.configuration.put("socket_sndbuf", String.valueOf(size));
            return this;
        }

        /**
         * Default socket reuse address option.
         *
         * @param value - socket reuse address option
         */
        public Builder setSocketReuseAddress(boolean value) {
            this.configuration.put("socket_reuseaddr", String.valueOf(value));
            return this;
        }

        /**
         * Default socket keep alive option.If set to true socket will be kept alive
         * until terminated by one of the parties.
         *
         * @param value - socket keep alive option
         */
        public Builder setSocketKeepAlive(boolean value) {
            this.configuration.put("socket_keepalive", String.valueOf(value));
            return this;
        }

        /**
         * Default socket tcp_no_delay option. If set to true, disables Nagle's algorithm.
         *
         * @param value - socket tcp no delay option
         */
        public Builder setSocketTcpNodelay(boolean value) {
            this.configuration.put("socket_tcp_nodelay", String.valueOf(value));
            return this;
        }

        /**
         * Default socket linger option. If set to true, socket will linger for the specified time.
         *
         * @param secondsToWait - socket linger time in seconds
         */
        public Builder setSocketLinger(int secondsToWait) {
            this.configuration.put("socket_linger", String.valueOf(secondsToWait));
            return this;
        }

        /**
         * Server response compression. If set to true server will compress the response.
         * Has most effect for read operations.
         *
         * @param enabled - indicates if server response compression is enabled
         */
        public Builder compressServerResponse(boolean enabled) {
            this.configuration.put("compress", String.valueOf(enabled));
            return this;
        }

        /**
         * Client request compression. If set to true client will compress the request.
         * Has most effect for write operations.
         *
         * @param enabled - indicates if client request compression is enabled
         */
        public Builder compressClientRequest(boolean enabled) {
            this.configuration.put("decompress", String.valueOf(enabled));
            return this;
        }

        /**
         * Sets the default database name that will be used by operations if not specified.
         * @param database - actual default database name.
         */
        public Builder setDefaultDatabase(String database) {
            this.configuration.put("database", database);
            return this;
        }

        public Builder addProxy(ProxyType type, String host, int port) {
            ValidationUtils.checkNotNull(type, "type");
            ValidationUtils.checkNonBlank(host, "host");
            ValidationUtils.checkRange(port, 1, ValidationUtils.TCP_PORT_NUMBER_MAX, "port");

            this.configuration.put(ClickHouseClientOption.PROXY_TYPE.getKey(), type.toString());
            this.configuration.put(ClickHouseClientOption.PROXY_HOST.getKey(), host);
            this.configuration.put(ClickHouseClientOption.PROXY_PORT.getKey(), String.valueOf(port));
            return this;
        }

        /**
         * Sets the maximum time for operation to complete. By default, it is set to 3 hours.
         * @param timeout
         * @param timeUnit
         * @return
         */
        public Builder setExecutionTimeout(long timeout, TimeUnit timeUnit) {
            this.configuration.put(ClickHouseClientOption.MAX_EXECUTION_TIME.getKey(), String.valueOf(timeUnit.toMillis(timeout)));
            return this;
        }

        /**
         * Switches to new implementation of the client.
         * @deprecated - do not use - it is only for development
         */
        public Builder useNewImplementation(boolean useNewImplementation) {
            this.useNewImplementation = useNewImplementation;
            return this;
        }

        public Client build() {
            // check if endpoint are empty. so can not initiate client
            if (this.endpoints.isEmpty()) {
                throw new IllegalArgumentException("At least one endpoint is required");
            }
            // check if username and password are empty. so can not initiate client?
            if (!this.configuration.containsKey("access_token") && (!this.configuration.containsKey("user") || !this.configuration.containsKey("password"))) {
                throw new IllegalArgumentException("Username and password are required");
            }

            this.configuration = setDefaults(this.configuration);

            return new Client(this.endpoints, this.configuration, this.useNewImplementation);
        }

        private Map<String, String> setDefaults(Map<String, String> userConfig) {

            // set default database name if not specified
            if (!userConfig.containsKey("database")) {
                userConfig.put("database", (String) ClickHouseDefaults.DATABASE.getDefaultValue());
            }

            if (!userConfig.containsKey(ClickHouseClientOption.MAX_EXECUTION_TIME.getKey())) {
                userConfig.put(ClickHouseClientOption.MAX_EXECUTION_TIME.getKey(),
                        String.valueOf(ClickHouseClientOption.MAX_EXECUTION_TIME.getDefaultValue()));
            }

            if (!userConfig.containsKey(ClickHouseClientOption.MAX_THREADS_PER_CLIENT.getKey())) {
                userConfig.put(ClickHouseClientOption.MAX_THREADS_PER_CLIENT.getKey(),
                        String.valueOf(ClickHouseClientOption.MAX_THREADS_PER_CLIENT.getDefaultValue()));
            }

            return userConfig;
        }
    }

    private ClickHouseNode getServerNode() {
        // TODO: implement load balancing using existing logic
        return this.serverNodes.get(0);
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
        try (ClickHouseClient client = ClientV1AdaptorHelper.createClient(configuration)) {
            return client.ping(getServerNode(), Math.toIntExact(timeout));
        }
    }

    /**
     * <p>Registers a POJO class and maps its fields to a table schema</p>
     *
     * @param clazz - class of a POJO
     * @param schema - correlating table schema
     */
    public void register(Class<?> clazz, TableSchema schema) {
        LOG.debug("Registering POJO: {}", clazz.getName());

        //Create a new POJOSerializer with static .serialize(object, columns) methods
        List<POJOSerializer> serializers = new ArrayList<>();
        Map<String, Method> getterMethods = new HashMap<>();

        for (Method method: clazz.getMethods()) {//Clean up the method names
            String methodName = method.getName();
            if (methodName.startsWith("get") || methodName.startsWith("has")) {
                methodName = methodName.substring(3).toLowerCase();
                getterMethods.put(methodName, method);
            } if (methodName.startsWith("is")) {
                methodName = methodName.substring(2).toLowerCase();
                getterMethods.put(methodName, method);
            }
        }
        this.getterMethods.put(clazz, getterMethods);//Store the getter methods for later use

        for (ClickHouseColumn column : schema.getColumns()) {
            String columnName = column.getColumnName().toLowerCase().replace("_", "").replace(".","");
            serializers.add((obj, stream) -> {
                if (!getterMethods.containsKey(columnName)) {
                    LOG.warn("No getter method found for column: {}", columnName);
                    return;
                }
                Method getterMethod = this.getterMethods.get(clazz).get(columnName);
                Object value = getterMethod.invoke(obj);
                boolean hasDefaults = this.hasDefaults.get(clazz);

                //Handle null values
                if (value == null) {
                    if (hasDefaults && !column.hasDefault()) {//Send this only if there is no default
                        BinaryStreamUtils.writeNonNull(stream);
                    }
                    BinaryStreamUtils.writeNull(stream);//We send this regardless of default or nullable
                    return;
                }

                //Handle default
                if (hasDefaults) {
                    BinaryStreamUtils.writeNonNull(stream);//Write 0
                }

                //Handle nullable
                if (column.isNullable()) {
                    BinaryStreamUtils.writeNonNull(stream);//Write 0
                }

                //Handle the different types
                SerializerUtils.serializeData(stream, value, column);
            });
        }
        this.serializers.put(clazz, serializers);
        this.hasDefaults.put(clazz, schema.hasDefaults());
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
     * @return {@code CompletableFuture<InsertResponse>} - a promise to insert response
     */
    public CompletableFuture<InsertResponse> insert(String tableName, List<?> data, InsertSettings settings) {

        String operationId = startOperation();
        settings.setOperationId(operationId);
        if (useNewImplementation) {
            globalClientStats.get(operationId).start(ClientMetrics.OP_DURATION);
        }
        globalClientStats.get(operationId).start(ClientMetrics.OP_SERIALIZATION);

        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Data cannot be empty");
        }

        //Add format to the settings
        if (settings == null) {
            settings = new InsertSettings();
        }

        boolean hasDefaults = this.hasDefaults.get(data.get(0).getClass());
        ClickHouseFormat format = hasDefaults? ClickHouseFormat.RowBinaryWithDefaults : ClickHouseFormat.RowBinary;

        //Lookup the Serializer for the POJO
        List<POJOSerializer> serializers = this.serializers.get(data.get(0).getClass());
        if (serializers == null || serializers.isEmpty()) {
            throw new SerializerNotFoundException(data.get(0).getClass());
        }

        if (useNewImplementation) {
            String retry = configuration.get(ClickHouseClientOption.RETRY.getKey());
            final int maxRetries = retry == null ? (int) ClickHouseClientOption.RETRY.getDefaultValue() : Integer.parseInt(retry);

            settings.setOption(ClickHouseClientOption.FORMAT.getKey(), format.name());
            final InsertSettings finalSettings = settings;
            CompletableFuture<InsertResponse> future = CompletableFuture.supplyAsync(() -> {
                // Selecting some node
                ClickHouseNode selectedNode = getNextAliveNode();

                for (int i = 0; i <= maxRetries; i++) {
                    // Execute request
                    try (ClassicHttpResponse httpResponse =
                            httpClientHelper.executeRequest(selectedNode, finalSettings.getAllSettings(),
                                    out -> {
                                        out.write("INSERT INTO ".getBytes());
                                        out.write(tableName.getBytes());
                                        out.write(" \n FORMAT ".getBytes());
                                        out.write(format.name().getBytes());
                                        out.write(" \n".getBytes());
                                        for (Object obj : data) {
                                            for (POJOSerializer serializer : serializers) {
                                                try {
                                                    serializer.serialize(obj, out);
                                                } catch (InvocationTargetException | IllegalAccessException | IOException e) {
                                                    throw new DataSerializationException(obj, serializer, e);
                                                }
                                            }
                                        }
                                    })) {


                        // Check response
                        if (httpResponse.getCode() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                            LOG.warn("Failed to get response. Server returned {}. Retrying.", httpResponse.getCode());
                            selectedNode = getNextAliveNode();
                            continue;
                        }

                        ClientStatisticsHolder clientStats = globalClientStats.remove(operationId);
                        OperationMetrics metrics = new OperationMetrics(clientStats);
                        String summary = HttpAPIClientHelper.getHeaderVal(httpResponse.getFirstHeader(ClickHouseHttpProto.HEADER_SRV_SUMMARY), "{}");
                        ProcessParser.parseSummary(summary, metrics);
                        String queryId =  HttpAPIClientHelper.getHeaderVal(httpResponse.getFirstHeader(ClickHouseHttpProto.QPARAM_QUERY_ID), finalSettings.getQueryId(), String::valueOf);
                        metrics.operationComplete();
                        metrics.setQueryId(queryId);
                        return new InsertResponse(metrics);
                    } catch (NoHttpResponseException e) {
                        LOG.warn("Failed to get response. Retrying.", e);
                        selectedNode = getNextAliveNode();
                        continue;
                    } catch (IOException e) {
                        LOG.info("Interrupted while waiting for response.");
                        throw new ClientException("Failed to get query response", e);
                    }
                }
                throw new ClientException("Failed to get table schema: too many retries");
            }, sharedOperationExecutor);
            return future;
        } else {
            //Create an output stream to write the data to
            ByteArrayOutputStream stream = new ByteArrayOutputStream();

            //Call the static .serialize method on the POJOSerializer for each object in the list
            for (Object obj : data) {
                for (POJOSerializer serializer : serializers) {
                    try {
                        serializer.serialize(obj, stream);
                    } catch (InvocationTargetException | IllegalAccessException | IOException e) {
                        throw new DataSerializationException(obj, serializer, e);
                    }
                }
            }

            globalClientStats.get(operationId).stop(ClientMetrics.OP_SERIALIZATION);
            LOG.debug("Total serialization time: {}", globalClientStats.get(operationId).getElapsedTime("serialization"));
            return insert(tableName, new ByteArrayInputStream(stream.toByteArray()), format, settings);
        }
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
        return insert(tableName, data, format, new InsertSettings());
    }

    /**
     * <p>Sends write request to database. Input data is read from the input stream.</p>
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
        String operationId = (String) settings.getOperationId();
        if (operationId == null) {
            operationId = startOperation();
            settings.setOperationId(operationId);
        }
        ClientStatisticsHolder clientStats = globalClientStats.remove(operationId);
        clientStats.start(ClientMetrics.OP_DURATION);

        if (useNewImplementation) {

            String retry = configuration.get(ClickHouseClientOption.RETRY.getKey());
            final int maxRetries = retry == null ? (int) ClickHouseClientOption.RETRY.getDefaultValue() : Integer.parseInt(retry);
            final int writeBufferSize = settings.getInputStreamCopyBufferSize() <= 0 ?
                    Integer.parseInt(configuration.getOrDefault(ClickHouseClientOption.WRITE_BUFFER_SIZE.getKey(), "8192")) :
                    settings.getInputStreamCopyBufferSize();

            if (writeBufferSize <= 0) {
                throw new IllegalArgumentException("Buffer size must be greater than 0");
            }

            settings.setOption(ClickHouseClientOption.FORMAT.getKey(), format.name());
            final InsertSettings finalSettings = settings;
            CompletableFuture<InsertResponse> future = CompletableFuture.supplyAsync(() -> {
                // Selecting some node
                ClickHouseNode selectedNode = getNextAliveNode();

                for (int i = 0; i <= maxRetries; i++) {
                    // Execute request
                    try (ClassicHttpResponse httpResponse =
                                 httpClientHelper.executeRequest(selectedNode, finalSettings.getAllSettings(),
                                         out -> {
                                             out.write("INSERT INTO ".getBytes());
                                             out.write(tableName.getBytes());
                                             out.write(" \n FORMAT ".getBytes());
                                             out.write(format.name().getBytes());
                                             out.write(" \n".getBytes());

                                             byte[] buffer = new byte[writeBufferSize];
                                             int bytesRead;
                                             while ((bytesRead = data.read(buffer)) != -1) {
                                                 out.write(buffer, 0, bytesRead);
                                             }
                                             out.flush();
                                         })) {


                        // Check response
                        if (httpResponse.getCode() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                            LOG.warn("Failed to get response. Server returned {}. Retrying.", httpResponse.getCode());
                            selectedNode = getNextAliveNode();
                            continue;
                        }

                        OperationMetrics metrics = new OperationMetrics(clientStats);
                        String summary = HttpAPIClientHelper.getHeaderVal(httpResponse.getFirstHeader(ClickHouseHttpProto.HEADER_SRV_SUMMARY), "{}");
                        ProcessParser.parseSummary(summary, metrics);
                        String queryId =  HttpAPIClientHelper.getHeaderVal(httpResponse.getFirstHeader(ClickHouseHttpProto.QPARAM_QUERY_ID), finalSettings.getQueryId(), String::valueOf);
                        metrics.operationComplete();
                        metrics.setQueryId(queryId);
                        return new InsertResponse(metrics);
                    } catch (NoHttpResponseException e) {
                        LOG.warn("Failed to get response. Retrying.", e);
                        selectedNode = getNextAliveNode();
                        continue;
                    } catch (IOException e) {
                        LOG.info("Interrupted while waiting for response.");
                        throw new ClientException("Failed to get query response", e);
                    }
                }
                throw new ClientException("Failed to get table schema: too many retries");
            }, sharedOperationExecutor);
            return future;
        } else {
            CompletableFuture<InsertResponse> responseFuture = new CompletableFuture<>();

            try (ClickHouseClient client = ClientV1AdaptorHelper.createClient(configuration)) {
                ClickHouseRequest.Mutation request = ClientV1AdaptorHelper
                        .createMutationRequest(client.write(getServerNode()), tableName, settings, configuration).format(format);

                CompletableFuture<ClickHouseResponse> future = null;
                try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(request.getConfig())) {
                    future = request.data(stream.getInputStream()).execute();

                    //Copy the data from the input stream to the output stream
                    byte[] buffer = new byte[settings.getInputStreamCopyBufferSize()];
                    int bytesRead;
                    while ((bytesRead = data.read(buffer)) != -1) {
                        stream.write(buffer, 0, bytesRead);
                    }
                } catch (IOException e) {
                    responseFuture.completeExceptionally(new ClientException("Failed to write data to the output stream", e));
                }

                if (!responseFuture.isCompletedExceptionally()) {
                    try {
                        int operationTimeout = getOperationTimeout();
                        ClickHouseResponse clickHouseResponse;
                        if (operationTimeout > 0) {
                            clickHouseResponse = future.get(operationTimeout, TimeUnit.MILLISECONDS);
                        } else {
                            clickHouseResponse = future.get();
                        }
                        InsertResponse response = new InsertResponse(client, clickHouseResponse, clientStats);
                        responseFuture.complete(response);
                    } catch (ExecutionException e) {
                        responseFuture.completeExceptionally(new ClientException("Failed to get insert response", e.getCause()));
                    } catch (InterruptedException | TimeoutException e) {
                        responseFuture.completeExceptionally(new ClientException("Operation has likely timed out.", e));
                    }
                }
                LOG.debug("Total insert (InputStream) time: {}", clientStats.getElapsedTime("insert"));
            }

            return responseFuture;
        }
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
     * <li>Server response format can be specified thru {@code settings} or in SQL query.</li>
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
        if (settings.getFormat() == null) {
            settings.setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        }
        ClientStatisticsHolder clientStats = new ClientStatisticsHolder();
        clientStats.start(ClientMetrics.OP_DURATION);

        if (useNewImplementation) {
            String retry = configuration.get(ClickHouseClientOption.RETRY.getKey());
            final int maxRetries = retry == null ? (int) ClickHouseClientOption.RETRY.getDefaultValue() : Integer.parseInt(retry);

            if (queryParams != null) {
                settings.setOption("statement_params", queryParams);
            }
            final QuerySettings finalSettings = settings;
            CompletableFuture<QueryResponse> future = CompletableFuture.supplyAsync(() -> {
                // Selecting some node
                ClickHouseNode selectedNode = getNextAliveNode();
                for (int i = 0; i <= maxRetries; i++) {
                    try {
                        ClassicHttpResponse httpResponse =
                                httpClientHelper.executeRequest(selectedNode, finalSettings.getAllSettings(), output -> {
                                    output.write(sqlQuery.getBytes(StandardCharsets.UTF_8));
                                    output.flush();
                                });

                        // Check response
                        if (httpResponse.getCode() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                            LOG.warn("Failed to get response. Server returned {}. Retrying.", httpResponse.getCode());
                            selectedNode = getNextAliveNode();
                            continue;
                        }

                        OperationMetrics metrics = new OperationMetrics(clientStats);
                        String summary = HttpAPIClientHelper.getHeaderVal(httpResponse
                                .getFirstHeader(ClickHouseHttpProto.HEADER_SRV_SUMMARY), "{}");
                        ProcessParser.parseSummary(summary, metrics);
                        String queryId = HttpAPIClientHelper.getHeaderVal(httpResponse
                                .getFirstHeader(ClickHouseHttpProto.HEADER_QUERY_ID), finalSettings.getQueryId());
                        metrics.setQueryId(queryId);
                        metrics.operationComplete();

                        return new QueryResponse(httpResponse, finalSettings, metrics);
                    } catch (Exception e) {
                        throw new ClientException("Failed to execute query", e);
                    }
                }
                throw new ClientException("Failed to get table schema: too many retries");
            }, sharedOperationExecutor);
            return future;
        } else {
            ClickHouseClient client = ClientV1AdaptorHelper.createClient(configuration);
            ClickHouseRequest<?> request = client.read(getServerNode());
            request.options(SettingsConverter.toRequestOptions(settings.getAllSettings()));
            request.settings(SettingsConverter.toRequestSettings(settings.getAllSettings(), queryParams));
            request.option(ClickHouseClientOption.ASYNC, false); // we have own async handling
            request.query(sqlQuery, settings.getQueryId());
            final ClickHouseFormat format = settings.getFormat();
            request.format(format);

            final QuerySettings finalSettings = settings;
            CompletableFuture<QueryResponse> future = CompletableFuture.supplyAsync(() -> {
                LOG.trace("Executing request: {}", request);
                try {

                    int operationTimeout = getOperationTimeout();
                    ClickHouseResponse clickHouseResponse;
                    if (operationTimeout > 0) {
                        clickHouseResponse = request.execute().get(operationTimeout, TimeUnit.MILLISECONDS);
                    } else {
                        clickHouseResponse = request.execute().get();
                    }

                    return new QueryResponse(client, clickHouseResponse, finalSettings, format, clientStats);
                } catch (ClientException e) {
                    throw e;
                } catch (Exception e) {
                    throw new ClientException("Failed to get query response", e);
                }
            }, sharedOperationExecutor);
            return future;
        }
    }

    /**
     * <p>Queries data in one of descriptive format and creates a reader out of the response stream.</p>
     * <p>Format is selected internally so is ignored when passed in settings. If query contains format
     * statement then it may cause incompatibility error.</p>
     *
     * @param sqlQuery
     * @return
     */
    public CompletableFuture<Records> queryRecords(String sqlQuery) {
        return queryRecords(sqlQuery, null);
    }

    /**
     * <p>Queries data in one of descriptive format and creates a reader out of the response stream.</p>
     * <p>Format is selected internally so is ignored when passed in settings. If query contains format
     * statement then it may cause incompatibility error.</p>
     *
     * @param sqlQuery
     * @param settings
     * @return
     */
    public CompletableFuture<Records> queryRecords(String sqlQuery, QuerySettings settings) {
        if (settings == null) {
            settings = new QuerySettings();
        }
        settings.setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        settings.waitEndOfQuery(true); // we rely on the summery

        final QuerySettings finalSettings = settings;
        return query(sqlQuery, settings).thenApplyAsync(response -> {
            try {
                return new Records(response, finalSettings);
            } catch (Exception e) {
                throw new ClientException("Failed to get query response", e);
            }
        }, sharedOperationExecutor);
    }

    /**
     * <p>Queries data in descriptive format and reads result to a collection.</p>
     * <p>Use this method for queries that would return only a few records only because client
     * will read whole dataset and convert it into a list of GenericRecord</p>
     * @param sqlQuery - SQL query
     * @return - complete list of records
     */
    public List<GenericRecord> queryAll(String sqlQuery) {
        try {
            int operationTimeout = getOperationTimeout();
            QuerySettings settings = new QuerySettings().waitEndOfQuery(true);
            try (QueryResponse response = operationTimeout == 0 ? query(sqlQuery, settings).get() :
                    query(sqlQuery, settings).get(operationTimeout, TimeUnit.MILLISECONDS)) {
                List<GenericRecord> records = new ArrayList<>();
                if (response.getResultRows() > 0) {
                    ClickHouseBinaryFormatReader reader = new RowBinaryWithNamesAndTypesFormatReader(response.getInputStream());
                    while (reader.hasNext()) {
                        records.add(new MapBackedRecord(reader.next(), reader.getSchema()));
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
        return getTableSchema(table, configuration.get("database"));
    }

    /**
     * <p>Fetches schema of a table and returns complete information about each column.
     * Information includes column name, type, default value, etc.</p>
     *
     * <p>See {@link #register(Class, TableSchema)}</p>
     *
     * @param table - table name
     * @param database - database name
     * @return {@code TableSchema} - Schema of the table
     */
    public TableSchema getTableSchema(String table, String database) {
        final String sql = "DESCRIBE TABLE " + table + " FORMAT " + ClickHouseFormat.TSKV.name();

        int operationTimeout = getOperationTimeout();

        try (QueryResponse response = operationTimeout == 0 ? query(sql).get() :
                query(sql).get(getOperationTimeout(), TimeUnit.SECONDS)) {
            return new TableSchemaParser().readTSKV(response.getInputStream(), table, database);
        } catch (TimeoutException e) {
            throw new ClientException("Operation has likely timed out after " + getOperationTimeout() + " seconds.", e);
        } catch (ExecutionException e) {
            throw new ClientException("Failed to get table schema", e.getCause());
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
     * @param sql - SQL command
     * @return {@code CompletableFuture<CommandResponse>} - a promise to command response
     */
    public CompletableFuture<CommandResponse> execute(String sql) {
        return query(sql)
                .thenApplyAsync(response -> {
                    try {
                        return new CommandResponse(response);
                    } catch (Exception e) {
                        throw new ClientException("Failed to get command response", e);
                    }
                });
    }

    private String startOperation() {
        String operationId = UUID.randomUUID().toString();
        globalClientStats.put(operationId, new ClientStatisticsHolder());
        return operationId;
    }

    public String toString() {
        return "Client{" +
                "endpoints=" + endpoints +
                '}';
    }

    /**
     * Returns unmodifiable map of configuration options.
     * @return - configuration options
     */
    public Map<String, String> getConfiguration() {
        return Collections.unmodifiableMap(configuration);
    }

    /** Returns operation timeout in seconds */
    protected int getOperationTimeout() {
        return Integer.parseInt(configuration.get(ClickHouseClientOption.MAX_EXECUTION_TIME.getKey()));
    }

    /**
     * Returns unmodifiable set of endpoints.
     * @return - set of endpoints
     */
    public Set<String> getEndpoints() {
        return Collections.unmodifiableSet(endpoints);
    }


    private ClickHouseNode getNextAliveNode() {
        return serverNodes.get(0);
    }
}
