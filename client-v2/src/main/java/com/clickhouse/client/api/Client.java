package com.clickhouse.client.api;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseClientBuilder;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.MapBackedRecord;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.enums.ProxyType;
import com.clickhouse.client.api.insert.DataSerializationException;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.insert.POJOSerializer;
import com.clickhouse.client.api.insert.SerializerNotFoundException;
import com.clickhouse.client.api.internal.ClientStatisticsHolder;
import com.clickhouse.client.api.internal.ClientV1AdaptorHelper;
import com.clickhouse.client.api.internal.SerializerUtils;
import com.clickhouse.client.api.internal.SettingsConverter;
import com.clickhouse.client.api.internal.TableSchemaParser;
import com.clickhouse.client.api.internal.ValidationUtils;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.format.BinaryStreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
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
public class Client {
    private static final long TIMEOUT = TimeUnit.SECONDS.toMillis(30);

    private static final String DEFAULT_DB_NAME = "default";

    private Set<String> endpoints;
    private Map<String, String> configuration;
    private List<ClickHouseNode> serverNodes = new ArrayList<>();
    private Map<Class<?>, List<POJOSerializer>> serializers;//Order is important to preserve for RowBinary
    private Map<Class<?>, Map<String, Method>> getterMethods;
    private Map<Class<?>, Boolean> hasDefaults;
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private ExecutorService queryExecutor;

    private Map<String, ClientStatisticsHolder> globalClientStats = new ConcurrentHashMap<>();

    private Client(Set<String> endpoints, Map<String,String> configuration) {
        this.endpoints = endpoints;
        this.configuration = configuration;
        this.endpoints.forEach(endpoint -> {
            this.serverNodes.add(ClickHouseNode.of(endpoint, this.configuration));
        });
        this.serializers = new HashMap<>();
        this.getterMethods = new HashMap<>();
        this.hasDefaults = new HashMap<>();

        final int numThreads = Integer.parseInt(configuration.getOrDefault(
                ClickHouseClientOption.MAX_THREADS_PER_CLIENT.getKey(), "3"));
        this.queryExecutor = Executors.newFixedThreadPool(numThreads, r -> {
            Thread t = new Thread(r);
            t.setName("ClickHouse-Query-Executor");
            t.setUncaughtExceptionHandler((t1, e) -> {
                LOG.error("Uncaught exception in thread {}", t1.getName(), e);
            });
            return t;
        });
        LOG.debug("Query executor created with {} threads", numThreads);
    }

    /**
     * Returns default database name that will be used by operations if not specified.
     *
     * @return String - actual default database name.
     */
    public String getDefaultDatabase() {
        return this.configuration.get("database");
    }

    public static class Builder {
        private Set<String> endpoints;
        private Map<String, String> configuration;

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

        public Client build() {
            // check if endpoint are empty. so can not initiate client
            if (this.endpoints.isEmpty()) {
                throw new IllegalArgumentException("At least one endpoint is required");
            }
            // check if username and password are empty. so can not initiate client?
            if (!this.configuration.containsKey("access_token") && (!this.configuration.containsKey("user") || !this.configuration.containsKey("password"))) {
                throw new IllegalArgumentException("Username and password are required");
            }
            // set default database name if not specified
            if (!this.configuration.containsKey("database")) {
                this.configuration.put("database", DEFAULT_DB_NAME);
            }
            return new Client(this.endpoints, this.configuration);
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
        return ping(Client.TIMEOUT);
    }

    /**
     * Pings the server to check if it is alive. Maximum timeout is 10 minutes.
     *
     * @param timeout timeout in milliseconds
     * @return true if the server is alive, false otherwise
     */
    public boolean ping(long timeout) {
        ValidationUtils.checkRange(timeout, TimeUnit.SECONDS.toMillis(1), TimeUnit.MINUTES.toMillis(10),
                "timeout");

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
            String columnName = column.getColumnName().toLowerCase().replace("_", "");
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

        //Create an output stream to write the data to
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        //Lookup the Serializer for the POJO
        List<POJOSerializer> serializers = this.serializers.get(data.get(0).getClass());
        if (serializers == null || serializers.isEmpty()) {
            throw new SerializerNotFoundException(data.get(0).getClass());
        }

        //Call the static .serialize method on the POJOSerializer for each object in the list
        for (Object obj : data) {
            for (POJOSerializer serializer : serializers) {
                try {
                    serializer.serialize(obj, stream);
                } catch (InvocationTargetException | IllegalAccessException | IOException  e) {
                    throw new DataSerializationException(obj, serializer, e);
                }
            }
        }

        globalClientStats.get(operationId).stop(ClientMetrics.OP_SERIALIZATION);
        LOG.debug("Total serialization time: {}", globalClientStats.get(operationId).getElapsedTime("serialization"));
        return insert(tableName, new ByteArrayInputStream(stream.toByteArray()), format, settings);
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

        CompletableFuture<InsertResponse> responseFuture = new CompletableFuture<>();

        try (ClickHouseClient client = ClientV1AdaptorHelper.createClient(configuration)) {
            ClickHouseRequest.Mutation request = ClientV1AdaptorHelper
                    .createMutationRequest(client.write(getServerNode()), tableName, settings, configuration).format(format);
            CompletableFuture<ClickHouseResponse> future = null;
            try(ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(request.getConfig())) {
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
                    InsertResponse response = new InsertResponse(client, future.get(), clientStats);
                    responseFuture.complete(response);
                } catch (InterruptedException | ExecutionException e) {
                    responseFuture.completeExceptionally(new ClientException("Operation has likely timed out.", e));
                }
            }
            LOG.debug("Total insert (InputStream) time: {}", clientStats.getElapsedTime("insert"));
        }

        return responseFuture;
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
        ClickHouseClient client = ClientV1AdaptorHelper.createClient(configuration);
        ClickHouseRequest<?> request = client.read(getServerNode());
        request.options(SettingsConverter.toRequestOptions(settings.getAllSettings()));
        request.settings(SettingsConverter.toRequestSettings(settings.getAllSettings(), queryParams));
        request.query(sqlQuery, settings.getQueryId());
        final ClickHouseFormat format = settings.getFormat();
        request.format(format);

        final QuerySettings finalSettings = settings;
        CompletableFuture<QueryResponse> future = CompletableFuture.supplyAsync(() -> {
            LOG.trace("Executing request: {}", request);
            try {
                QueryResponse queryResponse = new QueryResponse(client, request.execute(), finalSettings, format, clientStats);
                queryResponse.ensureDone();
                return queryResponse;
            } catch (ClientException e) {
                throw e;
            } catch (Exception e) {
                throw new ClientException("Failed to get query response", e);
            }
        }, queryExecutor);
        return future;
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
        ClientStatisticsHolder clientStats = new ClientStatisticsHolder();
        clientStats.start("query");
        ClickHouseClient client = ClientV1AdaptorHelper.createClient(configuration);
        ClickHouseRequest<?> request = client.read(getServerNode());
        request.options(SettingsConverter.toRequestOptions(settings.getAllSettings()));
        request.settings(SettingsConverter.toRequestSettings(settings.getAllSettings(), null));
        request.query(sqlQuery, settings.getQueryId());
        final ClickHouseFormat format = settings.getFormat();
        request.format(format);

        final QuerySettings finalSettings = settings;
        CompletableFuture<Records> future = CompletableFuture.supplyAsync(() -> {
            LOG.trace("Executing request: {}", request);
            try {
                QueryResponse queryResponse = new QueryResponse(client, request.execute(), finalSettings, format, clientStats);
                queryResponse.ensureDone();
                return new Records(queryResponse, finalSettings);
            } catch (ClientException e) {
                throw e;
            } catch (Exception e) {
                throw new ClientException("Failed to get query response", e);
            }
        }, queryExecutor);

        return future;
    }

    /**
     * <p>Queries data in descriptive format and reads result to a collection.</p>
     * <p>Use this method for queries that would return only a few records only.</p>
     * @param sqlQuery
     * @return
     */
    public List<GenericRecord> queryAll(String sqlQuery) {
        try {
            try (QueryResponse response = query(sqlQuery).get(TIMEOUT, TimeUnit.MILLISECONDS)) {
                List<GenericRecord> records = new ArrayList<>();
                if (response.getResultRows() > 0) {
                    ClickHouseBinaryFormatReader reader = new RowBinaryWithNamesAndTypesFormatReader(response.getInputStream());
                    while (reader.hasNext()) {
                        records.add(new MapBackedRecord(reader.next(), reader.getSchema()));
                    }
                }
                return records;
            }
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
        try (ClickHouseClient clientQuery = ClientV1AdaptorHelper.createClient(configuration)) {
            ClickHouseRequest<?> request = clientQuery.read(getServerNode());
            // XML - because java has a built-in XML parser. Will consider CSV later.
            request.query("DESCRIBE TABLE " + table + " FORMAT " + ClickHouseFormat.TSKV.name());
            try {
                return new TableSchemaParser().createFromBinaryResponse(clientQuery.execute(request).get(), table, database);
            } catch (Exception e) {
                throw new ClientException("Failed to get table schema", e);
            }
        }
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

    public Map<String, String> getConfiguration() {
        return Collections.unmodifiableMap(configuration);
    }

    public Set<String> getEndpoints() {
        return Collections.unmodifiableSet(endpoints);
    }
}
