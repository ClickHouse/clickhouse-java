package com.clickhouse.client.api;

import com.clickhouse.client.*;
import com.clickhouse.client.api.internal.SettingsConverter;
import com.clickhouse.client.api.internal.ValidationUtils;
import com.clickhouse.data.ClickHouseColumn;

import java.beans.Introspector;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.SocketException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.internal.TableSchemaParser;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.format.BinaryStreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.helpers.BasicMDCAdapter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.SECONDS;

public class Client {
    public static final int TIMEOUT = 30_000;
    private Set<String> endpoints;
    private Map<String, String> configuration;
    private List<ClickHouseNode> serverNodes = new ArrayList<>();
    private Map<Class<?>, List<POJOSerializer>> serializers = new HashMap<>();
    private static final  Logger LOG = LoggerFactory.getLogger(Client.class);

    private Client(Set<String> endpoints, Map<String,String> configuration) {
        this.endpoints = endpoints;
        this.configuration = configuration;
        this.endpoints.forEach(endpoint -> {
            this.serverNodes.add(ClickHouseNode.of(endpoint, this.configuration));
        });
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
                .setSocketSndbuf(804800);
        }

        public Builder addEndpoint(String endpoint) {
            // TODO: validate endpoint
            this.endpoints.add(endpoint);
            return this;
        }

        public Builder addEndpoint(Protocol protocol, String host, int port) {
            String endpoint = String.format("%s://%s:%d", protocol.toString().toLowerCase(), host, port);
            this.addEndpoint(endpoint);
            return this;
        }

        public Builder addConfiguration(String key, String value) {
            this.configuration.put(key, value);
            return this;
        }

        public Builder addUsername(String username) {
            this.configuration.put("user", username);
            return this;
        }

        public Builder addPassword(String password) {
            this.configuration.put("password", password);
            return this;
        }
        // SOCKET SETTINGS
        public Builder setConnectTimeout(long size) {
            this.configuration.put("connect_timeout", String.valueOf(size));
            return this;
        }
        public Builder setConnectTimeout(long amount, ChronoUnit unit) {
            this.setConnectTimeout(Duration.of(amount, unit).toMillis());
            return this;
        }

        public Builder setSocketTimeout(long size) {
            this.configuration.put("socket_timeout", String.valueOf(size));
            return this;
        }
        public Builder setSocketTimeout(long amount, ChronoUnit unit) {
            this.setSocketTimeout(Duration.of(amount, unit).toMillis());
            return this;
        }
        public Builder setSocketRcvbuf(long size) {
            this.configuration.put("socket_rcvbuf", String.valueOf(size));
            return this;
        }
        public Builder setSocketSndbuf(long size) {
            this.configuration.put("socket_sndbuf", String.valueOf(size));
            return this;
        }
        public Builder setSocketReuseaddr(boolean value) {
            this.configuration.put("socket_reuseaddr", String.valueOf(value));
            return this;
        }
        public Builder setSocketKeepalive(boolean value) {
            this.configuration.put("socket_keepalive", String.valueOf(value));
            return this;
        }
        public Builder setSocketTcpNodelay(boolean value) {
            this.configuration.put("socket_tcp_nodelay", String.valueOf(value));
            return this;
        }
        public Builder setSocketLinger(int secondsToWait) {
            this.configuration.put("socket_linger", String.valueOf(secondsToWait));
            return this;
        }
        public Client build() {
            // check if endpoint are empty. so can not initiate client
            if (this.endpoints.isEmpty()) {
                throw new IllegalArgumentException("At least one endpoint is required");
            }
            // check if username and password are empty. so can not initiate client?
            return new Client(this.endpoints, this.configuration);
        }
    }

    private ClickHouseNode getServerNode() {
        // TODO: implement load balancing using existing logic
        return this.serverNodes.get(0);
    }

    /**
     * Ping the server to check if it is alive
     * @return true if the server is alive, false otherwise
     */
    public boolean ping() {
        return ping(Client.TIMEOUT);
    }

    /**
     * Ping the server to check if it is alive
     * @param timeout timeout in milliseconds
     * @return true if the server is alive, false otherwise
     */
    public boolean ping(int timeout) {
        ClickHouseClient clientPing = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
        return clientPing.ping(getServerNode(), timeout);
    }

    /**
     * Register the POJO
     */
    public void register(Class<?> clazz, TableSchema schema) {
        //Create a new POJOSerializer with static .serialize(object, columns) methods
        Map<String, Method> getters = new HashMap<>();
        List<POJOSerializer> serializers = new ArrayList<>();

        //Retrieve all methods
        for (Method method: clazz.getMethods()) {
            String methodName = method.getName();
            if (method.getName().startsWith("get") &&
                    method.getParameterCount() == 0 &&
                    method.getReturnType() != void.class) {//Make sure they're getter methods
                String fieldName = methodName.substring(3);//Get the field name
                ClickHouseColumn column = schema.getColumnByName(fieldName);
                if(column != null) {//Check if the field is in the schema
                    getters.put(fieldName, method);
                    serializers.add((obj, stream, columns) -> {
                        if (columns == null || columns.contains(fieldName)) {
                            Method getter = getters.get(fieldName);
                            Object value = getter.invoke(obj);

                            //Serialize the value to the stream based on the type
                            switch (column.getDataType()) {
                                case Int8:
                                    BinaryStreamUtils.writeInt8(stream, (Integer) value);
                                    break;
                                case Int16:
                                    BinaryStreamUtils.writeInt16(stream, (Integer) value);
                                    break;
                                case Int32:
                                    BinaryStreamUtils.writeInt32(stream, (Integer) value);
                                    break;
                                case Int64:
                                    BinaryStreamUtils.writeInt64(stream, (Long) value);
                                    break;
                                case UInt8:
                                    BinaryStreamUtils.writeUnsignedInt8(stream, (Integer) value);
                                    break;
                                case UInt16:
                                    BinaryStreamUtils.writeUnsignedInt16(stream, (Integer) value);
                                    break;
                                case UInt32:
                                    BinaryStreamUtils.writeUnsignedInt32(stream, (Long) value);
                                    break;
                                case UInt64:
                                    BinaryStreamUtils.writeUnsignedInt64(stream, (Long) value);
                                    break;
                                case Float32:
                                    BinaryStreamUtils.writeFloat32(stream, (Float) value);
                                    break;
                                case Float64:
                                    BinaryStreamUtils.writeFloat64(stream, (Double) value);
                                    break;
                                case String:
                                    BinaryStreamUtils.writeString(stream, value.toString());
                                    break;
                                case FixedString:
                                    BinaryStreamUtils.writeFixedString(stream, value.toString(), column.getPrecision());
                                    break;
                                case Date:
                                    BinaryStreamUtils.writeDate(stream, (LocalDate) value);
                                    break;
                                case DateTime:
                                    BinaryStreamUtils.writeDateTime(stream, (LocalDateTime) value, column.getTimeZone());
                                    break;
                                case Enum8:
                                    BinaryStreamUtils.writeEnum8(stream, (Byte) value);
                                case Enum16:
                                    assert value instanceof Integer;
                                    BinaryStreamUtils.writeEnum16(stream, (Integer) value);
                                    break;
                                case Decimal32:
                                    BinaryStreamUtils.writeDecimal32(stream, (BigDecimal) value, column.getScale());
                                    break;
                                case Decimal64:
                                    BinaryStreamUtils.writeDecimal64(stream, (BigDecimal) value, column.getScale());
                                    break;
                                case Decimal128:
                                    BinaryStreamUtils.writeDecimal128(stream, (BigDecimal) value, column.getScale());
                                    break;

                            }
                        }
                    });
                }
            }
        }
    }

    /**
     * Insert data into ClickHouse using a POJO
     */
    public Future<InsertResponse> insert(String tableName,
                                     List<Object> data,
                                     InsertSettings settings,
                                     List<ClickHouseColumn> columns) throws ClickHouseException, SocketException {
        //Lookup the Serializer for the POJO
        //Call the static .serialize method on the POJOSerializer for each object in the list
        return null;//This is just a placeholder
    }

    /**
     * Insert data into ClickHouse using a binary stream
     */
    public Future<InsertResponse> insert(String tableName,
                                     InputStream data,
                                     InsertSettings settings) throws ClickHouseException, SocketException {
        return null;//This is just a placeholder
    }


    /**
     * Sends data query to the server and returns a reference to a result descriptor.
     * Control is returned when server accepted the query and started processing it.
     * <br/>
     * The caller should use {@link ClickHouseParameterizedQuery} to render the `sqlQuery` with parameters.
     *
     *
     * @param sqlQuery - complete SQL query.
     * @param settings
     * @return
     */
    public Future<QueryResponse> query(String sqlQuery, Map<String, Object> qparams, QuerySettings settings) {
        ClickHouseClient client = createClient();
        ClickHouseRequest<?> request = client.read(getServerNode());
        request.options(SettingsConverter.toRequestOptions(settings.getAllSettings()));
        request.settings(SettingsConverter.toRequestSettings(settings.getAllSettings()));
        request.query(sqlQuery, settings.getQueryID());
        request.format(ClickHouseFormat.valueOf(settings.getFormat()));
        if (qparams != null && !qparams.isEmpty()) {
            request.params(qparams);
        }
        MDC.put("queryId", settings.getQueryID());
        LOG.debug("Executing request: {}", request);
        return CompletableFuture.completedFuture(new QueryResponse(client, request.execute()));
    }

    public TableSchema getTableSchema(String table, String database) {
        try (ClickHouseClient clientQuery = createClient()) {
            ClickHouseRequest request = clientQuery.read(getServerNode());
            // XML - because java has a built-in XML parser. Will consider CSV later.
            request.query("DESCRIBE TABLE " + table + " FORMAT " + ClickHouseFormat.TSKV.name());
            TableSchema tableSchema = new TableSchema();
            try {
                return new TableSchemaParser().createFromBinaryResponse(clientQuery.execute(request).get(), table, database);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get table schema", e);
            }
        }
    }


    private ClickHouseClient createClient() {
        ClickHouseConfig clientConfig = new ClickHouseConfig();
        return ClickHouseClient.builder().config(clientConfig)
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
    }

    private static final Set<String> COMPRESS_ALGORITHMS = ValidationUtils.whiteList("LZ4", "LZ4HC", "ZSTD", "ZSTDHC", "NONE");

    public static Set<String> getCompressAlgorithms() {
        return COMPRESS_ALGORITHMS;
    }

    private static final Set<String> OUTPUT_FORMATS = createFormatWhitelist("output");

    private static final Set<String> INPUT_FORMATS = createFormatWhitelist("input");

    public static Set<String> getOutputFormats() {
        return OUTPUT_FORMATS;
    }

    private static Set<String> createFormatWhitelist(String shouldSupport) {
        Set<String> formats = new HashSet<>();
        boolean supportOutput = "output".equals(shouldSupport);
        boolean supportInput = "input".equals(shouldSupport);
        boolean supportBoth = "both".equals(shouldSupport);
        for (ClickHouseFormat format : ClickHouseFormat.values()) {
            if ((supportOutput && format.supportsOutput()) || (supportInput && format.supportsInput()) || (supportBoth)) {
                formats.add(format.name());
            }
        }
        return Collections.unmodifiableSet(formats);
    }
}
