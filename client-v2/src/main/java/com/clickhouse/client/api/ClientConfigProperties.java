package com.clickhouse.client.api;

import com.clickhouse.client.api.data_formats.ClickHouseFormatReader;
import com.clickhouse.client.api.data_formats.internal.AbstractBinaryFormatReader;
import com.clickhouse.client.api.enums.SSLMode;
import com.clickhouse.client.api.internal.CompressedBlockOutputStream;
import com.clickhouse.client.api.internal.WorkloadIdentitySupport;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Enumerates all client properties that are known at release.
 */
public enum ClientConfigProperties {

    @Description("Comma-separated list of roles that the server applies to each query.")
    SESSION_DB_ROLES("session_db_roles", List.class),

    @Description("Comment that the server writes to system.query_log for each query (server setting log_comment).")
    SETTING_LOG_COMMENT(serverSetting("log_comment"), String.class),

    @Description("Sends the user and the password with HTTP Basic authentication. Set to false to send them in the "
            + "X-ClickHouse-User and X-ClickHouse-Key headers.")
    HTTP_USE_BASIC_AUTH("http_use_basic_auth", Boolean.class, "true"),

    @Description("User name for authentication.")
    USER("user", String.class, "default"),

    @Description("Password for authentication.")
    PASSWORD("password", String.class),

    /**
     * Maximum number of active connection in internal connection pool.
     */
    @Description("Maximum number of open connections to one server in the connection pool.")
    HTTP_MAX_OPEN_CONNECTIONS("max_open_connections", Integer.class, "10"),

    /**
     * HTTP keep-alive timeout override.
     */
    @Description("HTTP keep-alive timeout in milliseconds. It overrides the keep-alive timeout of the server. When it "
            + "is not set, the client uses the server value.")
    HTTP_KEEP_ALIVE_TIMEOUT("http_keep_alive_timeout", Long.class),

    @Description("Uses the server time zone to decode Date and DateTime values.")
    USE_SERVER_TIMEZONE("use_server_time_zone", Boolean.class, "true"),

    @Description("Time zone ID that the client uses to decode Date and DateTime values instead of the server time "
            + "zone.")
    USE_TIMEZONE("use_time_zone", TimeZone.class),

    @Description("Server version that Client#getServerVersion() of client-v2 returns before the client reads the "
            + "version from the server. The JDBC driver does not use it.")
    SERVER_VERSION("server_version", String.class),

    @Description("Time zone ID of the server. The client uses it to decode Date and DateTime values when "
            + "use_server_time_zone is true and the server response does not give a time zone.")
    SERVER_TIMEZONE("server_time_zone", TimeZone.class, "UTC"),

    @Description("Runs each operation in a separate thread.")
    ASYNC_OPERATIONS("async", Boolean.class, "false"),

    @Description("Time in milliseconds that a connection can stay in the connection pool. After this time, the pool "
            + "does not reuse the connection. -1 means no limit.")
    CONNECTION_TTL("connection_ttl", Long.class, "-1"),

    @Description("Timeout in milliseconds to open a connection to the server.")
    CONNECTION_TIMEOUT("connection_timeout", Long.class),

    @Description("Order in which the connection pool reuses idle connections. Choices: FIFO, LIFO.")
    CONNECTION_REUSE_STRATEGY("connection_reuse_strategy", ConnectionReuseStrategy.class, String.valueOf(ConnectionReuseStrategy.FIFO)),

    @Description("Timeout in milliseconds to wait for data from the server. 0 means no timeout.")
    SOCKET_OPERATION_TIMEOUT("socket_timeout", Integer.class, "0"),

    /**
     * Socket receive buffer size in bytes (SO_RCVBUF). Not set by default: the operating system
     * sizes and auto-tunes the buffer. Setting it disables that auto-tuning and is not recommended
     * unless a measurement shows a benefit.
     */
    @Description("Socket receive buffer size in bytes (SO_RCVBUF). When it is not set, the operating system sets and "
            + "tunes the size.")
    SOCKET_RCVBUF_OPT("socket_rcvbuf", Integer.class),

    /**
     * Socket send buffer size in bytes (SO_SNDBUF). Not set by default: the operating system
     * sizes and auto-tunes the buffer. Setting it disables that auto-tuning and is not recommended
     * unless a measurement shows a benefit.
     */
    @Description("Socket send buffer size in bytes (SO_SNDBUF). When it is not set, the operating system sets and "
            + "tunes the size.")
    SOCKET_SNDBUF_OPT("socket_sndbuf", Integer.class),

    @Description("Reserved for the socket option SO_REUSEADDR. The client does not apply it.")
    SOCKET_REUSEADDR_OPT("socket_reuseaddr", Boolean.class),

    @Description("Reserved for the socket option SO_KEEPALIVE. The client does not apply it.")
    SOCKET_KEEPALIVE_OPT("socket_keepalive", Boolean.class),

    @Description("Sets the socket option TCP_NODELAY. This disables the Nagle algorithm.")
    SOCKET_TCP_NO_DELAY_OPT("socket_tcp_nodelay", Boolean.class),

    @Description("Socket linger time in seconds (SO_LINGER).")
    SOCKET_LINGER_OPT("socket_linger", Integer.class),

    @Description("Default database for operations.")
    DATABASE("database", String.class, "default"),

    @Description("Asks the server to compress the response data.")
    COMPRESS_SERVER_RESPONSE("compress", Boolean.class, "true"), // actually a server setting, but has client effect too

    @Description("Compresses the request data that the client sends to the server.")
    COMPRESS_CLIENT_REQUEST("decompress", Boolean.class, "false"), // actually a server setting, but has client effect too

    @Description("Uses HTTP compression. The Content-Encoding and Accept-Encoding headers control the compression of "
            + "requests and responses.")
    USE_HTTP_COMPRESSION("client.use_http_compression", Boolean.class, "false"),

    @Description("Size in bytes of the buffer for uncompressed data in block compression and decompression (LZ4 and "
            + "ZSTD).")
    COMPRESSION_LZ4_UNCOMPRESSED_BUF_SIZE("compression.lz4.uncompressed_buffer_size", Integer.class, String.valueOf(CompressedBlockOutputStream.UNCOMPRESSED_BUFF_SIZE)),

    @Description("Compression method for the request data that the client sends. Choices: LZ4, ZSTD.")
    COMPRESSION_METHOD("compression.method", CompressionMethod.class, CompressionMethod.ZSTD.name()),

    @Description("Uses the Java implementation of LZ4 instead of the native implementation.")
    DISABLE_NATIVE_COMPRESSION("disable_native_compression", Boolean.class, "false"),

    @Description("Type of the proxy server, in upper case. Choices: HTTP, SOCKS.")
    PROXY_TYPE("proxy_type", String.class), // "http"

    @Description("Host name or IP address of the proxy server.")
    PROXY_HOST("proxy_host", String.class),

    @Description("Port of the proxy server.")
    PROXY_PORT("proxy_port", Integer.class),

    @Description("User name for authentication with the HTTP proxy server.")
    PROXY_USER("proxy_user", String.class),

    @Description("Password for authentication with the HTTP proxy server.")
    PROXY_PASSWORD("proxy_password", String.class),

    @Description("Maximum time in milliseconds that the client waits for the result of a blocking operation, for "
            + "example ping or queryAll. 0 means no limit. The client does not send it to the server. For the server "
            + "limit, use clickhouse_setting_max_execution_time (seconds).")
    MAX_EXECUTION_TIME("max_execution_time", Integer.class,"0"),

    @Description("Path to the trust store file that the client uses to verify the server certificate.")
    SSL_TRUST_STORE("trust_store", String.class),

    @Description("Type of the trust store file, for example JKS or PKCS12.")
    SSL_KEYSTORE_TYPE("key_store_type", String.class),

    @Description("Reserved. The client does not use this property.")
    SSL_KEY_STORE("ssl_key_store", String.class),

    @Description("Password of the trust store file.")
    SSL_KEY_STORE_PASSWORD("key_store_password",  String.class),

    @Description("File path or PEM content of the private key of the client certificate.")
    SSL_KEY("ssl_key",  String.class),

    @Description("File path or PEM content of the CA certificate that the client uses to verify the server "
            + "certificate.")
    CA_CERTIFICATE("sslrootcert", String.class),

    @Description("File path or PEM content of the client certificate for mutual TLS and for SSL authentication.")
    SSL_CERTIFICATE("sslcert", String.class),

    @Description("How the client verifies the server on secure connections. Choices: STRICT, VERIFY_CA, TRUST, "
            + "DISABLED. The JDBC driver also accepts none for TRUST.")
    SSL_MODE("ssl_mode", SSLMode.class, SSLMode.STRICT.name()),

    @Description("A javax.net.ssl.SSLContext object for secure connections. Set it with Properties.put(). A string "
            + "value is not accepted.")
    SSL_CONTEXT("ssl_context", SSLContext.class),

    @Description("Maximum number of retries for the failure causes in client_retry_on_failures.")
    RETRY_ON_FAILURE("retry", Integer.class, "3"),

    @Description("Default data format of query results.")
    INPUT_OUTPUT_FORMAT("format", ClickHouseFormat.class, ClickHouseFormat.RowBinaryWithNamesAndTypes.name()),

    @Description("Reserved. The client does not use this property.")
    MAX_THREADS_PER_CLIENT("max_threads_per_client", Integer.class, "0"),

    @Description("Query ID that the client sends with the request.")
    QUERY_ID("query_id", String.class), // actually a server setting, but has client effect too

    @Description("Size in bytes of the buffer that copies data between the socket and the application.")
    CLIENT_NETWORK_BUFFER_SIZE("client_network_buffer_size", Integer.class, "300000"),

    @Description("Access token for authentication. The client sends it as is in the HTTP Authorization header.")
    ACCESS_TOKEN("access_token", String.class),

    @Description("Uses the client certificate (sslcert) for authentication with the server.")
    SSL_AUTH("ssl_authentication", Boolean.class, "false"),

    @Description("Enables the connection pool.")
    CONNECTION_POOL_ENABLED("connection_pool_enabled",  Boolean.class, "true"),

    @Description("Timeout in milliseconds to get a connection from the connection pool.")
    CONNECTION_REQUEST_TIMEOUT("connection_request_timeout", Long.class, "10000"),

    @Description("Comma-separated list of failure causes that the client retries. The values are the names of "
            + "ClientFaultCause constants. None disables the retries.")
    CLIENT_RETRY_ON_FAILURE("client_retry_on_failures", List.class,
            String.join(",", ClientFaultCause.NoHttpResponse.name(), ClientFaultCause.ConnectTimeout.name(),
            ClientFaultCause.ConnectionRequestTimeout.name(), ClientFaultCause.ServerRetryable.name())) {
        @Override
        public Object parseValue(String value) {
            List<String> strValues = (List<String>) super.parseValue(value);
            List<ClientFaultCause> failures = new ArrayList<ClientFaultCause>();
            if (strValues != null) {
                for (String strValue : strValues) {
                    failures.add(ClientFaultCause.valueOf(strValue));
                }
            }
            return failures;
        }
    },

    @Description("Name of the application. The client sends it at the start of the User-Agent header.")
    CLIENT_NAME("client_name", String.class, ""),

    /**
     * An old alias to {@link ClientConfigProperties#CLIENT_NAME}. Using the last one is preferred.
     */
    @Deprecated
    @Description("Deprecated. Use client_name.")
    PRODUCT_NAME("product_name", String.class),

    @Description("Bearer token for authentication. The client sends it in the HTTP Authorization header after the "
            + "Bearer prefix.")
    BEARERTOKEN_AUTH ("bearer_token", String.class),
    /**
     * Indicates that data provided for write operation is compressed by application.
     */
    @Description("Tells the client that the application compresses the data that it inserts.")
    APP_COMPRESSED_DATA("app_compressed_data", Boolean.class, "false"),

    /**
     * Name of the group under which client metrics appear
     */
    @Description("Name of the group of the connection pool metrics.")
    METRICS_GROUP_NAME("metrics_name", String.class, "ch-http-pool"),

    @Description("Keeps the HTTP cookies from the server and sends them back to the server.")
    HTTP_SAVE_COOKIES("client.http.cookies_enabled",  Boolean.class, "false"),

    @Description("Lets the binary format reader reuse its buffers for numbers. This decreases memory allocation.")
    BINARY_READER_USE_PREALLOCATED_BUFFERS("client_allow_binary_reader_to_reuse_buffers", Boolean.class, "false"),

    /**
     * Defines mapping between ClickHouse data type and target Java type
     * Used by binary readers to convert values into desired Java type.
     */
    @Description("Mapping of ClickHouse data types to the Java classes that the binary readers of client-v2 use for "
            + "values. The JDBC driver sets its own mapping. Use jdbc_type_mappings.")
    TYPE_HINT_MAPPING("type_hint_mapping", Map.class),

    /**
     * When enabled, {@code String} and {@code FixedString} columns are read into an intermediate {@code byte[]}
     * instead of decoding them into a {@link String}. Improves working with large strings and lets
     * {@link ClickHouseFormatReader#getByteArray} be used more effectively. Can be configured per operation.
     */
    @Description("Reads String and FixedString columns into byte arrays instead of String objects.")
    BINARY_STRING_SUPPORT("binary_string_support", Boolean.class, "false"),

    /**
     * SNI SSL parameter that will be set for each outbound SSL socket.
     */
    @Description("Server name for SNI (Server Name Indication) on each SSL socket. When it is set, the client also "
            + "sends this name in the Host header and does not verify the host name in the server certificate.")
    SSL_SOCKET_SNI("ssl_socket_sni", String.class,""),

    /**
     * If parameters should be sent in request body.
     * Note: work only with Server side compression. If compression is enabled on client level it will be disabled
     * for query requests with parameters.
     */
    @Description("Sends a query that has parameters as HTTP form data in the request body. This works only with "
            + "server compression.")
    HTTP_SEND_PARAMS_IN_BODY("client.http.use_form_request_for_query", Boolean.class, "false"),

    /**
     * When enabled for JSONEachRow queries, asks ClickHouse to emit large integer,
     * floating-point, and decimal values as JSON numbers instead of quoted strings.
     */
    @Description("For JSONEachRow queries, asks the server to write large integer, floating-point and decimal values "
            + "as JSON numbers instead of quoted strings.")
    JSON_DISABLE_NUMBER_QUOTING("json_disable_number_quoting", Boolean.class, "false"),

    /**
     *  Prefix for custom settings. Should be aligned with server configuration.
     *  See <a href="https://clickhouse.com/docs/operations/settings/query-level#custom_settings">ClickHouse Docs</a>
     */
    @Description("Prefix of custom server settings. It must agree with the server configuration.")
    CUSTOM_SETTINGS_PREFIX("custom_settings_prefix", String.class, "custom_"),

    /**
     * Comma-separated list of TLS cipher suites the client is allowed to negotiate on secure connections.
     * When set, only these cipher suites are enabled on the SSL socket (subject to what the JVM and server
     * support); when unset, the transport defaults are used (Apache HttpClient enables the JVM's default
     * suites minus those it considers weak).
     * <p>
     * The value is parsed into a sanitized list here, in the config-parsing layer (not in transport code):
     * blank tokens produced by a leading, trailing or doubled comma are dropped and each surviving name is
     * trimmed, so consumers receive a ready-to-use list. A null, empty or whitespace-only entry would
     * otherwise be rejected by the SSL socket and break the handshake even alongside valid suites.
     * <p>
     * Appended at the end of the enum on purpose: adding a constant in the middle would shift the ordinal
     * of every following constant (see {@code docs/changes_checklist.md}).
     */
    @Description("Comma-separated list of TLS cipher suites that the client can use. When it is not set, the client "
            + "uses the default cipher suites of the transport.")
    SSL_CIPHER_SUITES("ssl_cipher_suites", List.class) {
        @Override
        @SuppressWarnings("unchecked")
        public Object parseValue(String value) {
            List<String> suites = (List<String>) super.parseValue(value);
            if (suites == null) {
                return null;
            }
            return suites.stream()
                    .filter(s -> s != null && !s.trim().isEmpty())
                    .map(String::trim)
                    .collect(Collectors.toList());
        }
    },

    /**
     * Key/value pairs, written as {@code key1=value1,key2=value2}, that the client adds to the comment group of the
     * {@code User-Agent} header. A {@code ,} or {@code \} in a value is escaped with {@code \}. This is a client-level
     * setting: it is not read from operation settings. See {@link Client.Builder#setClientName(String, Map)} for the
     * format and the rules for keys and values.
     */
    @Description("Comma-separated key=value pairs that the client adds to the comment part of the User-Agent header.")
    CLIENT_ID_METADATA("client_id_metadata", Map.class) {
        @Override
        @SuppressWarnings("unchecked")
        public Object parseValue(String value) {
            Map<String, String> metadata = (Map<String, String>) super.parseValue(value);
            if (metadata != null) {
                metadata.forEach(WorkloadIdentitySupport::checkClientMetadataEntry);
            }
            return metadata;
        }
    },
    ;

    private static final Logger LOG = LoggerFactory.getLogger(ClientConfigProperties.class);

    private final String key;

    private final Class<?> valueType;

    private final String defaultValue;

    private final Object defaultObjValue;

    ClientConfigProperties(String key, Class<?> valueType) {
        this(key, valueType, null);
    }

    ClientConfigProperties(String key, Class<?> valueType, String defaultValue) {
        this.key = key;
        this.valueType = valueType;
        this.defaultValue = defaultValue;
        this.defaultObjValue = parseValue(defaultValue);
    }

    public String getKey() {
        return key;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    @SuppressWarnings("unchecked")
    public <T> T getDefObjVal() {
        return (T) defaultObjValue;
    }

    public static final String HTTP_HEADER_PREFIX = "http_header_";

    public static final String SERVER_SETTING_PREFIX = "clickhouse_setting_";

    // Key used to identify default value in configuration map
    public static final String DEFAULT_KEY = "_default_";

    public static final String IGNORE_UNKNOWN_CONFIG_KEY = "ignore_unknown_config_key";

    public static String serverSetting(String key) {
        return SERVER_SETTING_PREFIX + key;
    }

    public static String httpHeader(String key) {
        return HTTP_HEADER_PREFIX + key.toUpperCase(Locale.US);
    }

    public static String commaSeparated(Collection<?> values) {
        StringBuilder sb = new StringBuilder();
        for (Object value : values) {
            sb.append(value.toString().replaceAll(",", "\\\\,")).append(",");
        }

        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    public static List<String> valuesFromCommaSeparated(String value) {
        if (value == null || value.isEmpty()) {
            return Collections.emptyList();
        }

        return Arrays.stream(value.split("(?<!\\\\),")).map(s -> s.replaceAll("\\\\,", ","))
                .collect(Collectors.toList());
    }

    public Object parseValue(String value) {
        if (value == null) {
            return null;
        }

        if (valueType.equals(String.class)) {
            return value;
        }

        if (valueType.equals(Boolean.class)) {
            if (value.equals("1")) return true;
            if (value.equals("0")) return false;
            return Boolean.parseBoolean(value);
        }

        if (valueType.equals(Integer.class)) {
            return Integer.parseInt(value);
        }

        if (valueType.equals(Long.class)) {
            return Long.parseLong(value);
        }

        if (valueType.equals(List.class)) {
            return valuesFromCommaSeparated(value);
        }

        if (valueType.isEnum()) {
            String configValue = value.trim();
            if (configValue.isEmpty()) {
                return null;
            }
            if (valueType.equals(ClickHouseFormat.class)) {
                try {
                    return ClickHouseFormat.fromString(configValue);
                } catch (IllegalArgumentException e) {
                    return configValue;
                }
            }
            Object[] constants = valueType.getEnumConstants();
            for (Object constant : constants) {
                if (constant.toString().equalsIgnoreCase(configValue)) {
                    return constant;
                }
            }
            throw new IllegalArgumentException("Invalid constant name '" + value + "' for enum " + valueType.getName());
        }

        if (valueType.equals(TimeZone.class)) {
            return TimeZone.getTimeZone(value);
        }

        if (valueType.equals(Map.class)) {
            return toKeyValuePairs(value);
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    public <T> T getOrDefault(Map<String, Object> configMap) {
        return (T) configMap.getOrDefault(getKey(), getDefObjVal());
    }

    public <T> void applyIfSet(Map<String, Object> configMap, Consumer<T> consumer) {
        T value = (T) configMap.get(getKey());
        if  (value != null) {
            consumer.accept(value);
        }
    }

    public static Map<String, Object> parseConfigMap(Map<String, String> configMap) {
        Map<String, Object> parsedConfig = new HashMap<>();

        Map<String, String> tmpMap = new HashMap<>(configMap);

        for (ClientConfigProperties config : ClientConfigProperties.values()) {
            String value = tmpMap.remove(config.getKey());
            if (value != null) {
                Object parsedValue;
                switch (config) {
                    case TYPE_HINT_MAPPING:
                        parsedValue = translateTypeHintMapping(value);
                        break;
                    default:
                        parsedValue = config.parseValue(value);
                }
                if (parsedValue != null) {
                    parsedConfig.put(config.getKey(), parsedValue);
                }
            }
        }

        final String customSettingsPrefix = configMap.getOrDefault(ClientConfigProperties.CUSTOM_SETTINGS_PREFIX.getKey(),
                CUSTOM_SETTINGS_PREFIX.getDefaultValue());
        if (customSettingsPrefix == null || customSettingsPrefix.isEmpty()) {
            throw new ClientException(ClientConfigProperties.CUSTOM_SETTINGS_PREFIX.getKey() + " must be not-blank");
        }
        for (String key : new HashSet<>(tmpMap.keySet())) {
            if (key.startsWith(HTTP_HEADER_PREFIX) || key.startsWith(SERVER_SETTING_PREFIX)) {
                parsedConfig.put(key, tmpMap.remove(key));
            } else if (key.startsWith(customSettingsPrefix)) {
                parsedConfig.put(serverSetting(key), tmpMap.remove(key));
            }
        }

        tmpMap.remove(ClientConfigProperties.IGNORE_UNKNOWN_CONFIG_KEY);
        if (!tmpMap.isEmpty()) {
            String msg = "Unknown and unmapped config properties: " + tmpMap.keySet();
            if (Boolean.parseBoolean(String.valueOf(configMap.get(IGNORE_UNKNOWN_CONFIG_KEY)))) {
                LOG.warn(msg);
            } else {
                throw new ClientMisconfigurationException(msg);
            }
        }

        return parsedConfig;
    }


    /**
     * Converts given string to key value pairs.
     * This is very simple implementation that do not handle edge cases like
     * {@code k1=v1, ,k2=v2}
     *
     * @param str string
     * @return non-null key value pairs
     */
    public static Map<String, String> toKeyValuePairs(String str) {
        if (str == null || str.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> map = new LinkedHashMap<>();
        String key = null;
        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = str.length(); i < len; i++) {
            char ch = str.charAt(i);
            if (ch == '\\' && i + 1 < len) {
                ch = str.charAt(++i);
                builder.append(ch);
                continue;
            }

            if (Character.isWhitespace(ch)) {
                if (builder.length() > 0) {
                    builder.append(ch);
                }
            } else if (ch == '=' && key == null) {
                key = builder.toString().trim();
                builder.setLength(0);
            } else if (ch == ',' && key != null) {
                String value = builder.toString().trim();
                builder.setLength(0);
                if (!key.isEmpty() && !value.isEmpty()) {
                    map.put(key, value);
                }
                key = null;
            } else {
                builder.append(ch);
            }
        }

        if (key != null && builder.length() > 0) {
            String value = builder.toString().trim();
            if (!key.isEmpty() && !value.isEmpty()) {
                map.put(key, value);
            }
        }

        return Collections.unmodifiableMap(map);
    }



    /**
     * Inverse of {@link #toKeyValuePairs(String)}: escapes {@code \}, {@code ,} and {@code =} with {@code \} and
     * skips entries with a {@code null} key or value.
     */
    static String fromKeyValuePairs(Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(',');
            }
            appendEscaped(sb, entry.getKey()).append('=');
            appendEscaped(sb, entry.getValue());
        }
        return sb.toString();
    }

    private static StringBuilder appendEscaped(StringBuilder sb, String str) {
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (ch == '\\' || ch == ',' || ch == '=') {
                sb.append('\\');
            }
            sb.append(ch);
        }
        return sb;
    }

    public static String mapToString(Map<?,?> map, Function<Object, String> valueConverter) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            sb.append(entry.getKey()).append("=").append(valueConverter.apply(entry.getValue())).append(",");
        }

        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    public static Map<ClickHouseDataType, Class<?>> translateTypeHintMapping(String mappingStr) {
        if (mappingStr == null || mappingStr.isEmpty()) {
            return AbstractBinaryFormatReader.NO_TYPE_HINT_MAPPING;
        }

        Map<String, String> mapping= ClientConfigProperties.toKeyValuePairs(mappingStr);
        Map<ClickHouseDataType, Class<?>> hintMapping = new HashMap<>();
        try {
            for (Map.Entry<String, String> entry : mapping.entrySet()) {
                hintMapping.put(ClickHouseDataType.of(entry.getKey()),
                        Class.forName(entry.getValue()));
            }
        } catch (ClassNotFoundException e) {
            throw new ClientMisconfigurationException("Failed to translate type-hint mapping", e);
        }
        return hintMapping;
    }
}
