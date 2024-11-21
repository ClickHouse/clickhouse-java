package com.clickhouse.client.api.internal;

import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.config.ClickHouseHealthCheckMethod;
import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.client.config.ClickHouseSslMode;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseDataConfig;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class SettingsConverter {

    public static Map<String, Serializable> toRequestSettings(Map<String, Object> settings, Map<String, Object> queryParams) {
        Map<String, Serializable> requestSettings = new HashMap<>();

        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            String key = entry.getKey();
            boolean isServerSetting = key.startsWith("clickhouse_setting_");
            if (!isServerSetting && REQUEST_OPTIONS.get(key) != null) {
                // This definitely is a request option
                continue;
            }

            if (isServerSetting) {
                key = key.substring("clickhouse_setting_".length());
            }

            if (entry.getValue() instanceof Map<?,?>) {
                Map<String, String> map = (Map<String, String>) entry.getValue();
                requestSettings.put(key, convertMapToStringValue(map));
            } else if (entry.getValue() instanceof Collection<?>) {
                Collection<?> collection = (Collection<?>) entry.getValue();
                requestSettings.put(key, convertCollectionToStringValue(collection));
            } else {
                requestSettings.put(key, (Serializable) entry.getValue());
            }
        }

        if (queryParams != null && !queryParams.isEmpty()) {
            queryParams.entrySet().forEach(e -> requestSettings.put("param_" + e.getKey(), (Serializable) e.getValue()));
        }

        return requestSettings;
    }

    public static Map<ClickHouseOption, Serializable> toRequestOptions(Map<String, Object> settings) {
        Map<ClickHouseOption, Serializable> requestOptions = new HashMap<>();

        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            if (!REQUEST_OPTIONS.containsKey(entry.getKey())) {
                continue;
            }

            ClickHouseOption option = REQUEST_OPTIONS.get(entry.getKey());
            if (entry.getValue() instanceof Map<?,?>) {
                Map<String, String> map = (Map<String, String>) entry.getValue();
                requestOptions.put(option, convertMapToStringValue(map));
            } else if (entry.getValue() instanceof Collection<?>) {
                Collection<?> collection = (Collection<?>) entry.getValue();
                requestOptions.put(option, convertCollectionToStringValue(collection));
            } else {
                requestOptions.put(option, (Serializable) entry.getValue());
            }
        }

        return requestOptions;
    }

    private static String convertMapToStringValue(Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : map.entrySet()) {
            sb.append(escape(e.getKey())).append('=').append(escape(e.getValue())).append(',');
        }
        sb.setLength(sb.length() - 1);
        return  sb.toString();
    }

    private static String convertCollectionToStringValue(Collection<?> collection) {
        StringBuilder sb = new StringBuilder();
        for (Object value : collection) {
            sb.append(escape(value.toString())).append(',');
        }
        sb.setLength(sb.length() - 1);
        return  sb.toString();
    }
    private static final Pattern ESCAPE_PATTERN = Pattern.compile("[,'\\\"=\\t\\n]{1}");

    public static String escape(String value) {
        return ESCAPE_PATTERN.matcher(value).replaceAll("\\\\$0");
    }

    private static final Map<String, ClickHouseOption> REQUEST_OPTIONS = createMapOfRequestOptions();


    public static Map<String, ClickHouseOption> createMapOfRequestOptions() {
        Map<String, ClickHouseOption> map = new HashMap<>();


        Arrays.asList(OldClientOptions.FORMAT,
                        OldClientOptions.MAX_EXECUTION_TIME,
                        OldClientOptions.CUSTOM_PARAMS,
                        OldClientOptions.AUTO_DISCOVERY,
                        OldClientOptions.CUSTOM_SETTINGS,
                        OldClientOptions.CUSTOM_SOCKET_FACTORY,
                        OldClientOptions.CUSTOM_SOCKET_FACTORY_OPTIONS,
                        OldClientOptions.CLIENT_NAME,
                        OldClientOptions.DECOMPRESS,
                        OldClientOptions.DECOMPRESS_ALGORITHM,
                        OldClientOptions.DECOMPRESS_LEVEL,
                        OldClientOptions.COMPRESS,
                        OldClientOptions.COMPRESS_ALGORITHM,
                        OldClientOptions.COMPRESS_LEVEL,
                        OldClientOptions.CONNECTION_TIMEOUT,
                        OldClientOptions.DATABASE,
                        OldClientOptions.MAX_BUFFER_SIZE,
                        OldClientOptions.BUFFER_SIZE,
                        OldClientOptions.BUFFER_QUEUE_VARIATION,
                        OldClientOptions.READ_BUFFER_SIZE,
                        OldClientOptions.WRITE_BUFFER_SIZE,
                        OldClientOptions.REQUEST_CHUNK_SIZE,
                        OldClientOptions.REQUEST_BUFFERING,
                        OldClientOptions.RESPONSE_BUFFERING,
                        OldClientOptions.MAX_MAPPER_CACHE,
                        OldClientOptions.MAX_QUEUED_BUFFERS,
                        OldClientOptions.MAX_QUEUED_REQUESTS,
                        OldClientOptions.MAX_RESULT_ROWS,
                        OldClientOptions.MAX_THREADS_PER_CLIENT,
                        OldClientOptions.PRODUCT_NAME,
                        OldClientOptions.NODE_CHECK_INTERVAL,
                        OldClientOptions.FAILOVER,
                        OldClientOptions.RETRY,
                        OldClientOptions.REPEAT_ON_SESSION_LOCK,
                        OldClientOptions.REUSE_VALUE_WRAPPER,
                        OldClientOptions.SERVER_TIME_ZONE,
                        OldClientOptions.SERVER_VERSION,
                        OldClientOptions.SESSION_TIMEOUT,
                        OldClientOptions.SESSION_CHECK,
                        OldClientOptions.SOCKET_TIMEOUT,
                        OldClientOptions.SSL,
                        OldClientOptions.SSL_MODE,
                        OldClientOptions.SSL_ROOT_CERTIFICATE,
                        OldClientOptions.SSL_CERTIFICATE,
                        OldClientOptions.SSL_KEY,
                        OldClientOptions.KEY_STORE_TYPE,
                        OldClientOptions.TRUST_STORE,
                        OldClientOptions.KEY_STORE_PASSWORD,
                        OldClientOptions.TRANSACTION_TIMEOUT,
                        OldClientOptions.WIDEN_UNSIGNED_TYPES,
                        OldClientOptions.USE_BINARY_STRING,
                        OldClientOptions.USE_BLOCKING_QUEUE,
                        OldClientOptions.USE_COMPILATION,
                        OldClientOptions.USE_OBJECTS_IN_ARRAYS,
                        OldClientOptions.USE_SERVER_TIME_ZONE,
                        OldClientOptions.USE_SERVER_TIME_ZONE_FOR_DATES,
                        OldClientOptions.SERVER_TIME_ZONE,
                        OldClientOptions.USE_TIME_ZONE)
                .forEach(option -> map.put(option.getKey(), option));

        return Collections.unmodifiableMap(map);
    }

    // copy from old client 
    public enum OldClientOptions implements ClickHouseOption {

        CUSTOM_PARAMS("custom_http_params", "", "Custom HTTP query parameters."),
        
        /**
         * Whether the client should run in async mode(e.g.
         * {@link com.clickhouse.client.ClickHouseClient#execute(com.clickhouse.client.ClickHouseRequest)}
         * in a separate thread).
         */
        ASYNC("async", ClickHouseDataConfig.DEFAULT_ASYNC, "Whether the client should run in async mode."),
        /**
         * Whether the client should discover more nodes from system tables and/or
         * clickhouse-keeper/zookeeper.
         */
        AUTO_DISCOVERY("auto_discovery", false,
                "Whether the client should discover more nodes from system tables and/or clickhouse-keeper/zookeeper."),
        /**
         * Custom server settings for all queries.
         */
        CUSTOM_SETTINGS("custom_settings", "", "Comma separated custom server settings for all queries."),
        /**
         * Custom socket factory.
         */
        CUSTOM_SOCKET_FACTORY("custom_socket_factory", "",
                "Full qualified class name of custom socket factory. This is only supported by TCP client and Apache Http Client."),
        /**
         * Additional socket factory options. Only useful only when
         * {@link #CUSTOM_SOCKET_FACTORY} is set.
         */
        CUSTOM_SOCKET_FACTORY_OPTIONS("custom_socket_factory_options", "",
                "Comma separated options for custom socket factory."),
        /**
         * Load balancing policy.
         */
        LOAD_BALANCING_POLICY("load_balancing_policy", "",
                "Load balancing policy, can be one of '', 'firstAlive', 'random', 'roundRobin', or full qualified class name implementing ClickHouseLoadBalancingPolicy."),
        /**
         * Load balancing tags for filtering out nodes.
         */
        LOAD_BALANCING_TAGS("load_balancing_tags", "", "Load balancing tags for filtering out nodes."),
        /**
         * Health check interval in milliseconds.
         */
        HEALTH_CHECK_INTERVAL("health_check_interval", 0,
                "Health check interval in milliseconds, zero or negative value means one-time."),
        /**
         * Health check method.
         */
        HEALTH_CHECK_METHOD("health_check_method", ClickHouseHealthCheckMethod.SELECT_ONE, "Health check method."),
        /**
         * Node discovery interval in milliseconds.
         */
        NODE_DISCOVERY_INTERVAL("node_discovery_interval", 0,
                "Node discovery interval in milliseconds, zero or negative value means one-time discovery."),
        /**
         * Maximum number of nodes can be discovered at a time.
         */
        NODE_DISCOVERY_LIMIT("node_discovery_limit", 100,
                "Maximum number of nodes can be discovered at a time, zero or negative value means no limit."),
        /**
         * Node check interval in milliseconds.
         */
        NODE_CHECK_INTERVAL("node_check_interval", 0,
                "Node check interval in milliseconds, negative number is treated as zero."),
        /**
         * Maximum number of nodes can be used for operation at a time.
         */
        NODE_GROUP_SIZE("node_group_size", 50,
                "Maximum number of nodes can be used for operation at a time, zero or negative value means all."),
        /**
         * Whether to perform health check against all nodes or just faulty ones.
         */
        CHECK_ALL_NODES("check_all_nodes", false,
                "Whether to perform health check against all nodes or just faulty ones."),
        /**
         * Default buffer size in byte for both request and response. It will be reset
         * to {@link #MAX_BUFFER_SIZE} if it's too large.
         */
        BUFFER_SIZE("buffer_size", ClickHouseDataConfig.DEFAULT_BUFFER_SIZE,
                "Default buffer size in byte for both request and response."),
        /**
         * Number of times the buffer queue is filled up before increasing capacity of
         * buffer queue. Zero or negative value means the queue length is fixed.
         */
        BUFFER_QUEUE_VARIATION("buffer_queue_variation", ClickHouseDataConfig.DEFAULT_BUFFER_QUEUE_VARIATION,
                "Number of times the buffer queue is filled up before increasing capacity of buffer queue. Zero or negative value means the queue length is fixed."),
        /**
         * Read buffer size in byte. It's mainly for input stream(e.g. reading data from
         * server response). Its value defaults to {@link #BUFFER_SIZE}, and it will be
         * reset to {@link #MAX_BUFFER_SIZE} when it's too large.
         */
        READ_BUFFER_SIZE("read_buffer_size", 0,
                "Read buffer size in byte, zero or negative value means same as buffer_size"),
        /**
         * Write buffer size in byte. It's mainly for output stream(e.g. writing data
         * into request). Its value defaults to {@link #BUFFER_SIZE}, and it will
         * be reset to {@link #MAX_BUFFER_SIZE} when it's too large.
         */
        WRITE_BUFFER_SIZE("write_buffer_size", 0,
                "Write buffer size in byte, zero or negative value means same as buffer_size"),
        /**
         * Maximum request chunk size in byte.
         */
        REQUEST_CHUNK_SIZE("request_chunk_size", 0,
                "Maximum request chunk size in byte, zero or negative value means same as write_buffer_size"),
        /**
         * Request buffering mode.
         */
        REQUEST_BUFFERING("request_buffering", ClickHouseDefaults.BUFFERING.getDefaultValue(),
                "Request buffering mode"),
        /**
         * Response buffering mode.
         */
        RESPONSE_BUFFERING("response_buffering", ClickHouseDefaults.BUFFERING.getDefaultValue(),
                "Response buffering mode."),
        /**
         * Client name.
         */
        CLIENT_NAME("client_name", "ClickHouse Java Client",
                "Client name, which is either 'client_name' or 'http_user_agent' shows up in system.query_log table."),
        /**
         * Whether server will compress response to client or not.
         */
        COMPRESS("compress", true, "Whether the server will compress response it sends to client."),
        /**
         * Whether server will decompress request from client or not.
         */
        DECOMPRESS("decompress", false, "Whether the server will decompress request from client."),
        /**
         * Compression algorithm server will use to compress response, when
         * {@link #COMPRESS} is {@code true}.
         */
        COMPRESS_ALGORITHM("compress_algorithm", ClickHouseCompression.LZ4,
                "Algorithm used for server to compress response."),
        /**
         * Compression algorithm server will use to decompress request, when
         * {@link #DECOMPRESS} is {@code true}.
         */
        DECOMPRESS_ALGORITHM("decompress_algorithm", ClickHouseCompression.LZ4,
                "Algorithm for server to decompress request."),
        /**
         * Compression level for compressing server response.
         */
        COMPRESS_LEVEL("compress_level", ClickHouseDataConfig.DEFAULT_READ_COMPRESS_LEVEL,
                "Compression level for response, -1 standards for default"),
        /**
         * Compression level for decompress client request.
         */
        DECOMPRESS_LEVEL("decompress_level", ClickHouseDataConfig.DEFAULT_WRITE_COMPRESS_LEVEL,
                "Compression level for request, -1 standards for default"),

        /**
         * Connection timeout in milliseconds.
         */
        CONNECTION_TIMEOUT("connect_timeout", 5000,
                "Connection timeout in milliseconds. It's also used for waiting a connection being closed."),
        /**
         * Default database.
         */
        DATABASE("database", "", "Default database."),
        /**
         * Maximum number of times failover can happen for a request.
         */
        FAILOVER("failover", 0,
                "Maximum number of times failover can happen for a request, zero or negative value means no failover."),
        /**
         * Default format.
         */
        FORMAT("format", ClickHouseDataConfig.DEFAULT_FORMAT, "Default format."),
        /**
         * Whether to log leading comment(as log_comment in system.query_log) of the
         * query.
         */
        LOG_LEADING_COMMENT("log_leading_comment", false,
                "Whether to log leading comment(as log_comment in system.query_log) of the query."),
        /**
         * Maximum buffer size in byte can be used for streaming. It's not supposed to
         * be larger than {@code Integer.MAX_VALUE - 8}.
         */
        MAX_BUFFER_SIZE("max_buffer_size", ClickHouseDataConfig.DEFAULT_MAX_BUFFER_SIZE,
                "Maximum buffer size in byte can be used for streaming."),
        /**
         * Maximum number of mappers can be cached.
         */
        MAX_MAPPER_CACHE("max_mapper_cache", ClickHouseDataConfig.DEFAULT_MAX_MAPPER_CACHE,
                "Maximum number of mappers can be cached."),
        /**
         * Maximum query execution time in seconds.
         */
        MAX_EXECUTION_TIME("max_execution_time", 0, "Maximum query execution time in seconds, 0 means no limit."),
        /**
         * Maximum queued in-memory buffers.
         */
        MAX_QUEUED_BUFFERS("max_queued_buffers", ClickHouseDataConfig.DEFAULT_MAX_QUEUED_BUFFERS,
                "Maximum queued in-memory buffers, 0 or negative number means no limit."),
        /**
         * Maxium queued requests. When {@link #MAX_THREADS_PER_CLIENT} is greater than
         * zero, this will also be applied to client's thread pool as well.
         */
        MAX_QUEUED_REQUESTS("max_queued_requests", 0, "Maximum queued requests, 0 or negative number means no limit."),
        /**
         * Maximum rows allowed in the result.
         */
        MAX_RESULT_ROWS("max_result_rows", 0L,
                "Limit on the number of rows in the result. "
                        + "Also checked for subqueries, and on remote servers when running parts of a distributed query."),
        /**
         * Maximum size of thread pool for each client.
         */
        MAX_THREADS_PER_CLIENT("max_threads_per_client", 0,
                "Size of thread pool for each client instance, 0 or negative number means the client will use shared thread pool."),
        /**
         * Product name usered in user agent.
         */
        PRODUCT_NAME("product_name", "ClickHouse-JavaClient", "Product name used in user agent."),
        /**
         * Method to rename response columns.
         */
        RENAME_RESPONSE_COLUMN("rename_response_column", ClickHouseDataConfig.DEFAULT_COLUMN_RENAME_METHOD,
                "Method to rename response columns."),
        /**
         * Maximum number of times retry can happen for a request.
         */
        RETRY("retry", 0,
                "Maximum number of times retry can happen for a request, zero or negative value means no retry."),
        /**
         * Whether to repeat execution when session is locked, until timed out(according
         * to {@link #SESSION_TIMEOUT} or {@link #CONNECTION_TIMEOUT}).
         */
        REPEAT_ON_SESSION_LOCK("repeat_on_session_lock", true,
                "Whether to repeat execution when session is locked, until timed out(according to 'session_timeout' or 'connect_timeout')."),
        /**
         * Whether to reuse wrapper of value(e.g. ClickHouseValue or
         * ClickHouseRecord) for memory efficiency.
         */
        REUSE_VALUE_WRAPPER("reuse_value_wrapper", ClickHouseDataConfig.DEFAULT_REUSE_VALUE_WRAPPER,
                "Whether to reuse wrapper of value(e.g. ClickHouseValue or ClickHouseRecord) for memory efficiency."),
        /**
         * Server revision.
         */
        SERVER_REVISION("server_revision", 54442, "Server revision."),
        /**
         * Server timezone.
         */
        SERVER_TIME_ZONE("server_time_zone", "", "Server timezone."),
        /**
         * Server version.
         */
        SERVER_VERSION("server_version", "", "Server version."),
        /**
         * Session id.
         */
        SESSION_ID("session_id", "", "Session id"),
        /**
         * Whether to check if session id is validate.
         */
        SESSION_CHECK("session_check", false, "Whether to check if existence of session id."),
        /**
         * Session timeout in seconds.
         */
        SESSION_TIMEOUT("session_timeout", 0,
                "Session timeout in seconds. 0 or negative number means same as server default."),
        /**
         * Socket timeout in milliseconds.
         */
        SOCKET_TIMEOUT("socket_timeout", ClickHouseDataConfig.DEFAULT_TIMEOUT, "Socket timeout in milliseconds."),
        /**
         * Whether allows for the reuse of local addresses and ports. See
         * {@link java.net.StandardSocketOptions#SO_REUSEADDR}.
         */
        SOCKET_REUSEADDR("socket_reuseaddr", false,
                "Whether allows for the reuse of local addresses and ports. "
                        + "Only works for client using custom Socket(e.g. TCP client or HTTP provider with custom SocketFactory etc.)."),
        /**
         * Whether to enable keep-alive packets for a socket connection. See
         * {@link java.net.StandardSocketOptions#SO_KEEPALIVE}.
         */
        SOCKET_KEEPALIVE("socket_keepalive", false,
                "Whether to enable keep-alive packets for a socket connection. Only works for client using custom Socket."),
        /**
         * Seconds to wait while data is being transmitted before closing the socket.
         * Use negative number to disable the option. See
         * {@link java.net.StandardSocketOptions#SO_LINGER}.
         */
        SOCKET_LINGER("socket_linger", -1,
                "Seconds to wait while data is being transmitted before closing the socket. Use negative number to disable the option. "
                        + "Only works for client using custom Socket(e.g. TCP client or HTTP provider with custom SocketFactory etc.)."),
        /**
         * Type-of-service(TOS) or traffic class field in the IP header for a socket.
         * See {@link java.net.StandardSocketOptions#IP_TOS}.
         */
        SOCKET_IP_TOS("socket_ip_tos", 0,
                "Socket IP_TOS option which indicates IP package priority. Only works for client using custom Socket."),
        /**
         * See {@link java.net.StandardSocketOptions#TCP_NODELAY}.
         */
        SOCKET_TCP_NODELAY("socket_tcp_nodelay", false, ""),
        /**
         * Size of the socket receive buffer in bytes. See
         * {@link java.net.StandardSocketOptions#SO_RCVBUF}.
         */
        SOCKET_RCVBUF("socket_rcvbuf", 0,
                "Size of the socket receive buffer in bytes. Only works for client using custom Socket."),
        /**
         * Size of the socket send buffer in bytes. See
         * {@link java.net.StandardSocketOptions#SO_SNDBUF}.
         */
        SOCKET_SNDBUF("socket_sndbuf", 0,
                "Size of the socket send buffer in bytes. Only works for client using custom Socket."),
        // TODO: new and extended socket options(e.g SO_REUSEPORT and TCP_QUICKACK etc.)

        /**
         * Whether to enable SSL for the connection.
         */
        SSL("ssl", false, "Whether to enable SSL/TLS for the connection."),
        /**
         * SSL mode.
         */
        SSL_MODE("sslmode", ClickHouseSslMode.STRICT, "verify or not certificate: none (don't verify), strict (verify)"),
        /**
         * SSL root certificiate.
         */
        SSL_ROOT_CERTIFICATE("sslrootcert", "", "SSL/TLS root certificates."),
        /**
         * SSL certificiate.
         */
        SSL_CERTIFICATE("sslcert", "", "SSL/TLS certificate."),
        /**
         * SSL key.
         */
        SSL_KEY("sslkey", "", "RSA key in PKCS#8 format.", true),
        /**
         * Key Store type.
         */
        KEY_STORE_TYPE("key_store_type", "", "Specifies the type or format of the keystore/truststore file used for SSL/TLS configuration, such as \"JKS\" (Java KeyStore) or \"PKCS12.\"", true),
        /**
         * Trust Store.
         */
        TRUST_STORE("trust_store", "", "Path to the truststore file", true),
        /**
         * Trust Store password.
         */
        KEY_STORE_PASSWORD("key_store_password", "", "Password needed to access the keystore file specified in the keystore config", true),
        /**
         * Transaction timeout in seconds.
         */
        TRANSACTION_TIMEOUT("transaction_timeout", 0,
                "Transaction timeout in seconds. 0 or negative number means same as session_timeout."),
        /**
         * Whether to convert unsigned types to the next widest type(e.g. use
         * {@code short} for UInt8 instead of {@code byte}, and {@code UnsignedLong} for
         * UInt64).
         */
        WIDEN_UNSIGNED_TYPES("widen_unsigned_types", ClickHouseDataConfig.DEFAULT_WIDEN_UNSIGNED_TYPE,
                "Whether to convert unsigned types to the next widest type(e.g. use short for UInt8 instead of byte, and UnsignedLong for UInt64)."),
        /**
         * Whether to support binary string. Enable this option to treat
         * {@code FixedString} and {@code String} as byte array.
         */
        USE_BINARY_STRING("use_binary_string", ClickHouseDataConfig.DEFAULT_USE_BINARY_STRING,
                "Whether to support binary string. Enable this option to treat FixedString and String as byte array."),
        /**
         * Whether to use blocking queue for buffering.
         */
        USE_BLOCKING_QUEUE("use_blocking_queue", ClickHouseDataConfig.DEFAULT_USE_BLOCKING_QUEUE,
                "Whether to use blocking queue for buffering."),
        /**
         * Whether to use compilation(generated byte code) in object mapping and
         * serialization.
         */
        USE_COMPILATION("use_compilation", ClickHouseDataConfig.DEFAULT_USE_COMPILATION,
                "Whether to use compilation(generated byte code) in object mapping and serialization."),
        /**
         * Whether Object[] should be used instead of primitive arrays.
         */
        USE_OBJECTS_IN_ARRAYS("use_objects_in_arrays", ClickHouseDataConfig.DEFAULT_USE_OBJECT_IN_ARRAY,
                "Whether Object[] should be used instead of primitive arrays."),
        /**
         * Type of proxy can be used to access ClickHouse server. To use an HTTP/SOCKS
         * proxy, you must specify the {@link #PROXY_HOST} and {@link #PROXY_PORT}.
         */
        PROXY_TYPE("proxy_type", ClickHouseProxyType.IGNORE,
                "Type of proxy can be used to access ClickHouse server. To use an HTTP/SOCKS proxy, you must specify proxy_host and proxy_port."),
        /**
         * Set Clickhouse proxy hostname.
         */
        PROXY_HOST("proxy_host", "", "Set ClickHouse server proxy hostname."),
        /**
         * Set ClickHouse proxy port.
         */
        PROXY_PORT("proxy_port", -1, "Set ClickHouse server proxy port."),
        /**
         * Set Clickhouse proxy username.
         */
        PROXY_USERNAME("proxy_username", "", "Set ClickHouse server proxy username."),
        /**
         * Set ClickHouse proxy password.
         */
        PROXY_PASSWORD("proxy_password", "", "Set ClickHouse server proxy password."),
        /**
         * Whether to use server time zone.
         */
        USE_SERVER_TIME_ZONE("use_server_time_zone", true,
                "Whether to use server time zone. On connection init select timezone() will be executed"),
        /**
         * Whether to use time zone from server for Date.
         */
        USE_SERVER_TIME_ZONE_FOR_DATES("use_server_time_zone_for_dates", false,
                "Whether to use timezone from server on Date parsing in getDate(). "
                        + "If false, Date returned is a wrapper of a timestamp at start of the day in client timezone. "
                        + "If true - at start of the day in server or use_time_zone timezone."),
        /**
         * Custom time zone. Only works when {@code use_server_time_zone} is set to
         * false.
         */
        USE_TIME_ZONE("use_time_zone", "", "Time zone of all DateTime* values. "
                + "Only used when use_server_time_zone is false. Empty value means client time zone."),

        /**
         * Query ID to be attached to an operation
         */
        QUERY_ID("query_id", "", "Query id"),


        /**
         * Connection time to live in milliseconds. 0 or negative number means no limit.
         * Can be used to override keep-alive time suggested by a server.
         */
        CONNECTION_TTL("connection_ttl", 0L,
                "Connection time to live in milliseconds. 0 or negative number means no limit."),
        ;

        private final String key;
        private final Serializable defaultValue;
        private final Class<? extends Serializable> clazz;
        private final String description;
        private final boolean sensitive;

        private static final Map<String, OldClientOptions> options;

        static final String UNKNOWN = "unknown";
        public static final String LATEST_KNOWN_VERSION = "0.6.3";

        /**
         * Semantic version of the product.
         */
        public static final String PRODUCT_VERSION;
        /**
         * Revision(shortened git commit hash) of the product.
         */
        public static final String PRODUCT_REVISION;
        /**
         * Client O/S information in format of {@code <o/s name>/<o/s version>}.
         */
        public static final String CLIENT_OS_INFO;
        /**
         * Client JVM information in format of {@code <jvm name>/<jvm version>}.
         */
        public static final String CLIENT_JVM_INFO;
        /**
         * Client user name.
         */
        public static final String CLIENT_USER;
        /**
         * Client host name.
         */
        public static final String CLIENT_HOST;

        static {
            Map<String, OldClientOptions> map = new HashMap<>();
            for (OldClientOptions o : values()) {
                if (map.put(o.getKey(), o) != null) {
                    throw new IllegalStateException("Duplicated key found: " + o.getKey());
                }
            }
            options = Collections.unmodifiableMap(map);

            // <artifact-id> <version> (revision: <revision>)
            String ver = OldClientOptions.class.getPackage().getImplementationVersion();
            String[] parts = ver == null || ver.isEmpty() ? null : ver.split("\\s");
            if (parts != null && parts.length == 4 && parts[1].length() > 0 && parts[3].length() > 1
                    && ver.charAt(ver.length() - 1) == ')') {
                PRODUCT_VERSION = parts[1];
                ver = parts[3];
                PRODUCT_REVISION = ver.substring(0, ver.length() - 1);
            } else { // perhaps try harder by checking version from pom.xml?
                PRODUCT_VERSION = LATEST_KNOWN_VERSION;
                PRODUCT_REVISION = UNKNOWN;
            }


            CLIENT_OS_INFO = new StringBuilder().append(getSystemConfig("os.name", "O/S")).append('/')
                    .append(getSystemConfig("os.version", UNKNOWN)).toString();
            String javaVersion = System.getProperty("java.vendor.version");
            if (javaVersion == null || javaVersion.isEmpty() || javaVersion.indexOf(' ') >= 0) {
                javaVersion = getSystemConfig("java.vm.version", getSystemConfig("java.version", UNKNOWN));
            }
            CLIENT_JVM_INFO = new StringBuilder().append(getSystemConfig("java.vm.name", "Java")).append('/')
                    .append(javaVersion).toString();
            CLIENT_USER = getSystemConfig("user.name", UNKNOWN);
            try {
                ver = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e1) {
                // ignore
            }
            CLIENT_HOST = ver == null || ver.isEmpty() ? UNKNOWN : ver;
        }

        /**
         * Builds user-agent based on given product name. The user-agent will be
         * something look like
         * {@code <product name>/<product version> (<o/s name>/<o/s version>; <jvm name>/<jvm version>[; <additionalProperty>]; rv:<product revision>)}.
         *
         * @param productName        product name, null or blank string is treated as
         *                           {@code ClickHouse Java Client}
         * @param additionalProperty additional property if any
         * @return non-empty user-agent
         */
        public static String buildUserAgent(String productName, String additionalProperty) {
            productName = productName == null || productName.isEmpty() ? (String) PRODUCT_NAME.getEffectiveDefaultValue() : productName.trim();
            StringBuilder builder = new StringBuilder(productName).append(PRODUCT_VERSION.isEmpty() ? "" : "/" + PRODUCT_VERSION);

            if (!String.valueOf(PRODUCT_NAME.getDefaultValue()).equals(productName)) {//Append if someone changed the original value
                builder.append(" ").append(PRODUCT_NAME.getDefaultValue()).append(LATEST_KNOWN_VERSION);
            }
            builder.append(" (").append(CLIENT_JVM_INFO);
            if (additionalProperty != null && !additionalProperty.isEmpty()) {
                builder.append("; ").append(additionalProperty.trim());
            }
            return builder.append(")").toString();
        }

        /**
         * Gets system property and fall back to the given value as needed.
         *
         * @param propertyName non-null property name
         * @param defaultValue default value, only useful if it's not null
         * @return property value
         */
        public static String getSystemConfig(String propertyName, String defaultValue) {
            final String propertyValue = System.getProperty(propertyName);
            if (defaultValue == null) {
                return propertyValue;
            }

            return propertyValue == null || propertyValue.isEmpty() ? defaultValue : propertyValue;
        }

        /**
         * Gets client option by key.
         *
         * @param key key of the option
         * @return client option object, or null if not found
         */
        public static OldClientOptions fromKey(String key) {
            return options.get(key);
        }

        /**
         * Constructor of an option for client.
         *
         * @param <T>          type of the value, usually either String or Number(e.g.
         *                     int, long etc.)
         * @param key          non-null key, better use snake_case instead of camelCase
         *                     for consistency
         * @param defaultValue non-null default value
         * @param description  non-null description of this option
         */
        <T extends Serializable> OldClientOptions(String key, T defaultValue, String description) {
            this(key, defaultValue, description, false);
        }

        /**
         * Constructor of client option.
         *
         * @param <T>          type of the value, usually either String or Number(e.g.
         *                     int, long etc.)
         * @param key          non-null key, better use snake_case instead of camelCase
         *                     for consistency
         * @param defaultValue non-null default value
         * @param description  non-null description of this option
         * @param sensitive    whether the option is sensitive or not
         */
        <T extends Serializable> OldClientOptions(String key, T defaultValue, String description, boolean sensitive) {
            this.key = ClickHouseChecker.nonNull(key, "key");
            this.defaultValue = ClickHouseChecker.nonNull(defaultValue, "defaultValue");
            this.clazz = defaultValue.getClass();
            this.description = ClickHouseChecker.nonNull(description, "description");
            this.sensitive = sensitive;
        }

        @Override
        public Serializable getDefaultValue() {
            return defaultValue;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Class<? extends Serializable> getValueType() {
            return clazz;
        }

        @Override
        public boolean isSensitive() {
            return sensitive;
        }
    }
}
