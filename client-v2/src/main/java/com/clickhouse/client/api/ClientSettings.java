package com.clickhouse.client.api;

import com.clickhouse.client.api.internal.SettingsConverter;
import com.clickhouse.client.api.metrics.ClientMetrics;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * All known client settings at current version.
 *
 */
public class ClientSettings {

    public static final String HTTP_HEADER_PREFIX = "http_header_";

    public static final String SERVER_SETTING_PREFIX = "clickhouse_setting_";

    public static String commaSeparated(Collection<?> values) {
        StringBuilder sb = new StringBuilder();
        for (Object value : values) {
            sb.append(value.toString().replaceAll(",", "\\\\,")).append(",");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public static List<String> valuesFromCommaSeparated(String value) {
        if (value == null || value.isEmpty()) {
            return Collections.emptyList();
        }

        return Arrays.stream(value.split("(?<!\\\\),")).map(s -> s.replaceAll("\\\\,", ","))
                .collect(Collectors.toList());
    }

    public static final String SESSION_DB_ROLES = "session_db_roles";

    public static final String SETTING_LOG_COMMENT = SERVER_SETTING_PREFIX + "log_comment";

    public static final String HTTP_USE_BASIC_AUTH = "http_use_basic_auth";

    public static final String USER = "user";

    public static final String PASSWORD = "password";

    /**
     * Maximum number of active connection in internal connection pool.
     */
    public static final String HTTP_MAX_OPEN_CONNECTIONS = "max_open_connections";

    /**
     * HTTP keep-alive timeout override.
     */
    public static final String HTTP_KEEP_ALIVE_TIMEOUT = "http_keep_alive_timeout";

    public static final String USE_SERVER_TIMEZONE = "use_server_time_zone";

    public static final String USE_TIMEZONE = "use_time_zone";

    public static final String SERVER_TIMEZONE = "server_time_zone";

    public static final String ASYNC_OPERATIONS = "async";

    public static final String CONNECTION_TTL = "connection_ttl";

    public static final String CONNECTION_TIMEOUT = "connection_timeout";

    public static final String CONNECTION_REUSE_STRATEGY = "connection_reuse_strategy";

    public static final String SOCKET_OPERATION_TIMEOUT = "socket_timeout";

    public static final String SOCKET_RCVBUF_OPT = "socket_rcvbuf";

    public static final String SOCKET_SNDBUF_OPT = "socket_sndbuf";

    public static final String SOCKET_REUSEADDR_OPT = "socket_reuseaddr";

    public static final String SOCKET_KEEPALIVE_OPT = "socket_keepalive";

    public static final String SOCKET_TCP_NO_DELAY_OPT = "socket_tcp_nodelay";

    public static final String SOCKET_LINGER_OPT = "socket_linger";

    public static final String DATABASE = "database";

    public static final String COMPRESS_SERVER_RESPONSE = "compress"; // actually a server setting

    public static final String COMPRESS_CLIENT_REQUEST = "decompress"; // actually a server setting

    public static final String USE_HTTP_COMPRESSION = "client.use_http_compression";

    public static final String COMPRESSION_LZ4_UNCOMPRESSED_BUF_SIZE = "compression.lz4.uncompressed_buffer_size";

    public static final String PROXY_TYPE = "proxy_type"; // "http"

    public static final String PROXY_HOST = "proxy_host";

    public static final String PROXY_PORT = "proxy_port";

    public static final String PROXY_USER = "proxy_user";

    public static final String PROXY_PASSWORD = "proxy_password";

    public static final String MAX_EXECUTION_TIME = "max_execution_time";

    public static final String SSL_TRUST_STORE = "trust_store";

    public static final String SSL_KEYSTORE_TYPE = "key_store_type";

    public static final String SSL_KEY_STORE = "ssl_key_store";

    public static final String SSL_KEY_STORE_PASSWORD = "key_store_password";

    public static final String SSL_KEY =  "ssl_key";

    public static final String CA_CERTIFICATE = "sslrootcert";

    public static final String SSL_CERTIFICATE = "sslcert";

    public static final String RETRY_ON_FAILURE = "retry";

    public static final String INPUT_OUTPUT_FORMAT = "format";

    public static final String MAX_THREADS_PER_CLIENT = "max_threads_per_client";

    public static final String QUERY_ID = "query_id"; // actually a server setting

    public static final String CLIENT_NETWORK_BUFFER_SIZE = "client_network_buffer_size";

    // -- Experimental features --

    /**
     * Server will expect a string in JSON format and parse it into a JSON object.
     */
    public static final String INPUT_FORMAT_BINARY_READ_JSON_AS_STRING = "input_format_binary_read_json_as_string";

    /**
     * Server will return a JSON object as a string.
     */
    public static final String OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING = "output_format_binary_write_json_as_string";

}
