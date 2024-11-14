package com.clickhouse.client.api;

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

    // -- Experimental features --

    /**
     * Server will expect a string in JSON format and parse it into a JSON object.
     */
    public static final String INPUT_FORMAT_BINARY_READ_JSON_AS_STRING = "input_format_binary_read_json_as_string";

    /**
     * Server will return a JSON object as a string.
     */
    public static final String OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING = "output_format_binary_write_json_as_string";

    /**
     * Maximum number of active connection in internal connection pool.
     */
    public static final String HTTP_MAX_OPEN_CONNECTIONS = "max_open_connections";

    /**
     * HTTP keep-alive timeout override.
     */
    public static final String HTTP_KEEP_ALIVE_TIMEOUT = "http_keep_alive_timeout";
}
