package com.clickhouse.client.api.internal;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.config.ClickHouseOption;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class SettingsConverter {

    public static Map<String, Serializable> toRequestSettings(Map<String, Object> settings, Map<String, Object> queryParams) {
        Map<String, Serializable> requestSettings = new HashMap<>();

        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            if (REQUEST_OPTIONS.get(entry.getKey()) != null) {
                // This definitely is a request option
                continue;
            }

            if (entry.getValue() instanceof Map<?,?>) {
                Map<String, String> map = (Map<String, String>) entry.getValue();
                requestSettings.put(entry.getKey(), convertMapToStringValue(map));
            } else if (entry.getValue() instanceof Collection<?>) {
                Collection<?> collection = (Collection<?>) entry.getValue();
                requestSettings.put(entry.getKey(), convertCollectionToStringValue(collection));
            } else {
                requestSettings.put(entry.getKey(), (Serializable) entry.getValue());
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


        Arrays.asList(ClickHouseClientOption.FORMAT,
                        ClickHouseClientOption.MAX_EXECUTION_TIME,
                        ClickHouseHttpOption.CUSTOM_PARAMS,
                        ClickHouseClientOption.AUTO_DISCOVERY,
                        ClickHouseClientOption.CUSTOM_SETTINGS,
                        ClickHouseClientOption.CUSTOM_SOCKET_FACTORY,
                        ClickHouseClientOption.CUSTOM_SOCKET_FACTORY_OPTIONS,
                        ClickHouseClientOption.CLIENT_NAME,
                        ClickHouseClientOption.DECOMPRESS,
                        ClickHouseClientOption.DECOMPRESS_ALGORITHM,
                        ClickHouseClientOption.DECOMPRESS_LEVEL,
                        ClickHouseClientOption.COMPRESS,
                        ClickHouseClientOption.COMPRESS_ALGORITHM,
                        ClickHouseClientOption.COMPRESS_LEVEL,
                        ClickHouseClientOption.CONNECTION_TIMEOUT,
                        ClickHouseClientOption.DATABASE,
                        ClickHouseClientOption.MAX_BUFFER_SIZE,
                        ClickHouseClientOption.BUFFER_SIZE,
                        ClickHouseClientOption.BUFFER_QUEUE_VARIATION,
                        ClickHouseClientOption.READ_BUFFER_SIZE,
                        ClickHouseClientOption.WRITE_BUFFER_SIZE,
                        ClickHouseClientOption.REQUEST_CHUNK_SIZE,
                        ClickHouseClientOption.REQUEST_BUFFERING,
                        ClickHouseClientOption.RESPONSE_BUFFERING,
                        ClickHouseClientOption.MAX_MAPPER_CACHE,
                        ClickHouseClientOption.MAX_QUEUED_BUFFERS,
                        ClickHouseClientOption.MAX_QUEUED_REQUESTS,
                        ClickHouseClientOption.MAX_RESULT_ROWS,
                        ClickHouseClientOption.MAX_THREADS_PER_CLIENT,
                        ClickHouseClientOption.PRODUCT_NAME,
                        ClickHouseClientOption.NODE_CHECK_INTERVAL,
                        ClickHouseClientOption.FAILOVER,
                        ClickHouseClientOption.RETRY,
                        ClickHouseClientOption.REPEAT_ON_SESSION_LOCK,
                        ClickHouseClientOption.REUSE_VALUE_WRAPPER,
                        ClickHouseClientOption.SERVER_TIME_ZONE,
                        ClickHouseClientOption.SERVER_VERSION,
                        ClickHouseClientOption.SESSION_TIMEOUT,
                        ClickHouseClientOption.SESSION_CHECK,
                        ClickHouseClientOption.SOCKET_TIMEOUT,
                        ClickHouseClientOption.SSL,
                        ClickHouseClientOption.SSL_MODE,
                        ClickHouseClientOption.SSL_ROOT_CERTIFICATE,
                        ClickHouseClientOption.SSL_CERTIFICATE,
                        ClickHouseClientOption.SSL_KEY,
                        ClickHouseClientOption.KEY_STORE_TYPE,
                        ClickHouseClientOption.TRUST_STORE,
                        ClickHouseClientOption.KEY_STORE_PASSWORD,
                        ClickHouseClientOption.TRANSACTION_TIMEOUT,
                        ClickHouseClientOption.WIDEN_UNSIGNED_TYPES,
                        ClickHouseClientOption.USE_BINARY_STRING,
                        ClickHouseClientOption.USE_BLOCKING_QUEUE,
                        ClickHouseClientOption.USE_COMPILATION,
                        ClickHouseClientOption.USE_OBJECTS_IN_ARRAYS,
                        ClickHouseClientOption.USE_SERVER_TIME_ZONE,
                        ClickHouseClientOption.USE_SERVER_TIME_ZONE_FOR_DATES,
                        ClickHouseClientOption.USE_TIME_ZONE)
                .forEach(option -> map.put(option.getKey(), option));

        return Collections.unmodifiableMap(map);
    }
}
