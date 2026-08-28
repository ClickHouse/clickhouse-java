package com.clickhouse.migration.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Migration helper for converting ClickHouse configuration properties and connection URLs
 * from v1 (0.7.1) format to v2 (0.9.8+) format.
 */
public class ConfigurationMigrationHelper {

    private static final Logger log = LoggerFactory.getLogger(ConfigurationMigrationHelper.class);

    /**
     * Prefix used for ClickHouse server settings in v2.
     */
    public static final String SERVER_SETTING_PREFIX = "clickhouse_setting_";

    /**
     * Prefix used for custom HTTP headers in v2.
     */
    public static final String HTTP_HEADER_PREFIX = "http_header_";

    /**
     * Converts a {@link Properties} object from v1 format to v2 format.
     *
     * @param v1Properties source properties in v1 format
     * @return converted properties in v2 format
     */
    public static Properties convertProperties(Properties v1Properties) {
        if (v1Properties == null) {
            return new Properties();
        }
        Properties v2Properties = new Properties();
        Map<String, String> convertedMap = convertMap(propertiesToMap(v1Properties));
        for (Map.Entry<String, String> entry : convertedMap.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                v2Properties.setProperty(entry.getKey(), entry.getValue());
            }
        }
        return v2Properties;
    }

    /**
     * Converts a configuration map from v1 format to v2 format.
     * Unprefixed ClickHouse server settings are automatically prefixed with {@code clickhouse_setting_}.
     * Custom HTTP headers are prefixed with {@code http_header_}.
     * Renamed v1 client/driver properties are mapped to their corresponding v2 names.
     *
     * @param v1Config map of configuration key-values in v1 format
     * @return converted map in v2 format
     */
    public static Map<String, String> convertMap(Map<String, String> v1Config) {
        if (v1Config == null) {
            return new LinkedHashMap<>();
        }
        ConfigPropertyCache cache = ConfigPropertyCache.getInstance();
        Map<String, String> v2Config = new LinkedHashMap<>();

        for (Map.Entry<String, String> entry : v1Config.entrySet()) {
            String origKey = entry.getKey();
            String value = entry.getValue();

            if (origKey == null) {
                continue;
            }

            String key = origKey.trim();

            // 1. If key already starts with clickhouse_setting_ or http_header_, preserve it as is.
            if (key.toLowerCase().startsWith(SERVER_SETTING_PREFIX) || key.toLowerCase().startsWith(HTTP_HEADER_PREFIX)) {
                v2Config.put(key, value);
                continue;
            }

            // 2. Handle legacy composite setting properties: custom_settings, custom_http_params, custom_params
            if ("custom_settings".equalsIgnoreCase(key) || "custom_http_params".equalsIgnoreCase(key) || "custom_params".equalsIgnoreCase(key)) {
                parseAndAddKeyValuePairs(value, SERVER_SETTING_PREFIX, v2Config);
                continue;
            }

            // Handle legacy composite header properties: custom_http_headers, custom_headers
            if ("custom_http_headers".equalsIgnoreCase(key) || "custom_headers".equalsIgnoreCase(key)) {
                parseAndAddKeyValuePairs(value, HTTP_HEADER_PREFIX, v2Config);
                continue;
            }

            // 3. Check for mapped renamed key (e.g. connect_timeout -> connection_timeout)
            String mappedKey = cache.getV2MappedKey(key);
            boolean isMapped = mappedKey != null && !mappedKey.equalsIgnoreCase(key);

            if (isMapped) {
                v2Config.put(mappedKey, value);
                continue;
            }

            // 4. Check if key is deprecated in v2 without conversion
            if (cache.isDeprecatedProperty(key)) {
                log.debug("Property '{}' is deprecated in v2 without conversion and will be ignored.", key);
                continue;
            }

            // 5. If key is a known v2 property, keep as client/driver property
            if (cache.isV2KnownProperty(key)) {
                v2Config.put(key, value);
            } else {
                // 6. Unrecognized key: in v1 this was implicitly treated as a ClickHouse server setting.
                // In v2, it must be explicitly prefixed with clickhouse_setting_
                String serverSettingKey = SERVER_SETTING_PREFIX + key;
                v2Config.put(serverSettingKey, value);
            }
        }

        return v2Config;
    }

    /**
     * Converts an Object-valued configuration map from v1 to v2 format.
     *
     * @param v1Config map with Object values
     * @return converted map with Object values
     */
    public static Map<String, Object> convertObjectMap(Map<String, Object> v1Config) {
        if (v1Config == null) {
            return new LinkedHashMap<>();
        }
        Map<String, String> strMap = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : v1Config.entrySet()) {
            if (entry.getKey() != null) {
                strMap.put(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
            }
        }
        Map<String, String> convertedStrMap = convertMap(strMap);
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : convertedStrMap.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * Converts a connection URL containing query parameters from v1 format to v2 format.
     * Example:
     * {@code jdbc:clickhouse://localhost:8123/db?max_threads=8&connect_timeout=5000}
     * -&gt; {@code jdbc:clickhouse://localhost:8123/db?clickhouse_setting_max_threads=8&connection_timeout=5000}
     *
     * @param url connection string/URL
     * @return converted connection string/URL in v2 format
     */
    public static String convertUrl(String url) {
        if (url == null || url.trim().isEmpty()) {
            return url;
        }

        int queryIndex = url.indexOf('?');
        if (queryIndex < 0 || queryIndex == url.length() - 1) {
            return url;
        }

        String baseUrl = url.substring(0, queryIndex);
        String queryString = url.substring(queryIndex + 1);

        Map<String, String> queryParams = parseQueryString(queryString);
        Map<String, String> convertedParams = convertMap(queryParams);

        if (convertedParams.isEmpty()) {
            return baseUrl;
        }

        StringBuilder sb = new StringBuilder(baseUrl).append('?');
        boolean first = true;
        for (Map.Entry<String, String> entry : convertedParams.entrySet()) {
            if (!first) {
                sb.append('&');
            }
            sb.append(entry.getKey());
            if (entry.getValue() != null) {
                sb.append('=').append(entry.getValue());
            }
            first = false;
        }

        return sb.toString();
    }

    private static Map<String, String> propertiesToMap(Properties props) {
        Map<String, String> map = new LinkedHashMap<>();
        for (String name : props.stringPropertyNames()) {
            map.put(name, props.getProperty(name));
        }
        return map;
    }

    private static void parseAndAddKeyValuePairs(String valueStr, String prefix, Map<String, String> targetMap) {
        if (valueStr == null || valueStr.trim().isEmpty()) {
            return;
        }
        String[] pairs = valueStr.split(",");
        for (String pair : pairs) {
            String trimmed = pair.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int eqIndex = trimmed.indexOf('=');
            if (eqIndex > 0) {
                String k = trimmed.substring(0, eqIndex).trim();
                String v = trimmed.substring(eqIndex + 1).trim();
                if (!k.isEmpty()) {
                    if (!k.toLowerCase().startsWith(prefix)) {
                        k = prefix + k;
                    }
                    targetMap.put(k, v);
                }
            }
        }
    }

    private static Map<String, String> parseQueryString(String queryString) {
        Map<String, String> map = new LinkedHashMap<>();
        if (queryString == null || queryString.trim().isEmpty()) {
            return map;
        }
        String[] pairs = queryString.split("&");
        for (String pair : pairs) {
            if (pair.isEmpty()) {
                continue;
            }
            int eqIdx = pair.indexOf('=');
            if (eqIdx >= 0) {
                String k = pair.substring(0, eqIdx);
                String v = pair.substring(eqIdx + 1);
                map.put(k, v);
            } else {
                map.put(pair, "");
            }
        }
        return map;
    }
}
