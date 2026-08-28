package com.clickhouse.migration.config;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache for v1 and v2 configuration properties, loaded from resource files and pre-loaded into memory.
 */
public class ConfigPropertyCache {

    private static final Logger log = LoggerFactory.getLogger(ConfigPropertyCache.class);

    private static final String V1_KNOWN_RESOURCE = "/com/clickhouse/migration/config/v1-known-properties.properties";
    private static final String V2_KNOWN_RESOURCE = "/com/clickhouse/migration/config/v2-known-properties.properties";
    private static final String V1_DEPRECATED_RESOURCE = "/com/clickhouse/migration/config/v1-deprecated-properties.properties";
    private static final String MAPPINGS_RESOURCE = "/com/clickhouse/migration/config/v1-to-v2-mappings.properties";

    private static final ConfigPropertyCache INSTANCE = new ConfigPropertyCache();

    private final Set<String> v1KnownProperties;
    private final Set<String> v2KnownProperties;
    private final Set<String> v1DeprecatedProperties;
    private final Map<String, String> v1ToV2Mappings;

    /**
     * Gets the singleton instance of {@link ConfigPropertyCache}.
     *
     * @return cache singleton instance
     */
    public static ConfigPropertyCache getInstance() {
        return INSTANCE;
    }

    private ConfigPropertyCache() {
        Set<String> v1Props = new HashSet<>();
        Set<String> v2Props = new HashSet<>();
        Set<String> deprecatedProps = new HashSet<>();
        Map<String, String> mappings = new HashMap<>();

        // 1. Load properties from resource files
        loadPropertiesResource(V1_KNOWN_RESOURCE, v1Props, null);
        loadPropertiesResource(V2_KNOWN_RESOURCE, v2Props, null);
        loadPropertiesResource(V1_DEPRECATED_RESOURCE, deprecatedProps, null);
        loadPropertiesResource(MAPPINGS_RESOURCE, null, mappings);

        // 2. Pre-load / enrich with runtime enum keys from v1 and v2 if present on classpath
        enrichWithRuntimeEnums(v1Props, v2Props);

        this.v1KnownProperties = Collections.unmodifiableSet(v1Props);
        this.v2KnownProperties = Collections.unmodifiableSet(v2Props);
        this.v1DeprecatedProperties = Collections.unmodifiableSet(deprecatedProps);
        this.v1ToV2Mappings = Collections.unmodifiableMap(mappings);

        log.debug("Pre-loaded {} v1 properties, {} v2 properties, {} deprecated properties, {} mappings into cache.",
                v1KnownProperties.size(), v2KnownProperties.size(), v1DeprecatedProperties.size(), v1ToV2Mappings.size());
    }

    private void loadPropertiesResource(String resourcePath, Set<String> targetSet, Map<String, String> targetMap) {
        try (InputStream in = getClass().getResourceAsStream(resourcePath)) {
            if (in != null) {
                Properties props = new Properties();
                try (InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
                    props.load(reader);
                }
                for (String key : props.stringPropertyNames()) {
                    if (targetSet != null) {
                        targetSet.add(key.trim());
                    }
                    if (targetMap != null) {
                        targetMap.put(key.trim(), props.getProperty(key).trim());
                    }
                }
            } else {
                log.warn("Migration resource file not found on classpath: {}", resourcePath);
            }
        } catch (Exception e) {
            log.error("Failed to load migration resource file: {}", resourcePath, e);
        }
    }

    private void enrichWithRuntimeEnums(Set<String> v1Props, Set<String> v2Props) {
        // v2 ClientConfigProperties and ClientConfigurationProperties
        loadEnumKeysFromClasspath("com.clickhouse.client.api.ClientConfigProperties", v2Props);
        loadEnumKeysFromClasspath("com.clickhouse.client.api.ClientConfigurationProperties", v2Props);

        // v2 DriverProperties
        loadEnumKeysFromClasspath("com.clickhouse.jdbc.DriverProperties", v2Props);

        // v1 ClickHouseClientOption
        loadEnumKeysFromClasspath("com.clickhouse.client.config.ClickHouseClientOption", v1Props);

        // v1 ClickHouseHttpOption
        loadEnumKeysFromClasspath("com.clickhouse.client.http.config.ClickHouseHttpOption", v1Props);

        // v1 JdbcConfig
        loadJdbcConfigFromClasspath(v1Props);
    }

    private void loadEnumKeysFromClasspath(String className, Set<String> targetSet) {
        try {
            Class<?> clazz = Class.forName(className, false, getClass().getClassLoader());
            if (clazz.isEnum()) {
                Object[] constants = clazz.getEnumConstants();
                if (constants != null) {
                    Method getKeyMethod = null;
                    try {
                        getKeyMethod = clazz.getMethod("getKey");
                    } catch (NoSuchMethodException ignored) {
                        // ignore if getKey() is missing
                    }

                    for (Object obj : constants) {
                        if (obj != null) {
                            if (getKeyMethod != null) {
                                try {
                                    Object keyObj = getKeyMethod.invoke(obj);
                                    if (keyObj != null) {
                                        targetSet.add(keyObj.toString());
                                    }
                                } catch (Exception e) {
                                    targetSet.add(obj.toString());
                                }
                            } else {
                                targetSet.add(obj.toString());
                            }
                        }
                    }
                }
            }
        } catch (Throwable t) {
            log.debug("Class {} is not present on classpath or could not be loaded: {}", className, t.getMessage());
        }
    }

    private void loadJdbcConfigFromClasspath(Set<String> v1Props) {
        try {
            Class<?> clazz = Class.forName("com.clickhouse.jdbc.JdbcConfig", false, getClass().getClassLoader());
            Method getDriverPropertiesMethod = clazz.getMethod("getDriverProperties");
            Object driverProps = getDriverPropertiesMethod.invoke(null);
            if (driverProps != null && driverProps.getClass().isArray()) {
                int length = Array.getLength(driverProps);
                for (int i = 0; i < length; i++) {
                    Object info = Array.get(driverProps, i);
                    if (info != null) {
                        Field nameField = info.getClass().getField("name");
                        Object nameObj = nameField.get(info);
                        if (nameObj != null) {
                            v1Props.add(nameObj.toString());
                        }
                    }
                }
            }
        } catch (Throwable t) {
            log.debug("Could not inspect JdbcConfig properties: {}", t.getMessage());
        }
    }

    /**
     * Checks if the key is a known v1 configuration property.
     *
     * @param key property name
     * @return true if key is known in v1
     */
    public boolean isV1KnownProperty(String key) {
        return key != null && v1KnownProperties.contains(key);
    }

    /**
     * Checks if the key is a known v2 configuration property.
     *
     * @param key property name
     * @return true if key is known in v2
     */
    public boolean isV2KnownProperty(String key) {
        return key != null && v2KnownProperties.contains(key);
    }

    /**
     * Checks if the property is deprecated in v2 without direct conversion.
     *
     * @param key property name
     * @return true if property is deprecated
     */
    public boolean isDeprecatedProperty(String key) {
        return key != null && (v1DeprecatedProperties.contains(key) || v1DeprecatedProperties.contains(key.toLowerCase()));
    }

    /**
     * Gets the mapped v2 key name for a given v1 property key.
     *
     * @param v1Key property name in v1 format
     * @return mapped property name in v2 format, or original key if no explicit mapping exists
     */
    public String getV2MappedKey(String v1Key) {
        if (v1Key == null) {
            return null;
        }
        return v1ToV2Mappings.getOrDefault(v1Key, v1Key);
    }

    /**
     * Gets the unmodifiable set of known v1 property keys.
     *
     * @return set of v1 property keys
     */
    public Set<String> getV1KnownProperties() {
        return v1KnownProperties;
    }

    /**
     * Gets the unmodifiable set of known v2 property keys.
     *
     * @return set of v2 property keys
     */
    public Set<String> getV2KnownProperties() {
        return v2KnownProperties;
    }

    /**
     * Gets the unmodifiable set of deprecated v1 property keys without conversion.
     *
     * @return set of deprecated property keys
     */
    public Set<String> getV1DeprecatedProperties() {
        return v1DeprecatedProperties;
    }

    /**
     * Gets the unmodifiable map of v1-to-v2 property mappings.
     *
     * @return map of v1-to-v2 property mappings
     */
    public Map<String, String> getV1ToV2Mappings() {
        return v1ToV2Mappings;
    }
}
