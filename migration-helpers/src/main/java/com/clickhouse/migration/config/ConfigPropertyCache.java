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
    private final Map<String, String> v2CanonicalKeys;

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
        Map<String, String> v2Canonical = new HashMap<>();

        // 1. Load properties from resource files
        loadPropertiesResource(V1_KNOWN_RESOURCE, v1Props, null, null);
        loadPropertiesResource(V2_KNOWN_RESOURCE, v2Props, null, v2Canonical);
        loadPropertiesResource(V1_DEPRECATED_RESOURCE, deprecatedProps, null, null);
        loadPropertiesResource(MAPPINGS_RESOURCE, null, mappings, null);

        // 2. Pre-load / enrich with runtime enum keys from v1 and v2 if present on classpath
        enrichWithRuntimeEnums(v1Props, v2Props, v2Canonical);

        this.v1KnownProperties = Collections.unmodifiableSet(v1Props);
        this.v2KnownProperties = Collections.unmodifiableSet(v2Props);
        this.v1DeprecatedProperties = Collections.unmodifiableSet(deprecatedProps);
        this.v1ToV2Mappings = Collections.unmodifiableMap(mappings);
        this.v2CanonicalKeys = Collections.unmodifiableMap(v2Canonical);

        log.debug("Pre-loaded {} v1 properties, {} v2 properties, {} deprecated properties, {} mappings into cache.",
                v1KnownProperties.size(), v2KnownProperties.size(), v1DeprecatedProperties.size(), v1ToV2Mappings.size());
    }

    private void loadPropertiesResource(String resourcePath, Set<String> targetSet, Map<String, String> targetMap, Map<String, String> canonicalMap) {
        try (InputStream in = getClass().getResourceAsStream(resourcePath)) {
            if (in != null) {
                Properties props = new Properties();
                try (InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
                    props.load(reader);
                }
                for (String key : props.stringPropertyNames()) {
                    String trimmedKey = key.trim();
                    String val = props.getProperty(key).trim();
                    if (targetSet != null) {
                        addKeyToSet(targetSet, trimmedKey);
                    }
                    if (targetMap != null) {
                        targetMap.put(trimmedKey, val);
                        targetMap.put(trimmedKey.toLowerCase(), val);
                    }
                    if (canonicalMap != null) {
                        canonicalMap.putIfAbsent(trimmedKey, trimmedKey);
                        canonicalMap.putIfAbsent(trimmedKey.toLowerCase(), trimmedKey);
                    }
                }
            } else {
                log.warn("Migration resource file not found on classpath: {}", resourcePath);
            }
        } catch (Exception e) {
            log.error("Failed to load migration resource file: {}", resourcePath, e);
        }
    }

    private void enrichWithRuntimeEnums(Set<String> v1Props, Set<String> v2Props, Map<String, String> v2Canonical) {
        // v2 ClientConfigProperties and ClientConfigurationProperties
        loadEnumKeysFromClasspath("com.clickhouse.client.api.ClientConfigProperties", v2Props, v2Canonical);
        loadEnumKeysFromClasspath("com.clickhouse.client.api.ClientConfigurationProperties", v2Props, v2Canonical);

        // v2 DriverProperties
        loadEnumKeysFromClasspath("com.clickhouse.jdbc.DriverProperties", v2Props, v2Canonical);

        // v1 ClickHouseClientOption
        loadEnumKeysFromClasspath("com.clickhouse.client.config.ClickHouseClientOption", v1Props, null);

        // v1 ClickHouseHttpOption
        loadEnumKeysFromClasspath("com.clickhouse.client.http.config.ClickHouseHttpOption", v1Props, null);

        // v1 JdbcConfig
        loadJdbcConfigFromClasspath(v1Props);
    }

    private void loadEnumKeysFromClasspath(String className, Set<String> targetSet, Map<String, String> canonicalMap) {
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
                            String keyStr = null;
                            if (getKeyMethod != null) {
                                try {
                                    Object keyObj = getKeyMethod.invoke(obj);
                                    if (keyObj != null) {
                                        keyStr = keyObj.toString();
                                    }
                                } catch (Exception e) {
                                    keyStr = obj.toString();
                                }
                            } else {
                                keyStr = obj.toString();
                            }

                            if (keyStr != null) {
                                addKeyToSetAndCanonicalMap(targetSet, canonicalMap, keyStr);
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
            if (driverProps instanceof Iterable) {
                for (Object info : (Iterable<?>) driverProps) {
                    if (info != null) {
                        Field nameField = info.getClass().getField("name");
                        Object nameObj = nameField.get(info);
                        if (nameObj != null) {
                            addKeyToSet(v1Props, nameObj.toString());
                        }
                    }
                }
            } else if (driverProps != null && driverProps.getClass().isArray()) {
                int length = Array.getLength(driverProps);
                for (int i = 0; i < length; i++) {
                    Object info = Array.get(driverProps, i);
                    if (info != null) {
                        Field nameField = info.getClass().getField("name");
                        Object nameObj = nameField.get(info);
                        if (nameObj != null) {
                            addKeyToSet(v1Props, nameObj.toString());
                        }
                    }
                }
            }
        } catch (Throwable t) {
            log.debug("Could not inspect JdbcConfig properties: {}", t.getMessage());
        }
    }

    private void addKeyToSet(Set<String> targetSet, String key) {
        if (key != null) {
            String trimmed = key.trim();
            if (!trimmed.isEmpty()) {
                targetSet.add(trimmed);
                targetSet.add(trimmed.toLowerCase());
            }
        }
    }

    private void addKeyToSetAndCanonicalMap(Set<String> targetSet, Map<String, String> canonicalMap, String key) {
        if (key != null) {
            String trimmed = key.trim();
            if (!trimmed.isEmpty()) {
                targetSet.add(trimmed);
                targetSet.add(trimmed.toLowerCase());
                if (canonicalMap != null) {
                    canonicalMap.putIfAbsent(trimmed, trimmed);
                    canonicalMap.putIfAbsent(trimmed.toLowerCase(), trimmed);
                }
            }
        }
    }

    /**
     * Checks if the key is a known v1 configuration property.
     *
     * @param key property name
     * @return true if key is known in v1
     */
    public boolean isV1KnownProperty(String key) {
        return key != null && (v1KnownProperties.contains(key) || v1KnownProperties.contains(key.toLowerCase()));
    }

    /**
     * Checks if the key is a known v2 configuration property.
     *
     * @param key property name
     * @return true if key is known in v2
     */
    public boolean isV2KnownProperty(String key) {
        return key != null && (v2KnownProperties.contains(key) || v2KnownProperties.contains(key.toLowerCase()));
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
        String mapped = v1ToV2Mappings.get(v1Key);
        if (mapped == null) {
            mapped = v1ToV2Mappings.get(v1Key.toLowerCase());
        }
        return mapped != null ? mapped : v1Key;
    }

    /**
     * Gets the canonical v2 property key for a known v2 key.
     *
     * @param key property name in v2 format
     * @return canonical v2 key name, or null if key is not known in v2
     */
    public String getV2CanonicalKey(String key) {
        if (key == null) {
            return null;
        }
        String canonical = v2CanonicalKeys.get(key);
        if (canonical == null) {
            canonical = v2CanonicalKeys.get(key.toLowerCase());
        }
        return canonical;
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
