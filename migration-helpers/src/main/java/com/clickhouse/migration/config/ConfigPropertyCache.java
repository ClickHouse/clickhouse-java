package com.clickhouse.migration.config;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.jdbc.DriverProperties;
import com.clickhouse.jdbc.JdbcConfig;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.DriverPropertyInfo;
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
    private static final String MAPPINGS_RESOURCE = "/com/clickhouse/migration/config/v1-to-v2-mappings.properties";

    private static final ConfigPropertyCache INSTANCE = new ConfigPropertyCache();

    private final Set<String> v1KnownProperties;
    private final Set<String> v2KnownProperties;
    private final Map<String, String> v1ToV2Mappings;

    public static ConfigPropertyCache getInstance() {
        return INSTANCE;
    }

    private ConfigPropertyCache() {
        Set<String> v1Props = new HashSet<>();
        Set<String> v2Props = new HashSet<>();
        Map<String, String> mappings = new HashMap<>();

        // 1. Load properties from resource files
        loadPropertiesResource(V1_KNOWN_RESOURCE, v1Props, null);
        loadPropertiesResource(V2_KNOWN_RESOURCE, v2Props, null);
        loadPropertiesResource(MAPPINGS_RESOURCE, null, mappings);

        // 2. Pre-load / enrich with runtime enum keys from v1 and v2
        enrichWithRuntimeEnums(v1Props, v2Props);

        this.v1KnownProperties = Collections.unmodifiableSet(v1Props);
        this.v2KnownProperties = Collections.unmodifiableSet(v2Props);
        this.v1ToV2Mappings = Collections.unmodifiableMap(mappings);

        log.debug("Pre-loaded {} v1 properties, {} v2 properties, {} mappings into cache.",
                v1KnownProperties.size(), v2KnownProperties.size(), v1ToV2Mappings.size());
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
        // v2 ClientConfigProperties
        try {
            for (ClientConfigProperties prop : ClientConfigProperties.values()) {
                if (prop.getKey() != null) {
                    v2Props.add(prop.getKey());
                }
            }
        } catch (Throwable t) {
            log.debug("Could not inspect ClientConfigProperties: {}", t.getMessage());
        }

        // v2 DriverProperties
        try {
            for (DriverProperties prop : DriverProperties.values()) {
                if (prop.getKey() != null) {
                    v2Props.add(prop.getKey());
                }
            }
        } catch (Throwable t) {
            log.debug("Could not inspect DriverProperties: {}", t.getMessage());
        }

        // v1 ClickHouseClientOption
        try {
            for (ClickHouseClientOption prop : ClickHouseClientOption.values()) {
                if (prop.getKey() != null) {
                    v1Props.add(prop.getKey());
                }
            }
        } catch (Throwable t) {
            log.debug("Could not inspect ClickHouseClientOption: {}", t.getMessage());
        }

        // v1 ClickHouseHttpOption
        try {
            for (ClickHouseHttpOption prop : ClickHouseHttpOption.values()) {
                if (prop.getKey() != null) {
                    v1Props.add(prop.getKey());
                }
            }
        } catch (Throwable t) {
            log.debug("Could not inspect ClickHouseHttpOption: {}", t.getMessage());
        }

        // v1 JdbcConfig
        try {
            for (DriverPropertyInfo info : JdbcConfig.getDriverProperties()) {
                if (info.name != null) {
                    v1Props.add(info.name);
                }
            }
        } catch (Throwable t) {
            log.debug("Could not inspect JdbcConfig properties: {}", t.getMessage());
        }
    }

    public boolean isV1KnownProperty(String key) {
        return key != null && v1KnownProperties.contains(key);
    }

    public boolean isV2KnownProperty(String key) {
        return key != null && v2KnownProperties.contains(key);
    }

    public String getV2MappedKey(String v1Key) {
        if (v1Key == null) {
            return null;
        }
        return v1ToV2Mappings.getOrDefault(v1Key, v1Key);
    }

    public Set<String> getV1KnownProperties() {
        return v1KnownProperties;
    }

    public Set<String> getV2KnownProperties() {
        return v2KnownProperties;
    }

    public Map<String, String> getV1ToV2Mappings() {
        return v1ToV2Mappings;
    }
}
