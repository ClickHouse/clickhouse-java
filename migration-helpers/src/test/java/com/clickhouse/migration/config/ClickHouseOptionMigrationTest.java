package com.clickhouse.migration.config;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.config.ClickHouseOption;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class ClickHouseOptionMigrationTest {

    @Test
    public void testAllClickHouseOptionDerivativesAreConvertedOrCleanedUp() {
        Map<String, String> v1OptionsMap = new LinkedHashMap<>();

        // 1. Add all enum options from ClickHouseClientOption
        for (ClickHouseClientOption opt : ClickHouseClientOption.values()) {
            if (opt.getKey() != null) {
                v1OptionsMap.put(opt.getKey(), getSampleValueForOption(opt.getKey()));
            }
        }

        // 2. Add all enum options from ClickHouseHttpOption
        for (ClickHouseHttpOption opt : ClickHouseHttpOption.values()) {
            if (opt.getKey() != null) {
                v1OptionsMap.put(opt.getKey(), getSampleValueForOption(opt.getKey()));
            }
        }

        // 3. Add all enum options from ClickHouseDefaults
        for (ClickHouseDefaults opt : ClickHouseDefaults.values()) {
            if (opt.getKey() != null) {
                v1OptionsMap.put(opt.getKey(), getSampleValueForOption(opt.getKey()));
            }
        }

        // 4. Add all known v1 options from cache
        ConfigPropertyCache cache = ConfigPropertyCache.getInstance();
        for (String v1Key : cache.getV1KnownProperties()) {
            v1OptionsMap.put(v1Key, getSampleValueForOption(v1Key));
        }

        Assert.assertFalse(v1OptionsMap.isEmpty(), "v1OptionsMap should contain ClickHouseOption derivatives.");

        // Convert options map
        Map<String, String> convertedMap = ConfigurationMigrationHelper.convertMap(v1OptionsMap);
        Set<String> deprecatedProps = cache.getV1DeprecatedProperties();

        // Verify conversion rules for every single v1 option
        for (Map.Entry<String, String> entry : v1OptionsMap.entrySet()) {
            String origKey = entry.getKey();

            if (deprecatedProps.contains(origKey) || deprecatedProps.contains(origKey.toLowerCase())) {
                // Deprecated options without conversion must NOT be in the converted map
                Assert.assertFalse(convertedMap.containsKey(origKey),
                        "Deprecated option '" + origKey + "' should have been cleaned up/ignored.");
                Assert.assertFalse(convertedMap.containsKey(ConfigurationMigrationHelper.SERVER_SETTING_PREFIX + origKey),
                        "Deprecated option '" + origKey + "' should not be converted to a server setting.");
            } else if ("custom_settings".equalsIgnoreCase(origKey) || "custom_http_params".equalsIgnoreCase(origKey) || "custom_params".equalsIgnoreCase(origKey) || "custom_http_headers".equalsIgnoreCase(origKey) || "custom_headers".equalsIgnoreCase(origKey)) {
                // Legacy composite properties are unpacked into individual clickhouse_setting_ / http_header_ entries
                Assert.assertFalse(convertedMap.containsKey(origKey),
                        "Composite option '" + origKey + "' should be unpacked rather than remaining as a raw property.");
            } else {
                String mappedKey = cache.getV2MappedKey(origKey);
                if (mappedKey != null && !mappedKey.equalsIgnoreCase(origKey)) {
                    // Mapped keys should be converted to their v2 name
                    Assert.assertTrue(convertedMap.containsKey(mappedKey),
                            "Mapped option '" + origKey + "' -> '" + mappedKey + "' should be present in converted map.");
                } else if (cache.isV2KnownProperty(origKey)) {
                    // Known v2 property should remain present
                    Assert.assertTrue(convertedMap.containsKey(origKey),
                            "Known v2 option '" + origKey + "' should be present in converted map.");
                } else {
                    // Un-prefixed server settings should receive clickhouse_setting_ prefix
                    String expectedSettingKey = ConfigurationMigrationHelper.SERVER_SETTING_PREFIX + origKey;
                    Assert.assertTrue(convertedMap.containsKey(expectedSettingKey),
                            "Unrecognized option '" + origKey + "' should be prefixed with " + expectedSettingKey);
                }
            }
        }
    }

    @Test
    public void testClientInstantiationWithConvertedOptionsMap() {
        Map<String, String> v1Props = new LinkedHashMap<>();
        v1Props.put("user", "default");
        v1Props.put("password", "secret");
        v1Props.put("database", "default");
        v1Props.put("connect_timeout", "5000");
        v1Props.put("buffer_size", "65536");
        v1Props.put("max_threads", "8");
        v1Props.put("protocol", "http");             // deprecated - ignored
        v1Props.put("use_compilation", "true");      // deprecated - ignored
        v1Props.put("custom_settings", "join_use_nulls=1");
        v1Props.put("custom_http_headers", "X-App-Name=test");

        Map<String, String> convertedMap = ConfigurationMigrationHelper.convertMap(v1Props);

        // Client in client-v2 should be successfully instantiated with converted options
        try (Client client = new Client.Builder()
                .addEndpoint("http://localhost:8123")
                .setOptions(convertedMap)
                .build()) {

            Assert.assertNotNull(client, "Client instance should be successfully created.");
        }
    }

    private String getSampleValueForOption(String key) {
        if ("connect_timeout".equalsIgnoreCase(key) || "socket_timeout".equalsIgnoreCase(key) || "alive_timeout".equalsIgnoreCase(key)) {
            return "5000";
        }
        if ("buffer_size".equalsIgnoreCase(key) || "read_buffer_size".equalsIgnoreCase(key)) {
            return "65536";
        }
        if ("ssl".equalsIgnoreCase(key) || "async".equalsIgnoreCase(key) || "compress".equalsIgnoreCase(key)) {
            return "true";
        }
        if ("port".equalsIgnoreCase(key)) {
            return "8123";
        }
        return "sample_value";
    }
}
