package com.clickhouse.migration.config;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class ConfigurationMigrationHelperTest {

    @DataProvider(name = "propertyConversionData")
    public Object[][] providePropertyConversionData() {
        return new Object[][]{
                // Standard known v2 properties should remain as-is
                {"user", "default", "user", "default"},
                {"password", "secret", "password", "secret"},
                {"database", "analytics", "database", "analytics"},
                {"ssl", "true", "ssl", "true"},
                {"async", "false", "async", "false"},

                // Unprefixed server settings in v1 must be prefixed with clickhouse_setting_ in v2
                {"max_threads", "8", "clickhouse_setting_max_threads", "8"},
                {"date_time_input_format", "best_effort", "clickhouse_setting_date_time_input_format", "best_effort"},
                {"join_use_nulls", "1", "clickhouse_setting_join_use_nulls", "1"},

                // Renamed properties in v1 should be converted to v2 names
                {"connect_timeout", "10000", "connection_timeout", "10000"},
                {"buffer_size", "65536", "client_network_buffer_size", "65536"},
                {"sslmode", "strict", "ssl_mode", "strict"},
                {"sslkey", "/path/to/key", "ssl_key", "/path/to/key"},
                {"proxy_username", "puser", "proxy_user", "puser"},

                // Existing v2 prefixed properties should be preserved
                {"clickhouse_setting_max_execution_time", "60", "clickhouse_setting_max_execution_time", "60"},
                {"http_header_X-Custom-Header", "custom-val", "http_header_X-Custom-Header", "custom-val"}
        };
    }

    @Test(dataProvider = "propertyConversionData")
    public void testConvertSingleProperty(String inputKey, String inputValue, String expectedKey, String expectedValue) {
        Map<String, String> input = new HashMap<>();
        input.put(inputKey, inputValue);

        Map<String, String> result = ConfigurationMigrationHelper.convertMap(input);

        Assert.assertEquals(result.size(), 1);
        Assert.assertTrue(result.containsKey(expectedKey), "Expected key missing: " + expectedKey);
        Assert.assertEquals(result.get(expectedKey), expectedValue);
    }

    @Test
    public void testConvertCustomSettingsAndHeaders() {
        Map<String, String> input = new LinkedHashMap<>();
        input.put("user", "my_user");
        input.put("custom_settings", "max_threads=4, join_use_nulls=1");
        input.put("custom_http_headers", "X-Trace-Id=123, X-App-Name=demo");

        Map<String, String> result = ConfigurationMigrationHelper.convertMap(input);

        Assert.assertEquals(result.get("user"), "my_user");
        Assert.assertEquals(result.get("clickhouse_setting_max_threads"), "4");
        Assert.assertEquals(result.get("clickhouse_setting_join_use_nulls"), "1");
        Assert.assertEquals(result.get("http_header_X-Trace-Id"), "123");
        Assert.assertEquals(result.get("http_header_X-App-Name"), "demo");
    }

    @Test
    public void testConvertPropertiesObject() {
        Properties v1Props = new Properties();
        v1Props.setProperty("user", "default");
        v1Props.setProperty("connect_timeout", "5000");
        v1Props.setProperty("max_threads", "16");

        Properties v2Props = ConfigurationMigrationHelper.convertProperties(v1Props);

        Assert.assertEquals(v2Props.getProperty("user"), "default");
        Assert.assertEquals(v2Props.getProperty("connection_timeout"), "5000");
        Assert.assertEquals(v2Props.getProperty("clickhouse_setting_max_threads"), "16");
    }

    @DataProvider(name = "urlConversionData")
    public Object[][] provideUrlConversionData() {
        return new Object[][]{
                {
                        "jdbc:clickhouse://localhost:8123/default?user=default&connect_timeout=5000&max_threads=8",
                        "jdbc:clickhouse://localhost:8123/default?user=default&connection_timeout=5000&clickhouse_setting_max_threads=8"
                },
                {
                        "http://localhost:8123/?ssl=true&date_time_input_format=best_effort",
                        "http://localhost:8123/?ssl=true&clickhouse_setting_date_time_input_format=best_effort"
                },
                {
                        "jdbc:clickhouse://localhost:8123/db",
                        "jdbc:clickhouse://localhost:8123/db"
                }
        };
    }

    @Test(dataProvider = "urlConversionData")
    public void testConvertUrl(String inputUrl, String expectedUrl) {
        String resultUrl = ConfigurationMigrationHelper.convertUrl(inputUrl);
        Assert.assertEquals(resultUrl, expectedUrl);
    }

    @Test
    public void testCacheInitializationAndPreload() {
        ConfigPropertyCache cache = ConfigPropertyCache.getInstance();

        Assert.assertTrue(cache.isV1KnownProperty("connect_timeout"));
        Assert.assertTrue(cache.isV2KnownProperty("connection_timeout"));
        Assert.assertTrue(cache.isV2KnownProperty("user"));

        Assert.assertEquals(cache.getV2MappedKey("connect_timeout"), "connection_timeout");
        Assert.assertEquals(cache.getV2MappedKey("buffer_size"), "client_network_buffer_size");
    }
}
