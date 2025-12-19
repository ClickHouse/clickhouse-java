package com.clickhouse.client.api;

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Unit tests for custom URL path configuration feature.
 * Tests that the configuration property and builder method work correctly.
 */
public class CustomURLPathTest {

    @Test(groups = {"unit"})
    public void testClientConfigPropertiesHasCustomURLPath() {
        // Test that CUSTOM_URL_PATH property exists and has correct key
        assertEquals(ClientConfigProperties.CUSTOM_URL_PATH.getKey(), "custom_url_path");
        assertEquals(ClientConfigProperties.CUSTOM_URL_PATH.getDefaultValue(), "");
    }

    @Test(groups = {"unit"})
    public void testClientBuilderCustomURLPathMethod() {
        // Test that the builder method exists and sets configuration correctly
        // We create a minimal client configuration to test the builder method
        try {
            Client.Builder builder = new Client.Builder()
                    .addEndpoint("http://localhost:8123")
                    .setUsername("default")
                    .setPassword("")
                    .customURLPath("/sales/db");
            
            // Build client to verify configuration is set
            Client client = builder.build();
            try {
                // Verify configuration was set correctly
                Map<String, String> config = client.getConfiguration();
                assertNotNull(config);
                assertEquals(config.get(ClientConfigProperties.CUSTOM_URL_PATH.getKey()), "/sales/db");
            } finally {
                client.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to test customURLPath builder method", e);
        }
    }

    @Test(groups = {"unit"})
    public void testClientConfigPropertyParsing() {
        // Test that the configuration property can be parsed correctly
        Map<String, String> config = new HashMap<>();
        config.put(ClientConfigProperties.CUSTOM_URL_PATH.getKey(), "/sales/db");
        
        Map<String, Object> parsedConfig = ClientConfigProperties.parseConfigMap(config);
        
        String customPath = (String) parsedConfig.get(ClientConfigProperties.CUSTOM_URL_PATH.getKey());
        assertEquals(customPath, "/sales/db");
    }

    @Test(groups = {"unit"})
    public void testEmptyCustomURLPath() {
        // Test with empty custom path
        Map<String, String> config = new HashMap<>();
        config.put(ClientConfigProperties.CUSTOM_URL_PATH.getKey(), "");
        
        Map<String, Object> parsedConfig = ClientConfigProperties.parseConfigMap(config);
        
        String customPath = (String) parsedConfig.get(ClientConfigProperties.CUSTOM_URL_PATH.getKey());
        assertEquals(customPath, "");
    }

    @Test(groups = {"unit"})
    public void testNoCustomURLPathConfiguration() {
        // Test without custom path configured - should use default
        Map<String, String> config = new HashMap<>();
        // Don't set CUSTOM_URL_PATH
        
        Map<String, Object> parsedConfig = ClientConfigProperties.parseConfigMap(config);
        
        // Should not be in parsed config if not provided
        Object customPath = parsedConfig.get(ClientConfigProperties.CUSTOM_URL_PATH.getKey());
        // Either null or empty string is acceptable for unset value
        if (customPath != null) {
            assertEquals(customPath, "");
        }
    }

    @Test(groups = {"unit"})
    public void testCustomURLPathWithDifferentPaths() {
        // Test various path formats
        String[] testPaths = {
            "/sales/db",
            "/app/db",
            "/custom",
            "/a/b/c/d",
            "/123/456"
        };
        
        for (String testPath : testPaths) {
            Map<String, String> config = new HashMap<>();
            config.put(ClientConfigProperties.CUSTOM_URL_PATH.getKey(), testPath);
            
            Map<String, Object> parsedConfig = ClientConfigProperties.parseConfigMap(config);
            
            String customPath = (String) parsedConfig.get(ClientConfigProperties.CUSTOM_URL_PATH.getKey());
            assertEquals(customPath, testPath, "Failed for path: " + testPath);
        }
    }
}
