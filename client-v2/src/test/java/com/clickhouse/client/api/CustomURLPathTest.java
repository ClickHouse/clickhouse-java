package com.clickhouse.client.api;

import org.apache.hc.core5.net.URIBuilder;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for custom URL path configuration feature.
 * Tests that custom paths are correctly appended to endpoint URLs.
 */
public class CustomURLPathTest {

    /**
     * Helper method to build URI with custom path, simulating the logic from HttpAPIClientHelper.
     */
    private URI buildURIWithCustomPath(String baseURL, String customPath) throws URISyntaxException {
        Map<String, Object> requestConfig = new HashMap<>();
        if (customPath != null) {
            requestConfig.put(ClientConfigProperties.CUSTOM_URL_PATH.getKey(), customPath);
        }
        
        URIBuilder uriBuilder = new URIBuilder(baseURL);
        
        // Add custom URL path if configured
        String configuredPath = (String) requestConfig.get(ClientConfigProperties.CUSTOM_URL_PATH.getKey());
        if (configuredPath != null && !configuredPath.isEmpty()) {
            String existingPath = uriBuilder.getPath();
            if (existingPath == null || existingPath.isEmpty() || existingPath.equals("/")) {
                uriBuilder.setPath(configuredPath);
            } else {
                uriBuilder.setPath(existingPath + configuredPath);
            }
        }
        
        return uriBuilder.normalizeSyntax().build();
    }

    @Test(groups = {"unit"})
    public void testCustomURLPathConfiguration() {
        try {
            URI uri = buildURIWithCustomPath("http://localhost:8123", "/sales/db");
            
            assertEquals(uri.toString(), "http://localhost:8123/sales/db");
            assertEquals(uri.getPath(), "/sales/db");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(groups = {"unit"})
    public void testCustomURLPathWithExistingPath() {
        try {
            URI uri = buildURIWithCustomPath("http://localhost:8123/api", "/app/db");
            
            assertEquals(uri.toString(), "http://localhost:8123/api/app/db");
            assertEquals(uri.getPath(), "/api/app/db");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(groups = {"unit"})
    public void testEmptyCustomURLPath() {
        try {
            URI uri = buildURIWithCustomPath("http://localhost:8123", "");
            
            // Empty path should not modify the URL
            assertEquals(uri.toString(), "http://localhost:8123");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(groups = {"unit"})
    public void testNoCustomURLPath() {
        try {
            URI uri = buildURIWithCustomPath("http://localhost:8123", null);
            
            // No custom path should keep URL unchanged
            assertEquals(uri.toString(), "http://localhost:8123");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(groups = {"unit"})
    public void testClientBuilderCustomURLPath() {
        // Test that the builder method sets the configuration correctly
        Map<String, String> config = new HashMap<>();
        config.put(ClientConfigProperties.CUSTOM_URL_PATH.getKey(), "/sales/db");
        
        Map<String, Object> parsedConfig = ClientConfigProperties.parseConfigMap(config);
        
        String customPath = (String) parsedConfig.get(ClientConfigProperties.CUSTOM_URL_PATH.getKey());
        assertEquals(customPath, "/sales/db");
    }
}
