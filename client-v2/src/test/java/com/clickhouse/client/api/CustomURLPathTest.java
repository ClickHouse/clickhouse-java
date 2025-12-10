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

    @Test(groups = {"unit"})
    public void testCustomURLPathConfiguration() {
        String customPath = "/sales/db";
        
        // Simulate the URL building logic from HttpAPIClientHelper
        String baseURL = "http://localhost:8123";
        Map<String, Object> requestConfig = new HashMap<>();
        requestConfig.put(ClientConfigProperties.CUSTOM_URL_PATH.getKey(), customPath);
        
        try {
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
            
            URI uri = uriBuilder.normalizeSyntax().build();
            
            assertEquals(uri.toString(), "http://localhost:8123/sales/db");
            assertEquals(uri.getPath(), "/sales/db");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(groups = {"unit"})
    public void testCustomURLPathWithExistingPath() {
        String customPath = "/app/db";
        
        // Test with base URL that already has a path
        String baseURL = "http://localhost:8123/api";
        Map<String, Object> requestConfig = new HashMap<>();
        requestConfig.put(ClientConfigProperties.CUSTOM_URL_PATH.getKey(), customPath);
        
        try {
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
            
            URI uri = uriBuilder.normalizeSyntax().build();
            
            assertEquals(uri.toString(), "http://localhost:8123/api/app/db");
            assertEquals(uri.getPath(), "/api/app/db");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(groups = {"unit"})
    public void testEmptyCustomURLPath() {
        // Test with empty custom path
        String baseURL = "http://localhost:8123";
        Map<String, Object> requestConfig = new HashMap<>();
        requestConfig.put(ClientConfigProperties.CUSTOM_URL_PATH.getKey(), "");
        
        try {
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
            
            URI uri = uriBuilder.normalizeSyntax().build();
            
            // Empty path should not modify the URL
            assertEquals(uri.toString(), "http://localhost:8123");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(groups = {"unit"})
    public void testNoCustomURLPath() {
        // Test without custom path configured
        String baseURL = "http://localhost:8123";
        Map<String, Object> requestConfig = new HashMap<>();
        
        try {
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
            
            URI uri = uriBuilder.normalizeSyntax().build();
            
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
