package com.clickhouse.examples.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Set;

/**
 * API-key authentication settings.
 *
 * <p>Bound from the {@code iot.auth} prefix. A request is authenticated when it presents
 * one of the configured API keys in the {@code X-API-Key} header.
 *
 * @param headerName header carrying the API key
 * @param apiKeys    the set of accepted API keys
 */
@ConfigurationProperties(prefix = "iot.auth")
public record AuthProperties(String headerName, Set<String> apiKeys) {

    public AuthProperties {
        if (headerName == null || headerName.isBlank()) {
            headerName = "X-API-Key";
        }
        apiKeys = apiKeys == null ? Set.of() : Set.copyOf(apiKeys);
    }

    public boolean isValid(String candidate) {
        return candidate != null && apiKeys.contains(candidate);
    }
}
