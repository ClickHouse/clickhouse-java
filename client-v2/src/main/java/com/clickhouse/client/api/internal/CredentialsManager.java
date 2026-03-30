package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientMisconfigurationException;
import org.apache.hc.core5.http.HttpHeaders;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages mutable authentication-related client settings.
 */
public class CredentialsManager {
    private final Map<String, Object> configuration;
    private final Object lock = new Object();

    public CredentialsManager(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    public Map<String, Object> snapshot() {
        synchronized (lock) {
            return new HashMap<>(configuration);
        }
    }

    public void setCredentials(String username, String password) {
        synchronized (lock) {
            configuration.put(ClientConfigProperties.USER.getKey(), username);
            configuration.put(ClientConfigProperties.PASSWORD.getKey(), password);
            configuration.put(ClientConfigProperties.SSL_AUTH.getKey(), Boolean.FALSE);
            configuration.remove(ClientConfigProperties.ACCESS_TOKEN.getKey());
            configuration.remove(ClientConfigProperties.BEARERTOKEN_AUTH.getKey());
            configuration.remove(ClientConfigProperties.httpHeader(HttpHeaders.AUTHORIZATION));
        }
    }

    public void setAccessToken(String accessToken) {
        synchronized (lock) {
            configuration.put(ClientConfigProperties.ACCESS_TOKEN.getKey(), accessToken);
            configuration.put(ClientConfigProperties.SSL_AUTH.getKey(), Boolean.FALSE);
            configuration.remove(ClientConfigProperties.BEARERTOKEN_AUTH.getKey());
            configuration.remove(ClientConfigProperties.USER.getKey());
            configuration.remove(ClientConfigProperties.PASSWORD.getKey());
            configuration.put(ClientConfigProperties.httpHeader(HttpHeaders.AUTHORIZATION), "Bearer " + accessToken);
        }
    }

    public static ClientMisconfigurationException validateAuthConfig(Map<String, String> configuration) {
        // check if username and password are empty. so can not initiate client?
        boolean useSslAuth = MapUtils.getFlag(configuration, ClientConfigProperties.SSL_AUTH.getKey());
        boolean hasAccessToken = configuration.containsKey(ClientConfigProperties.ACCESS_TOKEN.getKey());
        boolean hasUser = configuration.containsKey(ClientConfigProperties.USER.getKey());
        boolean hasPassword = configuration.containsKey(ClientConfigProperties.PASSWORD.getKey());
        boolean customHttpHeaders = configuration.containsKey(ClientConfigProperties.httpHeader(HttpHeaders.AUTHORIZATION));

        if (!(useSslAuth || hasAccessToken || hasUser || hasPassword || customHttpHeaders)) {
            return new ClientMisconfigurationException("Username and password (or access token or SSL authentication or pre-define Authorization header) are required");
        }

        if (useSslAuth && (hasAccessToken || hasPassword)) {
            return new ClientMisconfigurationException("Only one of password, access token or SSL authentication can be used per client.");
        }

        if (useSslAuth && !configuration.containsKey(ClientConfigProperties.SSL_CERTIFICATE.getKey())) {
            return new ClientMisconfigurationException("SSL authentication requires a client certificate");
        }

        if (configuration.containsKey(ClientConfigProperties.SSL_TRUST_STORE.getKey()) &&
                configuration.containsKey(ClientConfigProperties.SSL_CERTIFICATE.getKey())) {
            return new ClientMisconfigurationException("Trust store and certificates cannot be used together");
        }

        return null;
    }
}
