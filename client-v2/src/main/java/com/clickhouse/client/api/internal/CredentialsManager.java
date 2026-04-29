package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientMisconfigurationException;
import org.apache.hc.core5.http.HttpHeaders;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages mutable authentication-related client settings.
 *
 * <p>This class is not thread-safe. Callers are responsible for coordinating
 * credential updates with request execution if they need stronger consistency.
 */
public class CredentialsManager {

    private static final String AUTHORIZATION_HEADER_KEY =
            ClientConfigProperties.httpHeader(HttpHeaders.AUTHORIZATION);
    private static final String AUTH_HEADER_BEARER_PREFIX = "Bearer ";

    private final Map<String, String> bakedConfig = new HashMap<>();

    public CredentialsManager(Map<String, String> configuration) {
        validateAndSet(configuration);
        // TODO: bake config
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> snapshot = new HashMap<>();
        applyCredentials(snapshot);
        return snapshot;
    }

    public void applyCredentials(Map<String, Object> target) {
        target.putAll(bakedConfig);
    }

    /**
     * Replaces the current username/password credentials.
     *
     * <p>This class does not synchronize credential updates. Callers must
     * serialize updates and request execution if they require thread safety.
     */
    public void setCredentials(String username, String password) {
        updateBackedConfig(username, password, false, null, null);
    }

    /**
     * Replaces the current credentials with a bearer token.
     *
     * <p>This class does not synchronize credential updates. Callers must
     * serialize updates and request execution if they require thread safety.
     */
    public void setAccessToken(String accessToken) {
        updateBackedConfig(null, null, false, accessToken, accessToken == null ? null :
                AUTH_HEADER_BEARER_PREFIX + accessToken);
    }

    private void updateBackedConfig(String username, String password, boolean useSslAuth, String accessToken, String authHeader) {
        bakedConfig.put(ClientConfigProperties.USER.getKey(), username);
        bakedConfig.put(ClientConfigProperties.PASSWORD.getKey(), password);
        bakedConfig.put(ClientConfigProperties.SSL_AUTH.getKey(), String.valueOf(useSslAuth));
        bakedConfig.put(ClientConfigProperties.ACCESS_TOKEN.getKey(), accessToken);
        bakedConfig.put(AUTHORIZATION_HEADER_KEY, authHeader);
    }

    public String getUsername() {
        String username = bakedConfig.get(ClientConfigProperties.USER.getKey());
        return username == null ? ClientConfigProperties.USER.getDefaultValue() : username;
    }

    private static final String NO_AUTH_ERR_MSG = "Auth configuration is missing. At least one the following should be provided: " +
            "user & password, access token, custom authentication headers";

    private void validateAndSet(Map<String, ?> configuration) throws ClientMisconfigurationException {
        // check if username and password are empty. so can not initiate client?
        boolean useSslAuth = MapUtils.getFlag(configuration, ClientConfigProperties.SSL_AUTH.getKey(), false);
        String accessToken = (String) configuration.get(ClientConfigProperties.ACCESS_TOKEN.getKey());
        boolean hasAccessToken = ClientUtils.isNotBlank(accessToken);
        String username = (String) configuration.get(ClientConfigProperties.USER.getKey());
        boolean hasUser = ClientUtils.isNotBlank(username);
        String password = (String) configuration.get(ClientConfigProperties.PASSWORD.getKey());
        boolean hasPassword = ClientUtils.isNotBlank(password);
        String authHeader = (String) configuration.get(AUTHORIZATION_HEADER_KEY);
        boolean customHttpHeaders = ClientUtils.isNotBlank(authHeader);

        if (!(useSslAuth || hasAccessToken || hasUser || hasPassword || customHttpHeaders)) {
            throw new ClientMisconfigurationException(NO_AUTH_ERR_MSG);
        }

        if (useSslAuth && (hasAccessToken || hasPassword)) {
            throw new ClientMisconfigurationException("Only one of password, access token or SSL authentication can be used per client.");
        }

        if (useSslAuth && !configuration.containsKey(ClientConfigProperties.SSL_CERTIFICATE.getKey())) {
            throw new ClientMisconfigurationException("SSL authentication requires a client certificate");
        }

        if (configuration.containsKey(ClientConfigProperties.SSL_TRUST_STORE.getKey()) &&
                configuration.containsKey(ClientConfigProperties.SSL_CERTIFICATE.getKey())) {
            throw new ClientMisconfigurationException("Trust store and certificates cannot be used together");
        }

        updateBackedConfig(username, password, useSslAuth, accessToken, authHeader);
    }
}
