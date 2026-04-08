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

    private String username;
    private String password;
    private String accessToken;
    private String authorizationHeader;
    private boolean useSslAuth;

    public CredentialsManager(Map<String, String> configuration) {
        validateAuthConfig(configuration);

        this.username = configuration.get(ClientConfigProperties.USER.getKey());
        this.password = configuration.get(ClientConfigProperties.PASSWORD.getKey());
        this.accessToken = readAccessToken(configuration);
        this.authorizationHeader = readAuthorizationHeader(configuration, accessToken);
        this.useSslAuth = MapUtils.getFlag(configuration, ClientConfigProperties.SSL_AUTH.getKey(), false);
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> snapshot = new HashMap<>();
        applyCredentials(snapshot);
        return snapshot;
    }

    public void applyCredentials(Map<String, Object> target) {
        putIfNotNull(target, ClientConfigProperties.USER.getKey(), username);
        putIfNotNull(target, ClientConfigProperties.PASSWORD.getKey(), password);
        putIfNotNull(target, ClientConfigProperties.ACCESS_TOKEN.getKey(), accessToken);
        putIfNotNull(target, AUTHORIZATION_HEADER_KEY, authorizationHeader);
        if (useSslAuth) {
            target.put(ClientConfigProperties.SSL_AUTH.getKey(), Boolean.TRUE);
        }
    }

    /**
     * Replaces the current username/password credentials.
     *
     * <p>This class does not synchronize credential updates. Callers must
     * serialize updates and request execution if they require thread safety.
     */
    public void setCredentials(String username, String password) {
        this.username = username;
        this.password = password;
        this.useSslAuth = false;
        this.accessToken = null;
        this.authorizationHeader = null;
    }

    /**
     * Replaces the current credentials with a bearer token.
     *
     * <p>This class does not synchronize credential updates. Callers must
     * serialize updates and request execution if they require thread safety.
     */
    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
        this.authorizationHeader = accessToken == null ? null : "Bearer " + accessToken;
        this.useSslAuth = false;
        this.username = null;
        this.password = null;
    }

    public String getUsername() {
        return username == null ? ClientConfigProperties.USER.getDefObjVal() : username;
    }

    public static void validateAuthConfig(Map<String, ?> configuration) throws ClientMisconfigurationException {
        // check if username and password are empty. so can not initiate client?
        boolean useSslAuth = MapUtils.getFlag(configuration, ClientConfigProperties.SSL_AUTH.getKey(), false);
        boolean hasAccessToken = configuration.containsKey(ClientConfigProperties.ACCESS_TOKEN.getKey());
        boolean hasUser = configuration.containsKey(ClientConfigProperties.USER.getKey());
        boolean hasPassword = configuration.containsKey(ClientConfigProperties.PASSWORD.getKey());
        boolean customHttpHeaders = configuration.containsKey(AUTHORIZATION_HEADER_KEY);

        if (!(useSslAuth || hasAccessToken || hasUser || hasPassword || customHttpHeaders)) {
            throw new ClientMisconfigurationException("Username and password (or access token or SSL authentication or pre-define Authorization header) are required");
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
    }

    private static String readAccessToken(Map<String, String> configuration) {
        Object accessToken = configuration.get(ClientConfigProperties.ACCESS_TOKEN.getKey());
        if (accessToken == null) {
            accessToken = configuration.get(ClientConfigProperties.BEARERTOKEN_AUTH.getKey());
        }
        return accessToken == null ? null : String.valueOf(accessToken);
    }

    private static String readAuthorizationHeader(Map<String, String> configuration, String accessToken) {
        Object configuredHeader = configuration.get(AUTHORIZATION_HEADER_KEY);
        if (configuredHeader != null) {
            return String.valueOf(configuredHeader);
        }
        return accessToken == null ? null : "Bearer " + accessToken;
    }

    private static void putIfNotNull(Map<String, Object> configuration, String key, Object value) {
        if (value != null) {
            configuration.put(key, value);
        }
    }
}
