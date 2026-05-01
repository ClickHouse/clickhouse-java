package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientMisconfigurationException;
import org.apache.hc.core5.http.HttpHeaders;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages mutable authentication-related client settings.
 * This class is not thread-safe. Callers are responsible for coordinating
 * credential updates with request execution if they need stronger consistency.
 */
public class CredentialsManager {

    public static final String AUTHORIZATION_HEADER_KEY =
            ClientConfigProperties.httpHeader(HttpHeaders.AUTHORIZATION);
    public static final String AUTH_HEADER_BEARER_PREFIX = "Bearer ";

    private final Map<String, Object> authConfig = new ConcurrentHashMap<>();

    private final boolean sslAuthEnabled;

    private final boolean hasUserPassword;

    private final boolean hasAccessToken;

    private final boolean hasAuthHeader;

    public CredentialsManager(Map<String, String> config) {
        this.hasUserPassword = isUserPassword(config);
        this.hasAccessToken = isAccessToken(config);
        this.sslAuthEnabled = isSslAuth(config);
        this.hasAuthHeader = isCustomAuthHeader(config);
        final long authMethodsCount = Arrays
                .stream(new Boolean[] {hasUserPassword, hasAccessToken, sslAuthEnabled, hasAuthHeader})
                .filter(b-> b).count();

        String username = config.get(ClientConfigProperties.USER.getKey());
        if (authMethodsCount == 1 && !hasAuthHeader) {
            // Auth header handled specially
            String password = config.getOrDefault(ClientConfigProperties.PASSWORD.getKey(), "");
            boolean useSslAuth = MapUtils.getFlag(config, ClientConfigProperties.SSL_AUTH.getKey(), false);
            String accessToken = config.get(ClientConfigProperties.ACCESS_TOKEN.getKey());
            updateBackedConfig(username, password, useSslAuth, accessToken);
        } else if (authMethodsCount == 0 && ClientUtils.isNotBlank(username)) {
            // password not set - it is still user, password case if no other auth
            String password = config.getOrDefault(ClientConfigProperties.PASSWORD.getKey(), "");
            updateBackedConfig(username, password, false, null);
        } else if (authMethodsCount == 0) {
            throw new ClientMisconfigurationException(NO_AUTH_ERR_MSG);
        } else if (hasAuthHeader) {
            if (hasUserPassword && MapUtils.getFlag(config, ClientConfigProperties.HTTP_USE_BASIC_AUTH.getKey(),
                    ClientConfigProperties.HTTP_USE_BASIC_AUTH.getDefObjVal())) {
                throw new ClientMisconfigurationException(PASSWORD_AND_AUTH_HEADER_BASIC_AUTH_ERR_MSG);
            }
            String password = config.getOrDefault(ClientConfigProperties.PASSWORD.getKey(), "");
            String authHeader = config.getOrDefault(AUTHORIZATION_HEADER_KEY, "");
            updateBackedConfig(username, password, false, authHeader);
        } else {
            throw new ClientMisconfigurationException(ONLY_ONE_METHOD_ERR_MSG);
        }
    }

    public boolean isHasAccessToken() {
        return hasAccessToken;
    }

    public boolean isHasUserPassword() {
        return hasUserPassword;
    }

    public void applyCredentials(Map<String, Object> target) {
        target.putAll(authConfig);
    }

    /**
     * Replaces the current username/password credentials.
     *
     * <p>This class does not synchronize credential updates. Callers must
     * serialize updates and request execution if they require thread safety.
     */
    public void setCredentials(String username, String password) {
        updateBackedConfig(username, password, false, null);
    }

    /**
     * Replaces the current credentials with a bearer token.
     *
     * <p>This class does not synchronize credential updates. Callers must
     * serialize updates and request execution if they require thread safety.
     */
    public void setAccessToken(String accessToken) {
        updateBackedConfig(null, null, false, accessToken);
    }

    private void updateBackedConfig(String username, String password, boolean useSslAuth, String authHeader) {
        authConfig.put(ClientConfigProperties.USER.getKey(), username);
        authConfig.put(ClientConfigProperties.PASSWORD.getKey(), password);
        authConfig.put(ClientConfigProperties.SSL_AUTH.getKey(), useSslAuth);
        authConfig.put(AUTHORIZATION_HEADER_KEY, authHeader);
    }

    private static final String NO_AUTH_ERR_MSG = "Auth configuration is missing. At least one the following should be provided: " +
            "user & password, access token, custom authentication headers";

    private static final String ONLY_ONE_METHOD_ERR_MSG = "Only one of password, access token or SSL authentication can be used per client.";

    private static final String SSL_REQUIRES_CERT_ERR_MSG = "SSL authentication requires a client certificate";

    private static final String PASSWORD_AND_AUTH_HEADER_BASIC_AUTH_ERR_MSG = "When both password and authentication header is set then basic auth. should be disabled";

    private boolean isUserPassword(Map<String, ?> config) {
        String username = (String) config.get(ClientConfigProperties.USER.getKey());
        boolean hasUser = ClientUtils.isNotBlank(username);
        String password = (String) config.get(ClientConfigProperties.PASSWORD.getKey());
        return hasUser && password != null;
    }

    private boolean isSslAuth(Map<String, ?> config) {
        boolean useSslAuth = MapUtils.getFlag(config, ClientConfigProperties.SSL_AUTH.getKey(), false);
        if (useSslAuth && !config.containsKey(ClientConfigProperties.SSL_CERTIFICATE.getKey())) {
            throw new ClientMisconfigurationException(SSL_REQUIRES_CERT_ERR_MSG);
        }
        return useSslAuth;
    }

    private boolean isAccessToken(Map<String, ?> config) {
        String accessToken = (String) config.get(ClientConfigProperties.ACCESS_TOKEN.getKey());
        return ClientUtils.isNotBlank(accessToken);
    }

    private boolean isCustomAuthHeader(Map<String, ?> config) {
        String authHeader = (String) config.get(AUTHORIZATION_HEADER_KEY);
        return ClientUtils.isNotBlank(authHeader);
    }
}
