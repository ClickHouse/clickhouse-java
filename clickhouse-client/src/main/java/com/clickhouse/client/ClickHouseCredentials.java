package com.clickhouse.client;

import java.io.Serializable;
import java.util.Objects;

import com.clickhouse.data.ClickHouseChecker;

/**
 * This encapsulates access token, certificate or user name password combination
 * for accessing ClickHouse.
 */
public class ClickHouseCredentials implements Serializable {
    private static final long serialVersionUID = -8883041793709590486L;

    private final String accessToken;

    private final String userName;
    private final String password;
    // TODO sslCert
    private final boolean gssEnabled;

    /**
     * Create credentials from access token.
     *
     * @param accessToken access token
     * @return credentials object for authentication
     */
    public static ClickHouseCredentials fromAccessToken(String accessToken) {
        return new ClickHouseCredentials(null, null, accessToken, false);
    }

    /**
     * Create credentials from user name and password.
     *
     * @param userName user name
     * @param password password
     * @return credentials object for authentication
     */
    public static ClickHouseCredentials fromUserAndPassword(String userName, String password) {
        return ClickHouseCredentials.fromUserAndPassword(userName, password, false);
    }

    public static ClickHouseCredentials fromUserAndPassword(String userName, String password, boolean useGss) {
        ClickHouseChecker.nonBlank(userName, "userName");
        return new ClickHouseCredentials(userName, password != null ? password : "", null, useGss);
    }

    /**
     * Create credentials for GSS authentication.
     * 
     * @param userName user name
     * @return credentials object for authentication
     */
    public static ClickHouseCredentials withGss(String userName) {
        ClickHouseChecker.nonBlank(userName, "userName");
        return new ClickHouseCredentials(userName, null, null, true);
    }

    private ClickHouseCredentials(String userName, String password, String accessToken, boolean gssEnabled) {
        this.userName = userName;
        this.password = password;
        this.accessToken = accessToken;
        this.gssEnabled = gssEnabled;
    }

    public boolean useAccessToken() {
        return accessToken != null;
    }

    public boolean isGssEnabled() {
        return gssEnabled;
    }

    /**
     * Get access token.
     *
     * @return access token
     */
    public String getAccessToken() {
        if (isGssEnabled()) {
            throw new IllegalStateException("No access token specified, please use GSS authentication instead.");
        } 
        if (!useAccessToken()) {
            throw new IllegalStateException("No access token specified, please use user name and password instead.");
        }
        return this.accessToken;
    }

    /**
     * Get user name.
     *
     * @return user name
     */
    public String getUserName() {
        if (useAccessToken()) {
            throw new IllegalStateException("No user name and password specified, please use access token instead.");
        }
        return this.userName;
    }

    /**
     * Get password.
     *
     * @return password
     */
    public String getPassword() {
        if (useAccessToken()) {
            throw new IllegalStateException("No user name and password specified, please use access token instead.");
        }
        if (isGssEnabled()) {
            throw new IllegalStateException("No password specified. Use GSS authentication instead.");
        }
        return this.password;
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessToken, userName, password, gssEnabled);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }

        if (this == obj) {
            return true;
        }

        ClickHouseCredentials c = (ClickHouseCredentials) obj;
        return Objects.equals(accessToken, c.accessToken)  && Objects.equals(userName, c.userName)
                && Objects.equals(password, c.password) && Objects.equals(gssEnabled, c.gssEnabled);
    }
}
