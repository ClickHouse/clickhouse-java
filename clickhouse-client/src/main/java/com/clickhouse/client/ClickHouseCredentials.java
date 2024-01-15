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

    private final String userName;
    private final String password;
    // TODO sslCert

    private final boolean useGss;
    private String accessToken;

    /**
     * Create credentials from access token.
     *
     * @param accessToken access token
     * @return credentials object for authentication
     */
    public static ClickHouseCredentials fromAccessToken(String accessToken) {
        return new ClickHouseCredentials(accessToken);
    }

    /**
     * Create credentials from user name and password.
     *
     * @param userName user name
     * @param password password
     * @return credentials object for authentication
     */
    public static ClickHouseCredentials fromUserAndPassword(String userName, String password) {
        return new ClickHouseCredentials(userName, password);
    }

    public static ClickHouseCredentials fromGss() {
        return new ClickHouseCredentials();
    }

    /**
     * Construct credentials object using access token.
     *
     * @param accessToken access token
     */
    protected ClickHouseCredentials(String accessToken) {
        this.accessToken = ClickHouseChecker.nonNull(accessToken, "accessToken");
        this.useGss = false;
        this.userName = null;
        this.password = null;
    }

    /**
     * Construct credentials using user name and password.
     *
     * @param userName user name
     * @param password password
     */
    protected ClickHouseCredentials(String userName, String password) {
        this.accessToken = null;

        this.userName = ClickHouseChecker.nonBlank(userName, "userName");
        this.password = password != null ? password : "";
        this.useGss = false;
    }

    protected ClickHouseCredentials() {
        this.useGss = true;
        this.userName = null;
        this.password = null;
        this.accessToken = null;
    }

    public boolean useAccessToken() {
        return accessToken != null;
    }

    public boolean useGss() {
        return useGss;
    }

    /**
     * Get access token.
     *
     * @return access token
     */
    public String getAccessToken() {
        if (!useAccessToken()) {
            throw new IllegalStateException("No access token specified, please use user name and password instead.");
        }
        return this.accessToken;
    }

    public void setAccessToken(String token) {
        this.accessToken = token;
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
        return this.password;
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessToken, userName, password, useGss);
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
        return Objects.equals(accessToken, c.accessToken) 
                && Objects.equals(userName, c.userName)
                && Objects.equals(password, c.password)
                && Objects.equals(useGss, c.useGss);
    }
}
