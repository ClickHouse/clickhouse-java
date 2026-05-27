package com.clickhouse.examples.jdbc;

import com.clickhouse.client.api.ClientConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Example showing how to use different authentication settings with JDBC.
 *
 * <p>Unlike the client-v2 example, JDBC normally works by creating a new connection with
 * updated properties instead of mutating an existing connection.</p>
 *
 * <p>Supported startup properties:</p>
 * <ul>
 *     <li>{@code chUrl} - ClickHouse JDBC URL, default {@code jdbc:clickhouse://localhost:8123/default}</li>
 *     <li>{@code chUser} and {@code chPassword} - initial username/password credentials</li>
 *     <li>{@code chAccessToken} - initial access token</li>
 *     <li>{@code chNextUser} and {@code chNextPassword} - replacement username/password credentials</li>
 *     <li>{@code chNextAccessToken} - replacement access token</li>
 * </ul>
 */
public class Authentication {
    private static final Logger log = LoggerFactory.getLogger(Authentication.class);

    public static void main(String[] args) {
        final String url = System.getProperty("chUrl", "jdbc:clickhouse://localhost:8123/default");

        final String initialUser = System.getProperty("chUser", "default");
        final String initialPassword = System.getProperty("chPassword", "");
        final String initialAccessToken = trimToNull(System.getProperty("chAccessToken"));

        final String nextUser = trimToNull(System.getProperty("chNextUser"));
        final String nextPassword = trimToNull(System.getProperty("chNextPassword"));
        final String nextAccessToken = trimToNull(System.getProperty("chNextAccessToken"));

        try {
            if (initialAccessToken != null) {
                authenticateWithAccessToken(url, initialAccessToken, "Initial connection");
            } else {
                authenticateWithCredentials(url, initialUser, initialPassword, "Initial connection");
            }

            if (nextAccessToken != null) {
                authenticateWithAccessToken(url, nextAccessToken, "Connection after auth update");
            } else if (nextUser != null || nextPassword != null) {
                authenticateWithCredentials(
                        url,
                        nextUser != null ? nextUser : initialUser,
                        nextPassword != null ? nextPassword : initialPassword,
                        "Connection after auth update");
            } else {
                log.info("No replacement credentials were provided. Set chNextAccessToken or chNextUser/chNextPassword to try authentication update with a new JDBC connection.");
            }
        } catch (SQLException e) {
            log.error("JDBC authentication example failed", e);
        }
    }

    private static void authenticateWithCredentials(String url, String user, String password, String stage) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(ClientConfigProperties.USER.getKey(), user); // user
        properties.setProperty(ClientConfigProperties.PASSWORD.getKey(), password); // password

        try (Connection connection = DriverManager.getConnection(url, properties)) {
            printCurrentUser(connection, stage + " using username/password");
        }
    }

    private static void authenticateWithAccessToken(String url, String accessToken, String stage) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(ClientConfigProperties.ACCESS_TOKEN.getKey(), accessToken); // access_token

        try (Connection connection = DriverManager.getConnection(url, properties)) {
            printCurrentUser(connection, stage + " using access token");
        }
    }

    private static void printCurrentUser(Connection connection, String stage) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT currentUser()")) {
            if (rs.next()) {
                log.info("{}: {}", stage, rs.getString(1));
            }
        }
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }

        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
