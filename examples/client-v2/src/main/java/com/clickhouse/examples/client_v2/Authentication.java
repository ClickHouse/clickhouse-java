package com.clickhouse.examples.client_v2;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.query.GenericRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Example showing how to update authentication settings on an existing client.
 *
 * <p>Note: the authentication <em>method</em> is fixed at client construction.
 * A client built with username/password can only update username/password at
 * runtime; a client built with an access token can only update the access token.
 * Trying to switch methods at runtime throws {@link com.clickhouse.client.api.ClientMisconfigurationException}.</p>
 *
 * <p>Supported startup properties:</p>
 * <ul>
 *     <li>{@code chEndpoint} - ClickHouse endpoint, default {@code http://localhost:8123}</li>
 *     <li>{@code chDatabase} - database name, default {@code default}</li>
 *     <li>{@code chUser} and {@code chPassword} - initial username/password credentials</li>
 *     <li>{@code chAccessToken} - initial access token, preferred over username/password when set</li>
 *     <li>{@code chNextUser} and {@code chNextPassword} - replacement username/password credentials (only if the client was built with username/password)</li>
 *     <li>{@code chNextAccessToken} - replacement access token (only if the client was built with an access token)</li>
 * </ul>
 */
@Slf4j
public class Authentication {

    public static void main(String[] args) {
        final String endpoint = System.getProperty("chEndpoint", "http://localhost:8123");
        final String database = System.getProperty("chDatabase", "default");

        final String initialUser = System.getProperty("chUser", "default");
        final String initialPassword = System.getProperty("chPassword", "");
        final String initialAccessToken = trimToNull(System.getProperty("chAccessToken"));

        final String nextUser = trimToNull(System.getProperty("chNextUser"));
        final String nextPassword = trimToNull(System.getProperty("chNextPassword"));
        final String nextAccessToken = trimToNull(System.getProperty("chNextAccessToken"));

        Client.Builder builder = new Client.Builder()
                .addEndpoint(endpoint)
                .setDefaultDatabase(database)
                .compressServerResponse(true);

        configureInitialAuthentication(builder, initialUser, initialPassword, initialAccessToken);

        try (Client client = builder.build()) {
            printCurrentUser(client, "Before authentication update");

            if (!updateAuthentication(client, initialUser, initialPassword, initialAccessToken,
                    nextUser, nextPassword, nextAccessToken)) {
                log.info("No replacement credentials were applied. Set chNextAccessToken or chNextUser/chNextPassword (matching the initial auth type) to try a runtime authentication update.");
                return;
            }

            printCurrentUser(client, "After authentication update");
        } catch (Exception e) {
            log.error("Authentication example failed", e);
        }
    }

    private static void configureInitialAuthentication(Client.Builder builder, String user, String password, String accessToken) {
        if (accessToken != null) {
            authenticateWithAccessToken(builder, accessToken);
        } else {
            authenticateWithCredentials(builder, user, password);
        }
    }

    private static boolean updateAuthentication(Client client, String initialUser, String initialPassword,
                                                String initialAccessToken,
                                                String nextUser, String nextPassword, String nextAccessToken) {
        if (nextAccessToken != null) {
            if (initialAccessToken == null) {
                log.warn("Skipping access-token update: client was built with username/password and the auth type cannot be switched at runtime.");
                return false;
            }
            authenticateWithAccessToken(client, nextAccessToken);
            return true;
        }

        if (nextUser != null || nextPassword != null) {
            if (initialAccessToken != null) {
                log.warn("Skipping username/password update: client was built with an access token and the auth type cannot be switched at runtime.");
                return false;
            }
            authenticateWithCredentials(
                    client,
                    nextUser != null ? nextUser : initialUser,
                    nextPassword != null ? nextPassword : initialPassword);
            return true;
        }

        return false;
    }

    private static void authenticateWithCredentials(Client.Builder builder, String user, String password) {
        builder.setOption(ClientConfigProperties.USER.getKey(), user); // user
        builder.setOption(ClientConfigProperties.PASSWORD.getKey(), password); // password
        log.info("Configured builder with username/password authentication");
    }

    private static void authenticateWithAccessToken(Client.Builder builder, String accessToken) {
        builder.setOption(ClientConfigProperties.ACCESS_TOKEN.getKey(), accessToken); // access_token
        log.info("Configured builder with access token authentication");
    }

    private static void authenticateWithCredentials(Client client, String user, String password) {
        client.updateUserAndPassword(user, password);
        log.info("Updated client authentication using username/password");
    }

    private static void authenticateWithAccessToken(Client client, String accessToken) {
        client.updateAccessToken(accessToken);
        log.info("Updated client authentication using access token");
    }

    private static void printCurrentUser(Client client, String stage) {
        List<GenericRecord> rows = client.queryAll("SELECT currentUser() AS user");
        log.info("{}: {}", stage, rows.get(0).getString("user"));
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }

        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
