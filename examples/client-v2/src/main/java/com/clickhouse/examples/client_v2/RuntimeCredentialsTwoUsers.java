package com.clickhouse.examples.client_v2;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Standalone demo for runtime credential updates using two created users.
 *
 * <p>Usage:</p>
 * <pre>
 *   mvn exec:java \
 *     -Dexec.mainClass="com.clickhouse.examples.client_v2.RuntimeCredentialsTwoUsers" \
 *     -Dexec.args="http://localhost:8123 [adminUser] [adminPassword]"
 * </pre>
 */
@Slf4j
public class RuntimeCredentialsTwoUsers {

    private static final String DEFAULT_ENDPOINT = "http://localhost:8123";
    private static final String DEFAULT_DATABASE = "default";
    private static final String DEFAULT_ADMIN_USER = "default";
    private static final String DEFAULT_ADMIN_PASSWORD = "";

    public static void main(String[] args) {
        String endpoint = args.length > 0 ? args[0] : DEFAULT_ENDPOINT;
        String adminUser = args.length > 1 ? args[1] : DEFAULT_ADMIN_USER;
        String adminPassword = args.length > 2 ? args[2] : DEFAULT_ADMIN_PASSWORD;
        String database = System.getProperty("chDatabase", DEFAULT_DATABASE);

        String suffix = String.valueOf(System.currentTimeMillis());
        String firstUser = "runtime_user_a_" + suffix;
        String secondUser = "runtime_user_b_" + suffix;
        String firstPassword = "pwdA_" + suffix;
        String secondPassword = "pwdB_" + suffix;

        log.info("Endpoint: {}", endpoint);
        log.info("Creating two demo users: {} and {}", firstUser, secondUser);

        try (Client adminClient = createClient(endpoint, database, adminUser, adminPassword)) {

            // Pre-create the two users
            createUser(adminClient, firstUser, firstPassword);
            createUser(adminClient, secondUser, secondPassword);

            // Create a client with the first user. (It is recommended to use non-existing users for security reasons)
            try (Client client = createClient(endpoint, database, firstUser, firstPassword)) {

                // Print the current user by executing a query `SELECT currentUser()`
                printCurrentUser(client, "Initial user");

                // Switch to the second user
                client.updateUserAndPassword(secondUser, secondPassword);
                // Print the current user by executing a query `SELECT currentUser()`
                printCurrentUser(client, "After switch to second user");

                // Switch back to the first user
                client.updateUserAndPassword(firstUser, firstPassword);
                // Print the current user by executing a query `SELECT currentUser()`
                printCurrentUser(client, "After switch back to first user");
            } finally {
                dropUser(adminClient, firstUser);
                dropUser(adminClient, secondUser);
            }
        } catch (Exception e) {
            log.error("Runtime credentials example failed. Ensure admin user can CREATE/DROP USER.", e);
            Runtime.getRuntime().exit(1);
        }
    }

    private static void createUser(Client adminClient, String user, String password) throws Exception {
        runCommand(adminClient, "CREATE USER IF NOT EXISTS " + user + " IDENTIFIED BY '" + password + "'");
        runCommand(adminClient, "GRANT SELECT ON system.one TO " + user);
    }

    private static void dropUser(Client adminClient, String user) {
        try {
            runCommand(adminClient, "DROP USER IF EXISTS " + user);
        } catch (Exception e) {
            log.warn("Failed to drop user {}", user, e);
        }
    }

    private static void runCommand(Client client, String sql) throws Exception {
        client.execute(sql).get(10, TimeUnit.SECONDS);
    }

    private static Client createClient(String endpoint, String database, String user, String password) {
        return new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(password)
                .setDefaultDatabase(database)
                .compressServerResponse(true)
                .build();
    }

    private static void printCurrentUser(Client client, String stage) {
        List<GenericRecord> rows = client.queryAll("SELECT currentUser() AS user FROM system.one");
        log.info("{}: {}", stage, rows.get(0).getString("user"));
    }
}
