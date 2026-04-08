package com.clickhouse.examples.client_v2;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.Session;
import com.clickhouse.client.api.command.CommandResponse;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * Example class showing different ways to work with ClickHouse HTTP sessions.
 *
 * <p>HTTP sessions require that requests for the same session id reach the same ClickHouse server,
 * so use a single endpoint or sticky routing if a load balancer is involved.</p>
 */
@Slf4j
public class Sessions {
    private static final long REQUEST_TIMEOUT_SECONDS = 10;

    private final String endpoint;
    private final String user;
    private final String password;
    private final String database;

    public Sessions(String endpoint, String user, String password, String database) {
        this.endpoint = endpoint;
        this.user = user;
        this.password = password;
        this.database = database;
    }

    public static void main(String[] args) {
        ExamplesSupport.ConnectionConfig config = ExamplesSupport.loadConnectionConfig();
        if (!ExamplesSupport.isServerAlive(config)) {
            log.error("ClickHouse server is not alive");
            Runtime.getRuntime().exit(-503);
        }

        Sessions sessions = new Sessions(config.endpoint, config.user, config.password, config.database);
        sessions.clientWideSession();
        sessions.operationWideSession();
        sessions.twoSessions();
        sessions.handleSessionTimeout();

        log.info("Session examples completed");
    }

    /**
     * Configure a session on the client so every operation automatically reuses it.
     */
    public void clientWideSession() {
        Session clientSession = new Session()
                .setSessionId("client_session_" + randomSuffix())
                .setSessionTimeout(60)
                .setSessionTimezone("UTC");
        String tableName = "tmp_client_session_" + randomSuffix();

        try (Client client = newClientBuilder().use(clientSession).build()) {
            execute(client, "CREATE TEMPORARY TABLE " + tableName + " (value UInt8)");
            execute(client, "INSERT INTO " + tableName + " VALUES (1)");

            long value = querySingleLong(client, "SELECT value FROM " + tableName, null);
            log.info("Client-wide session keeps the temporary table alive across operations. value={}", value);
        } catch (Exception e) {
            log.error("Failed to demonstrate client-wide session", e);
        }
    }

    /**
     * Apply a session only to the operations that should use it.
     */
    public void operationWideSession() {
        Session session = new Session()
                .setSessionId("operation_session_" + randomSuffix())
                .setSessionTimeout(60)
                .setSessionTimezone("UTC");
        CommandSettings commandSettings = new CommandSettings().use(session);
        QuerySettings querySettings = new QuerySettings().use(session);
        String tableName = "tmp_operation_session_" + randomSuffix();

        try (Client client = newClientBuilder().build()) {
            execute(client, "CREATE TEMPORARY TABLE " + tableName + " (value UInt8)", commandSettings);
            execute(client, "INSERT INTO " + tableName + " VALUES (2)", commandSettings);

            long value = querySingleLong(client, "SELECT value FROM " + tableName, querySettings);
            log.info("Operation-wide session keeps state only for requests that use it. value={}", value);
        } catch (Exception e) {
            log.error("Failed to demonstrate operation-wide session", e);
        }
    }

    /**
     * Reuse the same client with two independent sessions.
     */
    public void twoSessions() {
        Session sessionA = new Session()
                .setSessionId("session_a_" + randomSuffix())
                .setSessionTimeout(60);
        Session sessionB = new Session()
                .setSessionId("session_b_" + randomSuffix())
                .setSessionTimeout(60);

        CommandSettings commandsA = new CommandSettings().use(sessionA);
        QuerySettings queriesA = new QuerySettings().use(sessionA);
        CommandSettings commandsB = new CommandSettings().use(sessionB);
        QuerySettings queriesB = new QuerySettings().use(sessionB);
        String tableName = "tmp_shared_name";

        try (Client client = newClientBuilder().build()) {
            execute(client, "CREATE TEMPORARY TABLE " + tableName + " (value UInt8)", commandsA);
            execute(client, "INSERT INTO " + tableName + " VALUES (10)", commandsA);

            execute(client, "CREATE TEMPORARY TABLE " + tableName + " (value UInt8)", commandsB);
            execute(client, "INSERT INTO " + tableName + " VALUES (20)", commandsB);

            long valueA = querySingleLong(client, "SELECT value FROM " + tableName, queriesA);
            long valueB = querySingleLong(client, "SELECT value FROM " + tableName, queriesB);

            log.info("Two sessions can keep isolated temporary tables with the same name. sessionA={}, sessionB={}",
                    valueA, valueB);
        } catch (Exception e) {
            log.error("Failed to demonstrate two independent sessions", e);
        }
    }

    /**
     * Show one way to detect that a session has expired on the server.
     */
    public void handleSessionTimeout() {
        Session session = new Session()
                .setSessionId("timeout_session_" + randomSuffix())
                .setSessionTimeout(3);

        try (Client client = newClientBuilder().build()) {
            try (QueryResponse response = client.query("SELECT 1", new QuerySettings().use(session))
                    .get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.info("Started session {} with queryId {}", session.getSessionId(), response.getQueryId());
            }

            long waitSeconds = session.getSessionTimeout() + 1L;
            log.info("Waiting {} seconds for session {} to expire", waitSeconds, session.getSessionId());
            TimeUnit.SECONDS.sleep(waitSeconds);

            QuerySettings sessionCheck = new QuerySettings().use(session).setSessionCheck(true);
            try (QueryResponse response = client.query("SELECT 1", sessionCheck)
                    .get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("Session {} is still active after query {}. Increase the wait time if your server rounds session_timeout.",
                        session.getSessionId(), response.getQueryId());
            } catch (ClientException e) {
                ServerException serverException = findServerException(e);
                if (serverException == null) {
                    throw e;
                }
                log.info("Session {} expired as expected: {}", session.getSessionId(), serverException.getMessage());
            }
        } catch (Exception e) {
            log.error("Failed to demonstrate session timeout handling", e);
        }
    }

    private Client.Builder newClientBuilder() {
        return new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(password)
                .setDefaultDatabase(database)
                .compressServerResponse(true);
    }

    private void execute(Client client, String sql) throws Exception {
        execute(client, sql, null);
    }

    private void execute(Client client, String sql, CommandSettings settings) throws Exception {
        try (CommandResponse response = settings == null
                ? client.execute(sql).get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                : client.execute(sql, settings).get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            log.info("Executed: {}, readBytes={}", sql, response.getReadBytes());
        }
    }

    private long querySingleLong(Client client, String sql, QuerySettings settings) throws Exception {
        return (settings == null ? client.queryAll(sql) : client.queryAll(sql, settings)).get(0).getLong(1);
    }

    private ServerException findServerException(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof ServerException) {
                return (ServerException) current;
            }
            current = current.getCause();
        }
        return null;
    }

    private String randomSuffix() {
        return Long.toHexString(System.nanoTime());
    }
}
