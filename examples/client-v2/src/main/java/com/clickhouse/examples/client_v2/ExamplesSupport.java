package com.clickhouse.examples.client_v2;

import com.clickhouse.client.api.Client;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class ExamplesSupport {
    private ExamplesSupport() {
    }

    static ConnectionConfig loadConnectionConfig() {
        return new ConnectionConfig(
                System.getProperty("chEndpoint", "http://localhost:8123"),
                System.getProperty("chUser", "default"),
                System.getProperty("chPassword", ""),
                System.getProperty("chDatabase", "default"));
    }

    static boolean isServerAlive(ConnectionConfig config) {
        try (Client client = new Client.Builder()
                .addEndpoint(config.endpoint)
                .setUsername(config.user)
                .setPassword(config.password)
                .setDefaultDatabase(config.database)
                .build()) {
            return client.ping();
        } catch (Exception e) {
            log.error("Failed to ping ClickHouse server", e);
            return false;
        }
    }

    static final class ConnectionConfig {
        final String endpoint;
        final String user;
        final String password;
        final String database;

        ConnectionConfig(String endpoint, String user, String password, String database) {
            this.endpoint = endpoint;
            this.user = user;
            this.password = password;
            this.database = database;
        }
    }
}
