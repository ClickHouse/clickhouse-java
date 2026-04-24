package com.clickhouse.examples.client_v2.json_processors;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class ClientV2JsonProcessorsExample {
    private static final Logger LOG = LoggerFactory.getLogger(ClientV2JsonProcessorsExample.class);
    private static final List<String> SUPPORTED_PROCESSORS = Arrays.asList("JACKSON", "GSON");

    public static void main(String[] args) throws Exception {
        ConnectionConfig config = ConnectionConfig.load();

        for (String processor : requestedProcessors()) {
            runExample(config, processor);
        }
    }

    private static void runExample(ConnectionConfig config, String processor) throws Exception {
        LOG.info("Running client-v2 JSONEachRow example with processor {}", processor);

        try (Client client = new Client.Builder()
                .addEndpoint(config.endpoint)
                .setUsername(config.user)
                .setPassword(config.password)
                .setDefaultDatabase(config.database)
                // `json_processor` selects the parser used by `newBinaryFormatReader(...)`.
                .setOption(ClientConfigProperties.JSON_PROCESSOR.getKey(), processor)
                .build();
             QueryResponse response = client.query(
                     "SELECT number + 1 AS id, concat('processor-', toString(number + 1)) AS label FROM numbers(3)",
                     new QuerySettings().setFormat(ClickHouseFormat.JSONEachRow)).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            while (reader.hasNext()) {
                reader.next();
                LOG.info("[{}] id={}, label={}", processor, reader.getInteger("id"), reader.getString("label"));
            }
        }
    }

    private static List<String> requestedProcessors() {
        String requested = System.getProperty("jsonProcessor", "").trim();
        if (requested.isEmpty()) {
            return SUPPORTED_PROCESSORS;
        }

        String normalized = requested.toUpperCase(Locale.ROOT);
        if (!SUPPORTED_PROCESSORS.contains(normalized)) {
            throw new IllegalArgumentException("Unsupported jsonProcessor '" + requested
                    + "'. Expected one of: " + SUPPORTED_PROCESSORS);
        }

        return Collections.singletonList(normalized);
    }

    private static final class ConnectionConfig {
        private final String endpoint;
        private final String user;
        private final String password;
        private final String database;

        private ConnectionConfig(String endpoint, String user, String password, String database) {
            this.endpoint = endpoint;
            this.user = user;
            this.password = password;
            this.database = database;
        }

        private static ConnectionConfig load() {
            return new ConnectionConfig(
                    System.getProperty("chEndpoint", "http://localhost:8123"),
                    System.getProperty("chUser", "default"),
                    System.getProperty("chPassword", ""),
                    System.getProperty("chDatabase", "default"));
        }
    }
}
