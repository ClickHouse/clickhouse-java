package com.clickhouse.examples.client_v2.json_processors;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.command.CommandResponse;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ClientV2JsonProcessorsExample {
    private static final Logger LOG = LoggerFactory.getLogger(ClientV2JsonProcessorsExample.class);
    private static final String TABLE_NAME = "client_v2_json_processors_example";
    private static final String CREATE_TABLE_SQL = "CREATE TABLE " + TABLE_NAME + " ("
            + "id UInt32, "
            + "name String, "
            + "active Bool, "
            + "score Float64, "
            + "payload JSON"
            + ") ENGINE = MergeTree ORDER BY id";
    private static final String INSERT_DATA_SQL = "INSERT INTO " + TABLE_NAME + " (id, name, active, score, payload) VALUES "
            + "(1, 'first row', true, 10.5, '{\"source\":\"examples\",\"tags\":[\"demo\",\"shared\"],\"metrics\":{\"rank\":1,\"weight\":10.5}}'), "
            + "(2, 'second row', false, 20.25, '{\"source\":\"examples\",\"tags\":[\"demo\",\"shared\"],\"metrics\":{\"rank\":2,\"weight\":20.25}}')";
    private static final String SELECT_DATA_SQL = "SELECT id, name, active, score, payload "
            + "FROM " + TABLE_NAME + " ORDER BY id";

    public static void main(String[] args) throws Exception {
        ConnectionConfig config = ConnectionConfig.load();
        defineTableStructure(config);
        loadData(config);

        runGsonExample(config);
        runJacksonExample(config);
    }

    private static void defineTableStructure(ConnectionConfig config) throws Exception {
        LOG.info("Step 1. Defining table structure: {}", TABLE_NAME);
        try (Client client = createClient(config, "GSON")) {
            executeStatement(client, "DROP TABLE IF EXISTS " + TABLE_NAME);
            executeStatement(client, CREATE_TABLE_SQL);
        }
    }

    private static void loadData(ConnectionConfig config) throws Exception {
        LOG.info("Step 2. Loading sample data into {}", TABLE_NAME);
        try (Client client = createClient(config, "GSON")) {
            executeStatement(client, "TRUNCATE TABLE " + TABLE_NAME);
            executeStatement(client, INSERT_DATA_SQL);
        }
    }

    private static void runJacksonExample(ConnectionConfig config) throws Exception {
        LOG.info("Step 4. Running client-v2 example with Jackson");
        try (Client client = createClient(config, "JACKSON")) {
            readRows(client, "JACKSON");
        }
    }

    private static void runGsonExample(ConnectionConfig config) throws Exception {
        LOG.info("Step 3. Running client-v2 example with Gson");
        try (Client client = createClient(config, "GSON")) {
            readRows(client, "GSON");
        }
    }

    private static void readRows(Client client, String processor) throws Exception {
        try (QueryResponse response = client.query(SELECT_DATA_SQL, new QuerySettings().setFormat(ClickHouseFormat.JSONEachRow)).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            while (reader.next() != null) {
                Map<String, Object> payload = reader.readValue("payload");
                LOG.info("[{}] id={}, name={}, active={}, score={}, payload={}",
                        processor,
                        reader.getInteger("id"),
                        reader.getString("name"),
                        reader.getBoolean("active"),
                        reader.getDouble("score"),
                        payload);
            }
        }
    }

    private static Client createClient(ConnectionConfig config, String processor) {
        return new Client.Builder()
                .addEndpoint(config.endpoint)
                .setUsername(config.user)
                .setPassword(config.password)
                .setDefaultDatabase(config.database)
                .serverSetting("allow_experimental_json_type", "1")
                .setOption(ClientConfigProperties.JSON_PROCESSOR.getKey(), processor)
                .build();
    }

    private static void executeStatement(Client client, String sql) throws Exception {
        try (CommandResponse ignored = client.execute(sql).get()) {
            LOG.debug("Executed SQL: {}", sql);
        }
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
