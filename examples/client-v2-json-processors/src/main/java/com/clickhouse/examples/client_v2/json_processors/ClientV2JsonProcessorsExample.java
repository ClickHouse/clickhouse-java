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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ClientV2JsonProcessorsExample {
    private static final Logger LOG = LoggerFactory.getLogger(ClientV2JsonProcessorsExample.class);
    private static final String TABLE_NAME = "client_v2_json_processors_example";
    private static final String SAMPLE_DATA_RESOURCE = "/sample_data.csv";
    private static final String CREATE_TABLE_SQL = "CREATE TABLE " + TABLE_NAME + " ("
            + "id UInt32, "
            + "name String, "
            + "active Bool, "
            + "score Float64, "
            + "payload JSON"
            + ") ENGINE = MergeTree ORDER BY id";
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
        List<SampleRow> rows = readSampleRows();
        LOG.info("Step 2. Loading {} sample rows from {} into {}", rows.size(), SAMPLE_DATA_RESOURCE, TABLE_NAME);
        try (Client client = createClient(config, "GSON")) {
            executeStatement(client, "TRUNCATE TABLE " + TABLE_NAME);
            executeStatement(client, buildInsertSql(rows));
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
                LOG.info("[{}] id={}, name={}, active={}, score={}, payload={}({})",
                        processor,
                        reader.getInteger("id"),
                        reader.getString("name"),
                        reader.getBoolean("active"),
                        reader.getDouble("score"),
                        payload.getClass().getName(),
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

    private static List<SampleRow> readSampleRows() throws IOException {
        InputStream stream = ClientV2JsonProcessorsExample.class.getResourceAsStream(SAMPLE_DATA_RESOURCE);
        if (stream == null) {
            throw new IOException("Resource not found: " + SAMPLE_DATA_RESOURCE);
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String header = reader.readLine();
            if (header == null) {
                throw new IOException("CSV resource is empty: " + SAMPLE_DATA_RESOURCE);
            }

            List<SampleRow> rows = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                List<String> values = parseCsvLine(line);
                if (values.size() != 5) {
                    throw new IOException("Expected 5 columns in sample CSV but found " + values.size() + ": " + line);
                }

                rows.add(new SampleRow(
                        Integer.parseInt(values.get(0)),
                        values.get(1),
                        Boolean.parseBoolean(values.get(2)),
                        Double.parseDouble(values.get(3)),
                        values.get(4)));
            }

            return rows;
        }
    }

    private static List<String> parseCsvLine(String line) {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (ch == ',' && !inQuotes) {
                values.add(current.toString());
                current.setLength(0);
            } else {
                current.append(ch);
            }
        }

        values.add(current.toString());
        return values;
    }

    private static String buildInsertSql(List<SampleRow> rows) {
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Sample CSV does not contain any rows");
        }

        StringBuilder sql = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME)
                .append(" (id, name, active, score, payload) VALUES ");

        for (SampleRow row : rows) {
            sql.append('(')
                    .append(row.id)
                    .append(", ")
                    .append(quoteSqlString(row.name))
                    .append(", ")
                    .append(row.active)
                    .append(", ")
                    .append(row.score)
                    .append(", ")
                    .append(quoteSqlString(row.payload))
                    .append("), ");
        }

        sql.setLength(sql.length() - 2);
        return sql.toString();
    }

    private static String quoteSqlString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static final class SampleRow {
        private final int id;
        private final String name;
        private final boolean active;
        private final double score;
        private final String payload;

        private SampleRow(int id, String name, boolean active, double score, String payload) {
            this.id = id;
            this.name = name;
            this.active = active;
            this.score = score;
            this.payload = payload;
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
