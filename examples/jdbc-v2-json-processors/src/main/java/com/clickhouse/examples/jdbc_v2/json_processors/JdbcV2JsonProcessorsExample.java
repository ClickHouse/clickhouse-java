package com.clickhouse.examples.jdbc_v2.json_processors;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.jdbc.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class JdbcV2JsonProcessorsExample {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcV2JsonProcessorsExample.class);
    private static final String TABLE_NAME = "jdbc_v2_json_processors_example";
    private static final String SAMPLE_DATA_RESOURCE = "/sample_data.csv";
    private static final String CREATE_TABLE_SQL = "CREATE TABLE " + TABLE_NAME + " ("
            + "id UInt32, "
            + "name String, "
            + "active Bool, "
            + "score Float64, "
            + "payload JSON"
            + ") ENGINE = MergeTree ORDER BY id";
    private static final String SELECT_DATA_SQL = "SELECT id, name, active, score, payload "
            + "FROM " + TABLE_NAME + " ORDER BY id FORMAT JSONEachRow";

    public static void main(String[] args) throws Exception {
        String url = System.getProperty("chUrl", "jdbc:clickhouse://localhost:8123/default");
        String user = System.getProperty("chUser", "default");
        String password = System.getProperty("chPassword", "");
        Properties setupProperties = baseProperties(user, password);

        registerDriver();
        defineTableStructure(url, setupProperties);
        loadData(url, setupProperties);

        runGsonExample(url, user, password);
        runJacksonExample(url, user, password);
    }

    private static void defineTableStructure(String url, Properties properties) throws Exception {
        LOG.info("Step 1. Defining table structure: {}", TABLE_NAME);
        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            statement.execute(CREATE_TABLE_SQL);
        }
    }

    private static void loadData(String url, Properties properties) throws Exception {
        List<SampleRow> rows = readSampleRows();
        LOG.info("Step 2. Loading {} sample rows from {} into {}", rows.size(), SAMPLE_DATA_RESOURCE, TABLE_NAME);
        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE " + TABLE_NAME);
            statement.executeUpdate(buildInsertSql(rows));
        }
    }

    private static void runJacksonExample(String url, String user, String password) throws Exception {
        Properties properties = baseProperties(user, password);
        properties.setProperty(ClientConfigProperties.JSON_PROCESSOR.getKey(), "JACKSON");
        LOG.info("Step 4. Running jdbc-v2 example with Jackson");
        readRows(url, properties, "JACKSON");
    }

    private static void runGsonExample(String url, String user, String password) throws Exception {
        Properties properties = baseProperties(user, password);
        properties.setProperty(ClientConfigProperties.JSON_PROCESSOR.getKey(), "GSON");
        LOG.info("Step 3. Running jdbc-v2 example with Gson");
        readRows(url, properties, "GSON");
    }

    private static void readRows(String url, Properties properties, String processor) throws Exception {
        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(SELECT_DATA_SQL)) {
            while (rs.next()) {
                Object payload = rs.getObject("payload");
                LOG.info("[{}] id={}, name={}, active={}, score={}, payload={}",
                        processor,
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getBoolean("active"),
                        rs.getDouble("score"),
                        payload);
            }
        }
    }

    private static Properties baseProperties(String user, String password) {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty(ClientConfigProperties.serverSetting("allow_experimental_json_type"), "1");
        return properties;
    }

    private static void registerDriver() {
        // `jdbc-v2` does not self-register from its static initializer, so standalone
        // examples should register it explicitly before calling DriverManager.
        Driver.load();
    }

    private static List<SampleRow> readSampleRows() throws IOException {
        InputStream stream = JdbcV2JsonProcessorsExample.class.getResourceAsStream(SAMPLE_DATA_RESOURCE);
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
}
