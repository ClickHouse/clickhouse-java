package com.clickhouse.examples.jdbc_v2.json_processors;

import com.clickhouse.client.api.ClientConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class JdbcV2JsonProcessorsExample {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcV2JsonProcessorsExample.class);
    private static final String TABLE_NAME = "jdbc_v2_json_processors_example";
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
            + "FROM " + TABLE_NAME + " ORDER BY id FORMAT JSONEachRow";

    public static void main(String[] args) throws Exception {
        String url = System.getProperty("chUrl", "jdbc:clickhouse://localhost:8123/default");
        String user = System.getProperty("chUser", "default");
        String password = System.getProperty("chPassword", "");
        Properties setupProperties = baseProperties(user, password);

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
        LOG.info("Step 2. Loading sample data into {}", TABLE_NAME);
        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE " + TABLE_NAME);
            statement.executeUpdate(INSERT_DATA_SQL);
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
}
