package com.clickhouse.examples.jdbc_v2.json_processors;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.data_formats.GsonJsonParserFactory;
import com.clickhouse.client.api.data_formats.JacksonJsonParserFactory;
import com.clickhouse.client.api.data_formats.JsonParserFactory;
import com.clickhouse.jdbc.Driver;
import com.clickhouse.jdbc.DriverProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Reads {@code FORMAT JSONEachRow} responses through {@code jdbc-v2} using one of the factories
 * shipped under {@link com.clickhouse.client.api.data_formats}.
 *
 * <p>The driver selects a factory from the {@link DriverProperties#JSON_PARSER_FACTORY} property:
 * its value is the fully-qualified class name of a {@link JsonParserFactory}, which the driver
 * loads reflectively via a public no-arg constructor. To customize parsing, extend the shipped
 * factory, override its protected hook ({@code createMapper()} or {@code customize(GsonBuilder)}),
 * and point the property at the subclass FQN.
 */
public class JdbcV2JsonProcessorsExample {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcV2JsonProcessorsExample.class);

    private static final String TABLE = "jdbc_v2_json_processors_example";

    private static final List<Row> SAMPLE_ROWS = List.of(
            new Row(1, "alpha", true, 1.5, "{\"city\":\"Berlin\",\"tags\":[\"a\",\"b\"]}"),
            new Row(2, "beta", false, 2.5, "{\"city\":\"Paris\",\"tags\":[\"c\"]}"),
            new Row(3, "gamma", true, 3.5, "{\"city\":\"Tokyo\",\"tags\":[]}"));

    private final String url;
    private final String user;
    private final String password;

    public JdbcV2JsonProcessorsExample(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public static void main(String[] args) throws Exception {
        // jdbc-v2 does not self-register, so standalone examples must load the driver explicitly.
        Driver.load();

        var url = System.getProperty("chUrl", "jdbc:clickhouse://localhost:8123/default");
        var user = System.getProperty("chUser", "default");
        var password = System.getProperty("chPassword", "");
        new JdbcV2JsonProcessorsExample(url, user, password).run();
    }

    public void run() throws Exception {
        recreateTable();
        loadSampleData();

        readAll("Jackson (default)", JacksonJsonParserFactory.class);
        readAll("Jackson (custom)", CustomJacksonParserFactory.class);
        readAll("Gson (default)", GsonJsonParserFactory.class);
        readAll("Gson (custom)", CustomGsonParserFactory.class);
    }

    /**
     * Reads every row through a fresh connection configured to use the given factory. The factory is
     * selected per connection and cannot be swapped on an existing one. When the factory also
     * implements {@link PayloadConverter}, the raw {@code payload} map is converted into a typed
     * {@link Payload}; otherwise the raw map is logged.
     */
    public void readAll(String label, Class<? extends JsonParserFactory> factoryClass) throws Exception {
        LOG.info("--- Reading rows with {} ({}) ---", label, factoryClass.getName());

        var props = baseProperties();
        props.setProperty(DriverProperties.JSON_PARSER_FACTORY.getKey(), factoryClass.getName());

        PayloadConverter converter = PayloadConverter.class.isAssignableFrom(factoryClass)
                ? (PayloadConverter) factoryClass.getDeclaredConstructor().newInstance()
                : null;

        var sql = "SELECT id, name, active, score, payload FROM " + TABLE + " ORDER BY id FORMAT JSONEachRow";

        try (Connection connection = DriverManager.getConnection(url, props);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {

            while (rs.next()) {
                Object rawPayload = rs.getObject("payload");
                Object payload = converter != null ? converter.toPayload(rawPayload) : rawPayload;
                LOG.info("  id={}, name={}, active={}, score={}, payload={} ({})",
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getBoolean("active"),
                        rs.getDouble("score"),
                        payload,
                        payload == null ? "null" : payload.getClass().getSimpleName());
            }
        }
    }

    public void recreateTable() throws SQLException {
        try (Connection connection = DriverManager.getConnection(url, baseProperties());
             Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + TABLE);
            statement.execute("""
                    CREATE TABLE %s (
                        id UInt32, name String, active Bool, score Float64, payload JSON
                    ) ENGINE = MergeTree ORDER BY id""".formatted(TABLE));
        }
    }

    public void loadSampleData() throws SQLException {
        var values = SAMPLE_ROWS.stream()
                .map(r -> "(%d, %s, %b, %s, %s)".formatted(
                        r.id(), sqlString(r.name()), r.active(), r.score(), sqlString(r.payload())))
                .collect(Collectors.joining(", "));
        try (Connection connection = DriverManager.getConnection(url, baseProperties());
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO " + TABLE + " (id, name, active, score, payload) VALUES " + values);
        }
    }

    private Properties baseProperties() {
        var properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty(ClientConfigProperties.serverSetting("allow_experimental_json_type"), "1");
        properties.setProperty(ClientConfigProperties.serverSetting("output_format_json_quote_64bit_integers"), "0");
        properties.setProperty(ClientConfigProperties.serverSetting("output_format_json_quote_64bit_floats"), "0");
        properties.setProperty(ClientConfigProperties.serverSetting("output_format_json_quote_decimals"), "0");
        return properties;
    }

    private static String sqlString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    // ---------------------------------------------------------------------
    // Customized factories
    // ---------------------------------------------------------------------

    /**
     * Jackson factory that tolerates unknown keys and keeps big integers/decimals exact. Reuses the
     * same mapper to convert the {@code payload} map into a typed {@link Payload}.
     *
     * <p>Must be {@code public static} with a public no-arg constructor so the driver can load it
     * reflectively from {@link DriverProperties#JSON_PARSER_FACTORY}.
     */
    public static final class CustomJacksonParserFactory extends JacksonJsonParserFactory implements PayloadConverter {

        private static final ObjectMapper MAPPER = JsonMapper.builder()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS, true)
                .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                .build();

        @Override
        protected ObjectMapper createMapper() {
            return MAPPER;
        }

        @Override
        public Payload toPayload(Object rawPayload) {
            return MAPPER.convertValue(rawPayload, Payload.class);
        }
    }

    /**
     * Gson factory that reads integer-shaped numbers as {@code Long} (the default {@code Double}
     * loses precision for large {@code Int64} values) and disables HTML escaping. Reuses the same
     * {@link Gson} to convert the {@code payload} map into a typed {@link Payload}.
     *
     * <p>Must be {@code public static} with a public no-arg constructor so the driver can load it
     * reflectively from {@link DriverProperties#JSON_PARSER_FACTORY}.
     */
    public static final class CustomGsonParserFactory extends GsonJsonParserFactory implements PayloadConverter {

        private static final Gson GSON = new GsonBuilder()
                .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
                .disableHtmlEscaping()
                .setLenient()
                .create();

        @Override
        protected void customize(GsonBuilder builder) {
            builder.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
                    .disableHtmlEscaping();
        }

        @Override
        public Payload toPayload(Object rawPayload) {
            return GSON.fromJson(GSON.toJsonTree(rawPayload), Payload.class);
        }
    }

    // ---------------------------------------------------------------------
    // Domain types
    // ---------------------------------------------------------------------

    /** Implemented by factories that can turn the raw {@code payload} map into a typed {@link Payload}. */
    public interface PayloadConverter {
        Payload toPayload(Object rawPayload);
    }

    /** A sample row to insert. */
    private record Row(int id, String name, boolean active, double score, String payload) {
    }

    /** POJO shape of the {@code payload} JSON column. */
    public record Payload(String city, List<String> tags) {
    }
}
