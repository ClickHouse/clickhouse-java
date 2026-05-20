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

/**
 * Demonstrates how to consume {@code FORMAT JSONEachRow} responses through
 * {@code jdbc-v2} with the two factories shipped under
 * {@link com.clickhouse.client.api.data_formats}.
 *
 * <p>The class is intentionally written as a regular component (instance
 * methods, shared connection settings as fields) so it can be copied as-is
 * into other projects and have its individual methods invoked.</p>
 *
 * <p>The JDBC driver selects a parser factory from the
 * {@link DriverProperties#JSON_PARSER_FACTORY} driver property — its value is
 * the <em>fully-qualified class name</em> of a {@link JsonParserFactory}
 * implementation, which the driver loads reflectively and instantiates via
 * a public no-arg constructor. There is no enum-style selector.</p>
 *
 * <p>For Jackson/Gson customization the recommended approach is therefore to
 * extend the shipped factory and override its protected hook
 * ({@code createMapper()} or {@code customize(GsonBuilder)}), then point
 * {@code JSON_PARSER_FACTORY} at the FQN of the subclass. See
 * {@link CustomJacksonParserFactory} and {@link CustomGsonParserFactory}
 * below.</p>
 */
public class JdbcV2JsonProcessorsExample {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcV2JsonProcessorsExample.class);

    private static final String TABLE = "jdbc_v2_json_processors_example";

    /**
     * Sample dataset: {@code { id, name, active, score, payload }}.
     */
    private static final Object[][] SAMPLE_ROWS = {
            {1, "alpha", true, 1.5, "{\"city\":\"Berlin\",\"tags\":[\"a\",\"b\"]}"},
            {2, "beta", false, 2.5, "{\"city\":\"Paris\", \"tags\":[\"c\"]}"},
            {3, "gamma", true, 3.5, "{\"city\":\"Tokyo\", \"tags\":[]}"},
    };

    private final String url;
    private final String user;
    private final String password;

    public JdbcV2JsonProcessorsExample(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public static void main(String[] args) throws Exception {
        // jdbc-v2 does not self-register from a static initializer; standalone
        // examples must register the driver explicitly before calling DriverManager.
        Driver.load();

        String url = System.getProperty("chUrl", "jdbc:clickhouse://localhost:8123/default");
        String user = System.getProperty("chUser", "default");
        String password = System.getProperty("chPassword", "");
        new JdbcV2JsonProcessorsExample(url, user, password).run();
    }

    /**
     * Runs the full demo: prepares the table, loads sample rows, reads them four times.
     */
    public void run() throws Exception {
        recreateTable();
        loadSampleData();

        // 1. Default Jackson: factory FQN points to the shipped class.
        readAll("Jackson (default)", JacksonJsonParserFactory.class);

        // 2. Customized Jackson: factory FQN points to a subclass of
        //    JacksonJsonParserFactory whose no-arg constructor overrides createMapper().
        readAll("Jackson (custom)", CustomJacksonParserFactory.class);

        // 3. Default Gson.
        readAll("Gson (default)", GsonJsonParserFactory.class);

        // 4. Customized Gson: subclass overriding customize(GsonBuilder).
        readAll("Gson (custom)", CustomGsonParserFactory.class);
    }

    /**
     * Reads every row from {@link #TABLE} through a fresh JDBC connection
     * configured to use the supplied {@link JsonParserFactory} implementation.
     *
     * <p>Selection is connection-level: the factory cannot be swapped on an
     * existing connection. The driver instantiates the named class once during
     * connection creation and reuses that instance for every {@code JSONEachRow}
     * response served by the connection.</p>
     *
     * <p>When {@code factoryClass} also implements {@link PayloadConverter}
     * (as the two custom factories below do), the method also instantiates a
     * converter — through the same public no-arg constructor the driver uses —
     * and feeds {@code rs.getObject("payload")} through
     * {@link PayloadConverter#toPayload(Object)}, so the row is logged with a
     * typed {@link Payload} POJO instead of the bare {@code Map<String, Object>}
     * produced by the underlying library.</p>
     */
    public void readAll(String label, Class<? extends JsonParserFactory> factoryClass) throws Exception {
        LOG.info("--- Reading rows with {} ({}) ---", label, factoryClass.getName());

        Properties props = baseProperties();
        props.setProperty(DriverProperties.JSON_PARSER_FACTORY.getKey(), factoryClass.getName());

        PayloadConverter converter = PayloadConverter.class.isAssignableFrom(factoryClass)
                ? (PayloadConverter) factoryClass.getDeclaredConstructor().newInstance()
                : null;

        String sql = "SELECT id, name, active, score, payload FROM " + TABLE
                + " ORDER BY id FORMAT JSONEachRow";

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
            statement.execute("CREATE TABLE " + TABLE + " ("
                    + "id UInt32, name String, active Bool, score Float64, payload JSON"
                    + ") ENGINE = MergeTree ORDER BY id");
        }
    }

    /**
     * Inserts {@link #SAMPLE_ROWS} into {@link #TABLE} as a single batched INSERT.
     */
    public void loadSampleData() throws SQLException {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(TABLE)
                .append(" (id, name, active, score, payload) VALUES");
        for (int i = 0; i < SAMPLE_ROWS.length; i++) {
            Object[] row = SAMPLE_ROWS[i];
            sql.append(i == 0 ? " " : ", ")
                    .append('(').append(row[0])
                    .append(", ").append(sqlString((String) row[1]))
                    .append(", ").append(row[2])
                    .append(", ").append(row[3])
                    .append(", ").append(sqlString((String) row[4]))
                    .append(')');
        }
        try (Connection connection = DriverManager.getConnection(url, baseProperties());
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql.toString());
        }
    }

    private Properties baseProperties() {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty(ClientConfigProperties.serverSetting("allow_experimental_json_type"), "1");
        return properties;
    }

    private static String sqlString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    // ---------------------------------------------------------------------
    // Customized factories
    // ---------------------------------------------------------------------

    /**
     * Customized {@link JacksonJsonParserFactory}. Override {@code createMapper()}
     * to return any {@link ObjectMapper} you want — modules, feature flags,
     * deserializers, etc. all carry over to row parsing.
     *
     * <p>This class must be {@code public static} with a public no-arg
     * constructor because the JDBC driver loads it reflectively via the
     * {@code jdbc_json_parser_factor} driver property; the {@code .getName()}
     * of a nested class is the {@code Outer$Inner} binary form, which
     * {@code Class.forName(...)} accepts.</p>
     *
     * <p>This example tolerates new server-side keys and preserves big integers
     * and decimals exactly inside the {@code payload} JSON column. It also
     * implements {@link PayloadConverter} so the same configured mapper is
     * reused to convert the row's {@code payload} {@code Map} into a typed
     * {@link Payload} POJO via {@link ObjectMapper#convertValue(Object, Class)}.</p>
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
     * Customized {@link GsonJsonParserFactory}. Override
     * {@code customize(GsonBuilder)} and configure the builder; the factory
     * applies {@code setLenient()} on its own afterwards (which is required for
     * the stream-of-objects shape of {@code JSONEachRow}).
     *
     * <p>This class must be {@code public static} with a public no-arg
     * constructor because the JDBC driver loads it reflectively via the
     * {@code jdbc_json_parser_factor} driver property; the {@code .getName()}
     * of a nested class is the {@code Outer$Inner} binary form, which
     * {@code Class.forName(...)} accepts.</p>
     *
     * <p>This example parses integer-shaped JSON numbers as {@code Long} (the
     * default is {@code Double}, which loses precision for large {@code Int64}
     * values) and disables HTML escaping on round-trips. It also implements
     * {@link PayloadConverter} so the same configured {@link Gson} is reused
     * to convert the row's {@code payload} {@code Map} into a typed
     * {@link Payload} POJO via {@code fromJson(toJsonTree(...))}.</p>
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
    // Domain types used to demonstrate POJO materialization of payload
    // ---------------------------------------------------------------------

    /**
     * Optional hook implemented by customized factories that know how to turn
     * the raw {@code payload} value (a {@code Map<String, Object>} produced by
     * the underlying JSON library) into a typed {@link Payload} POJO. The
     * default factories do not implement it, so
     * {@link #readAll(String, Class)} logs the raw map for them.
     *
     * <p>The interface is invoked from application code, not from the JDBC
     * driver itself: {@code readAll(...)} detects it on the factory class and
     * instantiates its own converter through the same public no-arg constructor
     * the driver uses for row parsing.</p>
     */
    public interface PayloadConverter {
        Payload toPayload(Object rawPayload);
    }

    /** POJO shape of the {@code payload} JSON column used by the sample data. */
    public static final class Payload {

        private String city;
        private List<String> tags;

        public Payload() {
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public List<String> getTags() {
            return tags;
        }

        public void setTags(List<String> tags) {
            this.tags = tags;
        }

        @Override
        public String toString() {
            return "Payload{city='" + city + "', tags=" + tags + '}';
        }
    }
}
