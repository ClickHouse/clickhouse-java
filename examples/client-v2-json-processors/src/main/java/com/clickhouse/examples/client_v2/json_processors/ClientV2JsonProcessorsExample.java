package com.clickhouse.examples.client_v2.json_processors;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.command.CommandResponse;
import com.clickhouse.client.api.data_formats.ClickHouseTextFormatReader;
import com.clickhouse.client.api.data_formats.GsonJsonParserFactory;
import com.clickhouse.client.api.data_formats.JSONEachRowFormatReader;
import com.clickhouse.client.api.data_formats.JacksonJsonParserFactory;
import com.clickhouse.client.api.data_formats.JsonParserFactory;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Demonstrates how to consume a {@code JSONEachRow} response with the client-v2
 * {@link JSONEachRowFormatReader} and a {@link JsonParserFactory}.
 *
 * <p>The class is intentionally written as a regular component (instance methods,
 * shared {@link Client} field) so it can be copied as-is into other projects and
 * have its individual methods invoked.</p>
 *
 * <p>Two factories ship with the client and serve as the customization points:
 * {@link com.clickhouse.client.api.data_formats.JacksonJsonParserFactory} and {@link com.clickhouse.client.api.data_formats.GsonJsonParserFactory}. Extend
 * either of them and override the protected hook
 * ({@code createMapper()} for Jackson, {@code customize(GsonBuilder)} for Gson)
 * to plug in any library-level customization.</p>
 */
public class ClientV2JsonProcessorsExample {

    private static final Logger LOG = LoggerFactory.getLogger(ClientV2JsonProcessorsExample.class);

    private static final String TABLE = "client_v2_json_processors_example";

    /**
     * Sample dataset: {@code { id, name, active, score, payload }}.
     */
    private static final Object[][] SAMPLE_ROWS = {
            {1, "alpha", true, 1.5, "{\"city\":\"Berlin\",\"tags\":[\"a\",\"b\"]}"},
            {2, "beta", false, 2.5, "{\"city\":\"Paris\", \"tags\":[\"c\"]}"},
            {3, "gamma", true, 3.5, "{\"city\":\"Tokyo\", \"tags\":[]}"},
    };

    private final Client client;

    public ClientV2JsonProcessorsExample(Client client) {
        this.client = client;
    }

    public static void main(String[] args) throws Exception {
        try (Client client = buildClient()) {
            new ClientV2JsonProcessorsExample(client).run();
        }
    }

    /**
     * Runs the full demo: prepares the table, loads sample rows, reads them four times.
     */
    public void run() throws Exception {
        recreateTable();
        loadSampleData();

        // 1. Default Jackson: use the shipped factory as-is.
        readAll("Jackson (default)", new JacksonJsonParserFactory());

        // 2. Customized Jackson: extend the factory and override createMapper().
        readAll("Jackson (custom)", new CustomJacksonParserFactory());

        // 3. Default Gson: use the shipped factory as-is.
        readAll("Gson (default)", new GsonJsonParserFactory());

        // 4. Customized Gson: extend the factory and override customize(GsonBuilder).
        readAll("Gson (custom)", new CustomGsonParserFactory());
    }

    /**
     * Reads every row from {@link #TABLE} using a {@code JSONEachRow} stream
     * decoded with the supplied {@link JsonParserFactory}.
     *
     * <p>When the factory also implements {@link PayloadConverter} (as the two
     * custom factories below do), the raw {@code payload} value is fed through
     * {@link PayloadConverter#toPayload(Object)} so the row is logged with a
     * typed {@link Payload} POJO instead of the bare {@code Map<String, Object>}
     * produced by the underlying library.</p>
     */
    public void readAll(String label, JsonParserFactory factory) throws Exception {
        LOG.info("--- Reading rows with {} ---", label);

        QuerySettings settings = new QuerySettings()
                .setFormat(ClickHouseFormat.JSONEachRow)
                .setOption(ClientConfigProperties.JSON_DISABLE_NUMBER_QUOTING.getKey(), true);
        String sql = "SELECT id, name, active, score, payload FROM " + TABLE + " ORDER BY id";

        PayloadConverter converter = factory instanceof PayloadConverter ? (PayloadConverter) factory : null;

        try (QueryResponse response = client.query(sql, settings).get();
             ClickHouseTextFormatReader reader = new JSONEachRowFormatReader(
                     factory.createJsonParser(response.getInputStream()))) {

            while (reader.next() != null) {
                Object rawPayload = reader.readValue("payload");
                Object payload = converter != null ? converter.toPayload(rawPayload) : rawPayload;
                LOG.info("  id={}, name={}, active={}, score={}, payload={} ({})",
                        reader.getInteger("id"),
                        reader.getString("name"),
                        reader.getBoolean("active"),
                        reader.getDouble("score"),
                        payload,
                        payload == null ? "null" : payload.getClass().getSimpleName());
            }
        }
    }

    public void recreateTable() throws Exception {
        execute("DROP TABLE IF EXISTS " + TABLE);
        execute("CREATE TABLE " + TABLE + " ("
                + "id UInt32, name String, active Bool, score Float64, payload JSON"
                + ") ENGINE = MergeTree ORDER BY id");
    }

    /**
     * Inserts {@link #SAMPLE_ROWS} into {@link #TABLE} as a single batched INSERT.
     */
    public void loadSampleData() throws Exception {
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
        execute(sql.toString());
    }

    private void execute(String sql) throws Exception {
        try (CommandResponse ignored = client.execute(sql).get()) {
            LOG.debug("Executed: {}", sql);
        }
    }

    private static Client buildClient() {
        return new Client.Builder()
                .addEndpoint(System.getProperty("chEndpoint", "http://localhost:8123"))
                .setUsername(System.getProperty("chUser", "default"))
                .setPassword(System.getProperty("chPassword", ""))
                .setDefaultDatabase(System.getProperty("chDatabase", "default"))
                .serverSetting("allow_experimental_json_type", "1")
                .build();
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
     * applies {@code setLenient()} on its own afterward (which is required for
     * the stream-of-objects shape of {@code JSONEachRow}).
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
     * default factories do not implement it, so {@link #readAll(String, JsonParserFactory)}
     * logs the raw map for them.
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
