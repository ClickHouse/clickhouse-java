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
import java.util.stream.Collectors;

/**
 * Reads a {@code JSONEachRow} response with the client-v2 {@link JSONEachRowFormatReader}
 * and a {@link JsonParserFactory}.
 *
 * <p>Two factories ship with the client: {@link JacksonJsonParserFactory} and
 * {@link GsonJsonParserFactory}. To customize parsing, extend either one and override its
 * protected hook ({@code createMapper()} for Jackson, {@code customize(GsonBuilder)} for Gson).
 */
public class ClientV2JsonProcessorsExample {

    private static final Logger LOG = LoggerFactory.getLogger(ClientV2JsonProcessorsExample.class);

    private static final String TABLE = "client_v2_json_processors_example";

    private static final List<Row> SAMPLE_ROWS = List.of(
            new Row(1, "alpha", true, 1.5, "{\"city\":\"Berlin\",\"tags\":[\"a\",\"b\"]}"),
            new Row(2, "beta", false, 2.5, "{\"city\":\"Paris\",\"tags\":[\"c\"]}"),
            new Row(3, "gamma", true, 3.5, "{\"city\":\"Tokyo\",\"tags\":[]}"));

    private final Client client;

    public ClientV2JsonProcessorsExample(Client client) {
        this.client = client;
    }

    public static void main(String[] args) throws Exception {
        try (Client client = buildClient()) {
            new ClientV2JsonProcessorsExample(client).run();
        }
    }

    public void run() throws Exception {
        recreateTable();
        loadSampleData();

        readAll("Jackson (default)", new JacksonJsonParserFactory());
        readAll("Jackson (custom)", new CustomJacksonParserFactory());
        readAll("Gson (default)", new GsonJsonParserFactory());
        readAll("Gson (custom)", new CustomGsonParserFactory());
    }

    /**
     * Reads every row from a {@code JSONEachRow} stream decoded with the given factory. When the
     * factory also implements {@link PayloadConverter}, the raw {@code payload} map is converted
     * into a typed {@link Payload}; otherwise the raw map is logged.
     */
    public void readAll(String label, JsonParserFactory factory) throws Exception {
        LOG.info("--- Reading rows with {} ---", label);

        var settings = new QuerySettings()
                .setFormat(ClickHouseFormat.JSONEachRow)
                .setOption(ClientConfigProperties.JSON_DISABLE_NUMBER_QUOTING.getKey(), true);
        var sql = "SELECT id, name, active, score, payload FROM " + TABLE + " ORDER BY id";

        PayloadConverter converter = factory instanceof PayloadConverter c ? c : null;

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
        execute("""
                CREATE TABLE %s (
                    id UInt32, name String, active Bool, score Float64, payload JSON
                ) ENGINE = MergeTree ORDER BY id""".formatted(TABLE));
    }

    public void loadSampleData() throws Exception {
        var values = SAMPLE_ROWS.stream()
                .map(r -> "(%d, %s, %b, %s, %s)".formatted(
                        r.id(), sqlString(r.name()), r.active(), r.score(), sqlString(r.payload())))
                .collect(Collectors.joining(", "));
        execute("INSERT INTO " + TABLE + " (id, name, active, score, payload) VALUES " + values);
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
     * Jackson factory that tolerates unknown keys and keeps big integers/decimals exact. Reuses the
     * same mapper to convert the {@code payload} map into a typed {@link Payload}.
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
