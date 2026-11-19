package com.clickhouse.client.cli;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@code client} backend: client-v2 over HTTP.
 *
 * <p>Every statement runs with {@link Client#query(String, Map, QuerySettings)} in the default format of the
 * client ({@code RowBinaryWithNamesAndTypes}). The values come from
 * {@link ClickHouseBinaryFormatReader#readValue(int)}, and the column types from the {@link TableSchema} of the
 * reader. The session is the client's {@code session_id}.
 *
 * <p>With {@code text} (transport only), the client sends no format header, so the server writes the text of the
 * FORMAT clause of the statement (or {@code TabSeparated}), and the shim copies the bytes. That checks only the
 * transport, not the decoding.
 */
final class ClientBackend implements Backend {

    private final Client client;
    private final Map<String, Object> params;
    private final boolean text;

    ClientBackend(CliOptions opts, int port, String sessionId, boolean text, InvocationLog log) {
        String endpoint = (opts.secure ? "https" : "http") + "://" + opts.host + ":" + port;
        log.add("endpoint", endpoint);
        this.client = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(opts.user)
                .setPassword(opts.password)
                .setDefaultDatabase(opts.database)
                .setSessionId(sessionId)
                .build();
        this.params = opts.params.isEmpty() ? null : new LinkedHashMap<>(opts.params);
        this.text = text;
    }

    @Override
    public Result execute(String sql, Map<String, String> settings, String database) throws Exception {
        QuerySettings qs = new QuerySettings().setDatabase(database);
        for (Map.Entry<String, String> e : settings.entrySet()) {
            qs.serverSetting(e.getKey(), e.getValue());
        }
        if (text) {
            // No X-ClickHouse-Format header: the FORMAT clause of the statement selects the output.
            qs.setOption(ClientConfigProperties.INPUT_OUTPUT_FORMAT.getKey(), null);
        }
        QueryResponse response = Errors.unwrap(() -> client.query(sql, params, qs).get());
        if (text) {
            return new TextResult(response);
        }
        ClickHouseBinaryFormatReader reader;
        try {
            reader = client.newBinaryFormatReader(response);
        } catch (Exception e) {
            response.close();
            throw e;
        }
        TableSchema schema = reader.getSchema();
        if (schema == null) {
            // No result set (for example an INSERT or a DDL statement).
            reader.close();
            response.close();
            return null;
        }
        return new ReaderResult(response, reader, schema);
    }

    @Override
    public void close() {
        client.close();
    }

    private static final class ReaderResult implements Result {
        private final QueryResponse response;
        private final ClickHouseBinaryFormatReader reader;
        private final List<String> names = new ArrayList<>();
        private final List<String> types = new ArrayList<>();

        ReaderResult(QueryResponse response, ClickHouseBinaryFormatReader reader, TableSchema schema) {
            this.response = response;
            this.reader = reader;
            for (ClickHouseColumn c : schema.getColumns()) {
                names.add(c.getColumnName());
                types.add(c.getOriginalTypeName());
            }
        }

        @Override
        public List<String> names() {
            return names;
        }

        @Override
        public List<String> types() {
            return types;
        }

        @Override
        public boolean next() {
            return reader.next() != null;
        }

        @Override
        public Object get(int i) {
            return reader.readValue(i + 1);
        }

        @Override
        public void close() throws Exception {
            try {
                reader.close();
            } finally {
                response.close();
            }
        }
    }

    private static final class TextResult implements Result {
        private final QueryResponse response;

        TextResult(QueryResponse response) {
            this.response = response;
        }

        @Override
        public List<String> names() {
            return new ArrayList<>();
        }

        @Override
        public List<String> types() {
            return new ArrayList<>();
        }

        @Override
        public boolean next() {
            return false;
        }

        @Override
        public Object get(int i) {
            throw new IllegalStateException("no values in the text backend");
        }

        @Override
        public byte[] rawText() throws Exception {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (InputStream in = response.getInputStream()) {
                byte[] buf = new byte[8192];
                int n;
                while ((n = in.read(buf)) != -1) {
                    out.write(buf, 0, n);
                }
            }
            return out.toByteArray();
        }

        @Override
        public void close() throws Exception {
            response.close();
        }
    }
}
