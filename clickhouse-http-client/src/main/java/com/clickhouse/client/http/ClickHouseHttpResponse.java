package com.clickhouse.client.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseOption;

public class ClickHouseHttpResponse extends InputStream {
    private static long getLongValue(Map<String, String> map, String key) {
        String value = map.get(key);
        if (value != null) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                // ignore error
            }
        }
        return 0L;
    }

    private final ClickHouseHttpConnection connection;
    private final InputStream input;

    protected final String serverDisplayName;
    protected final String queryId;
    protected final ClickHouseFormat format;
    protected final TimeZone timeZone;

    protected final ClickHouseResponseSummary summary;

    protected ClickHouseConfig getConfig(ClickHouseRequest<?> request) {
        ClickHouseConfig config = request.getConfig();
        if (format != null && format != config.getFormat()) {
            Map<ClickHouseOption, Serializable> options = new HashMap<>();
            options.putAll(config.getAllOptions());
            options.put(ClickHouseClientOption.FORMAT, format);
            config = new ClickHouseConfig(options, config.getDefaultCredentials(), config.getNodeSelector(),
                    config.getMetricRegistry());
        }
        return config;
    }

    public ClickHouseHttpResponse(ClickHouseHttpConnection connection, InputStream input) {
        if (connection == null || input == null) {
            throw new IllegalArgumentException("Non-null connection and input stream are required");
        }

        this.connection = connection;
        this.input = input;

        this.serverDisplayName = connection.getResponseHeader("X-ClickHouse-Server-Display-Name",
                connection.server.getHost());
        // queryId, format and timeZone are only available for queries
        this.queryId = connection.getResponseHeader("X-ClickHouse-Query-Id", "");
        // {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
        Map<String, String> map = (Map<String, String>) ClickHouseUtils
                .parseJson(connection.getResponseHeader("X-ClickHouse-Summary", "{}"));
        // discard those X-ClickHouse-Progress headers
        this.summary = new ClickHouseResponseSummary(
                new ClickHouseResponseSummary.Progress(getLongValue(map, "read_rows"), getLongValue(map, "read_bytes"),
                        getLongValue(map, "total_rows_to_read"), getLongValue(map, "written_rows"),
                        getLongValue(map, "written_bytes")),
                null);

        if (ClickHouseChecker.isNullOrEmpty(this.queryId)) {
            this.format = connection.config.getFormat();
            this.timeZone = connection.config.getServerTimeZone();
            // better to close input stream since there's no response to read?
            // input.close();
        } else {
            String value = connection.getResponseHeader("X-ClickHouse-Format", "");
            this.format = !ClickHouseChecker.isNullOrEmpty(value) ? ClickHouseFormat.valueOf(value)
                    : connection.config.getFormat();
            value = connection.getResponseHeader("X-ClickHouse-Timezone", "");
            this.timeZone = !ClickHouseChecker.isNullOrEmpty(value) ? TimeZone.getTimeZone(value)
                    : connection.config.getServerTimeZone();
        }
    }

    @Override
    public int read() throws IOException {
        return input.read();
    }

    @Override
    public int available() throws IOException {
        return input.available();
    }

    @Override
    public void close() throws IOException {
        IOException error = null;

        try {
            input.close();
        } catch (IOException e) {
            error = e;
        }

        if (!connection.isReusable()) {
            try {
                connection.close();
            } catch (Exception e) {
                // ignore
            }
        }

        if (error != null) {
            throw error;
        }
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return input.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return input.skip(n);
    }
}
