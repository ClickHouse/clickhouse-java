package com.clickhouse.client.http;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseOption;

public class ClickHouseHttpResponse extends ClickHouseInputStream {
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
    private final ClickHouseInputStream input;

    protected final String serverDisplayName;
    protected final String queryId;
    protected final ClickHouseFormat format;
    protected final TimeZone timeZone;

    protected final ClickHouseResponseSummary summary;

    private boolean closed;

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

    public ClickHouseHttpResponse(ClickHouseHttpConnection connection, ClickHouseInputStream input,
            String serverDisplayName, String queryId, String summary, ClickHouseFormat format, TimeZone timeZone) {
        if (connection == null || input == null) {
            throw new IllegalArgumentException("Non-null connection and input stream are required");
        }

        this.connection = connection;
        this.input = input;

        this.serverDisplayName = !ClickHouseChecker.isNullOrEmpty(serverDisplayName) ? serverDisplayName
                : connection.server.getHost();
        this.queryId = !ClickHouseChecker.isNullOrEmpty(queryId) ? queryId : "";
        // {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
        Map<String, String> map = (Map<String, String>) ClickHouseUtils
                .parseJson(!ClickHouseChecker.isNullOrEmpty(summary) ? summary : "{}");
        // discard those X-ClickHouse-Progress headers
        this.summary = new ClickHouseResponseSummary(
                new ClickHouseResponseSummary.Progress(getLongValue(map, "read_rows"), getLongValue(map, "read_bytes"),
                        getLongValue(map, "total_rows_to_read"), getLongValue(map, "written_rows"),
                        getLongValue(map, "written_bytes")),
                null);

        this.format = format != null ? format : connection.config.getFormat();
        this.timeZone = timeZone != null ? timeZone : connection.config.getServerTimeZone();

        closed = false;
    }

    @Override
    public byte readByte() throws IOException {
        return input.readByte();
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
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws IOException {
        IOException error = null;

        try {
            input.close();
        } catch (IOException e) {
            error = e;
        }
        closed = true;

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

    @Override
    public byte[] readBytes(int length) throws IOException {
        return input.readBytes(length);
    }

    @Override
    public String readString(int byteLength, Charset charset) throws IOException {
        return input.readString(byteLength, charset);
    }
}
