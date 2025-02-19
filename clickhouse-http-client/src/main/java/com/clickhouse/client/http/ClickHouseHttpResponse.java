package com.clickhouse.client.http;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.logging.Logger;

@Deprecated
public class ClickHouseHttpResponse {
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
                        getLongValue(map, "written_bytes"), getLongValue(map, "elapsed_ns"),
                        getLongValue(map, "result_rows"), this.queryId),
                null);

        this.format = format != null ? format : connection.config.getFormat();
        this.timeZone = timeZone != null ? timeZone : connection.config.getServerTimeZone();
    }

    public ClickHouseInputStream getInputStream() {
        return input;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }
}
