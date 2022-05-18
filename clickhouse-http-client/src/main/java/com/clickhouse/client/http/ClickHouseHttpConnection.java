package com.clickhouse.client.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseCompression;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseOutputStream;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.client.http.config.ClickHouseHttpOption;

public abstract class ClickHouseHttpConnection implements AutoCloseable {
    static String urlEncode(String str, Charset charset) {
        if (charset == null) {
            charset = StandardCharsets.UTF_8;
        }

        try {
            return URLEncoder.encode(str, charset.name());
        } catch (UnsupportedEncodingException e) {
            // should not happen
            throw new IllegalArgumentException(e);
        }
    }

    private static StringBuilder appendQueryParameter(StringBuilder builder, String key, String value) {
        return builder.append(urlEncode(key, StandardCharsets.UTF_8)).append('=')
                .append(urlEncode(value, StandardCharsets.UTF_8)).append('&');
    }

    static String buildQueryParams(ClickHouseRequest<?> request) {
        if (request == null) {
            return "";
        }

        ClickHouseConfig config = request.getConfig();
        StringBuilder builder = new StringBuilder();

        // start with custom query parameters first
        Map<String, String> customParams = ClickHouseUtils
                .getKeyValuePairs((String) config.getOption(ClickHouseHttpOption.CUSTOM_PARAMS));
        for (Entry<String, String> cp : customParams.entrySet()) {
            appendQueryParameter(builder, cp.getKey(), cp.getValue());
        }

        if (config.isResponseCompressed()) {
            // request server to compress response
            appendQueryParameter(builder, "compress", "1");
        }
        if (config.isRequestCompressed()) {
            // inform server that client's request is compressed
            appendQueryParameter(builder, "decompress", "1");
        }

        Map<String, Object> settings = request.getSettings();
        List<String> stmts = request.getStatements(false);
        String settingKey = "max_execution_time";
        if (config.getMaxExecutionTime() > 0 && !settings.containsKey(settingKey)) {
            appendQueryParameter(builder, settingKey, String.valueOf(config.getMaxExecutionTime()));
        }
        settingKey = "max_result_rows";
        if (config.getMaxResultRows() > 0L && !settings.containsKey(settingKey)) {
            appendQueryParameter(builder, settingKey, String.valueOf(config.getMaxResultRows()));
            appendQueryParameter(builder, "result_overflow_mode", "break");
        }
        settingKey = "log_comment";
        if (!stmts.isEmpty() && (boolean) config.getOption(ClickHouseClientOption.LOG_LEADING_COMMENT)
                && !settings.containsKey(settingKey)) {
            String comment = ClickHouseUtils.getLeadingComment(stmts.get(0));
            if (!comment.isEmpty()) {
                appendQueryParameter(builder, settingKey, comment);
            }
        }
        settingKey = "extremes";
        if (!settings.containsKey(settingKey)) {
            appendQueryParameter(builder, settingKey, "0");
        }

        Optional<String> optionalValue = request.getSessionId();
        if (optionalValue.isPresent()) {
            appendQueryParameter(builder, "session_id", optionalValue.get());

            if (config.isSessionCheck()) {
                appendQueryParameter(builder, "session_check", "1");
            }
            if (config.getSessionTimeout() > 0) {
                // see default_session_timeout
                appendQueryParameter(builder, "session_timeout", String.valueOf(config.getSessionTimeout()));
            }
        }

        optionalValue = request.getQueryId();
        if (optionalValue.isPresent()) {
            appendQueryParameter(builder, "query_id", optionalValue.get());
        }

        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            appendQueryParameter(builder, entry.getKey(), String.valueOf(entry.getValue()));
        }

        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    static String buildUrl(ClickHouseNode server, ClickHouseRequest<?> request) {
        ClickHouseConfig config = request.getConfig();

        StringBuilder builder = new StringBuilder();
        builder.append(config.isSsl() ? "https" : "http").append("://").append(server.getHost()).append(':')
                .append(server.getPort()).append('/');
        String context = (String) config.getOption(ClickHouseHttpOption.WEB_CONTEXT);
        if (context != null && !context.isEmpty()) {
            char prev = '/';
            for (int i = 0, len = context.length(); i < len; i++) {
                char ch = context.charAt(i);
                if (ch != '/' || ch != prev) {
                    builder.append(ch);
                }
                prev = ch;
            }

            if (prev != '/') {
                builder.append('/');
            }
        }

        String query = buildQueryParams(request);
        if (!query.isEmpty()) {
            builder.append('?').append(query);
        }

        return builder.toString();
    }

    protected final ClickHouseConfig config;
    protected final ClickHouseNode server;
    protected final Map<String, String> defaultHeaders;

    protected final ClickHouseOutputStream output;

    protected final String url;

    protected ClickHouseHttpConnection(ClickHouseNode server, ClickHouseRequest<?> request) {
        if (server == null || request == null) {
            throw new IllegalArgumentException("Non-null server and request are required");
        }

        this.config = request.getConfig();
        this.server = server;

        this.output = request.getOutputStream().orElse(null);

        this.url = buildUrl(server, request);

        Map<String, String> map = new LinkedHashMap<>();
        // add customer headers
        map.putAll(ClickHouseUtils.getKeyValuePairs((String) config.getOption(ClickHouseHttpOption.CUSTOM_HEADERS)));
        map.put("Accept", "*/*");
        if (!(boolean) config.getOption(ClickHouseHttpOption.KEEP_ALIVE)) {
            map.put("Connection", "Close");
        }
        map.put("User-Agent", config.getClientName());

        ClickHouseCredentials credentials = server.getCredentials(config);
        if (credentials.useAccessToken()) {
            // TODO check if auth-scheme is available and supported
            map.put("Authorization", credentials.getAccessToken());
        } else {
            map.put("X-ClickHouse-User", credentials.getUserName());
            if (config.isSsl() && !ClickHouseChecker.isNullOrEmpty(config.getSslCert())) {
                map.put("X-ClickHouse-SSL-Certificate-Auth", "on");
            } else if (!ClickHouseChecker.isNullOrEmpty(credentials.getPassword())) {
                map.put("X-ClickHouse-Key", credentials.getPassword());
            }
        }

        String database = server.getDatabase(config);
        if (!ClickHouseChecker.isNullOrEmpty(database)) {
            map.put("X-ClickHouse-Database", database);
        }
        // Also, you can use the ‘default_format’ URL parameter
        map.put("X-ClickHouse-Format", config.getFormat().name());
        if (config.isResponseCompressed()) {
            map.put("Accept-Encoding", config.getResponseCompressAlgorithm().encoding());
        }
        if (config.isRequestCompressed()
                && config.getRequestCompressAlgorithm() != ClickHouseCompression.LZ4) {
            map.put("Content-Encoding", config.getRequestCompressAlgorithm().encoding());
        }

        this.defaultHeaders = Collections.unmodifiableMap(map);
    }

    protected void closeQuietly() {
        try {
            close();
        } catch (Exception e) {
            // ignore
        }
    }

    protected String getBaseUrl() {
        String baseUrl;
        int index = url.indexOf('?');
        if (index > 0) {
            baseUrl = url.substring(0, index);
        } else {
            baseUrl = url;
        }

        return baseUrl;
    }

    /**
     * Creates a merged map.
     *
     * @param requestHeaders request headers
     * @return
     */
    protected Map<String, String> mergeHeaders(Map<String, String> requestHeaders) {
        if (requestHeaders == null || requestHeaders.isEmpty()) {
            return defaultHeaders;
        }

        Map<String, String> merged = new LinkedHashMap<>();
        merged.putAll(defaultHeaders);
        for (Entry<String, String> header : requestHeaders.entrySet()) {
            if (header.getValue() == null) {
                merged.remove(header.getKey());
            } else {
                merged.put(header.getKey(), header.getValue());
            }
        }
        return merged;
    }

    /**
     * Posts query and data to server.
     *
     * @param query   non-blank query
     * @param data    optionally input stream for batch updating
     * @param tables  optionally external tables for query
     * @param headers optionally request headers
     * @return response
     * @throws IOException when error occured posting request and/or server failed
     *                     to respond
     */
    protected abstract ClickHouseHttpResponse post(String query, InputStream data, List<ClickHouseExternalTable> tables,
            Map<String, String> headers) throws IOException;

    /**
     * Checks whether the connection is reusable or not. This method will be called
     * in
     * {@link ClickHouseHttpClient#checkConnection(ClickHouseHttpConnection, ClickHouseNode, ClickHouseNode, ClickHouseRequest)}
     * for making a decision of whether to create a new connection. In addition to
     * that, if a connection is NOT reusable, it will be closed right after
     * corresponding ClickHouseResponse is closed.
     *
     * @return true if it's reusable; false otherwise
     */
    protected boolean isReusable() {
        return true;
    }

    /**
     * Sends a request to {@code <baseUrl>/ping} for liveness detection.
     *
     * @param timeout timeout in millisecond
     * @return true if server responded {@code Ok.}; false otherwise
     */
    public abstract boolean ping(int timeout);

    public ClickHouseHttpResponse update(String query) throws IOException {
        return post(query, null, null, null);
    }

    public ClickHouseHttpResponse update(String query, Map<String, String> headers) throws IOException {
        return post(query, null, null, headers);
    }

    public ClickHouseHttpResponse update(String query, InputStream data) throws IOException {
        return post(query, data, null, null);
    }

    public ClickHouseHttpResponse update(String query, InputStream data, Map<String, String> headers)
            throws IOException {
        return post(query, data, null, headers);
    }

    public ClickHouseHttpResponse query(String query) throws IOException {
        return post(query, null, null, null);
    }

    public ClickHouseHttpResponse query(String query, Map<String, String> headers) throws IOException {
        return post(query, null, null, headers);
    }

    public ClickHouseHttpResponse query(String query, List<ClickHouseExternalTable> tables) throws IOException {
        return post(query, null, tables, null);
    }

    public ClickHouseHttpResponse query(String query, List<ClickHouseExternalTable> tables, Map<String, String> headers)
            throws IOException {
        return post(query, null, tables, headers);
    }
}
