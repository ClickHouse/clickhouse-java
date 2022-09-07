package com.clickhouse.client.http;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseCompression;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseOutputStream;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseOption;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.client.http.config.ClickHouseHttpOption;

public abstract class ClickHouseHttpConnection implements AutoCloseable {
    private static StringBuilder appendQueryParameter(StringBuilder builder, String key, String value) {
        return builder.append(urlEncode(key, StandardCharsets.UTF_8)).append('=')
                .append(urlEncode(value, StandardCharsets.UTF_8)).append('&');
    }

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

    static String buildQueryParams(ClickHouseRequest<?> request) {
        if (request == null) {
            return "";
        }

        ClickHouseConfig config = request.getConfig();
        StringBuilder builder = new StringBuilder();

        // start with custom query parameters first
        Map<String, String> customParams = ClickHouseOption
                .toKeyValuePairs(config.getStrOption(ClickHouseHttpOption.CUSTOM_PARAMS));
        for (Entry<String, String> cp : customParams.entrySet()) {
            appendQueryParameter(builder, cp.getKey(), cp.getValue());
        }

        ClickHouseInputStream chIn = request.getInputStream().orElse(null);
        if (chIn != null && chIn.getUnderlyingFile().isAvailable()) {
            appendQueryParameter(builder, "query", request.getStatements().get(0));
        } else if (config.isRequestCompressed()) {
            // inform server that client's request is compressed
            appendQueryParameter(builder, "decompress", "1");
        }

        ClickHouseOutputStream chOut = request.getOutputStream().orElse(null);
        if (chOut != null && chOut.getUnderlyingFile().isAvailable()) {
            appendQueryParameter(builder, "enable_http_compression", "1");
        } else if (config.isResponseCompressed()) {
            // request server to compress response
            appendQueryParameter(builder, "compress", "1");
        }

        Map<String, Serializable> settings = request.getSettings();
        List<String> stmts = request.getStatements(false);
        String settingKey = ClickHouseClientOption.MAX_EXECUTION_TIME.getKey();
        if (config.getMaxExecutionTime() > 0 && !settings.containsKey(settingKey)) {
            appendQueryParameter(builder, settingKey, String.valueOf(config.getMaxExecutionTime()));
        }
        settingKey = ClickHouseClientOption.MAX_RESULT_ROWS.getKey();
        if (config.getMaxResultRows() > 0L && !settings.containsKey(settingKey)) {
            appendQueryParameter(builder, settingKey, String.valueOf(config.getMaxResultRows()));
            appendQueryParameter(builder, "result_overflow_mode", "break");
        }
        settingKey = "log_comment";
        if (!stmts.isEmpty() && config.getBoolOption(ClickHouseClientOption.LOG_LEADING_COMMENT)
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
            appendQueryParameter(builder, ClickHouseClientOption.SESSION_ID.getKey(), optionalValue.get());

            if (config.isSessionCheck()) {
                appendQueryParameter(builder, ClickHouseClientOption.SESSION_CHECK.getKey(), "1");
            }
            if (config.getSessionTimeout() > 0) {
                // see default_session_timeout
                appendQueryParameter(builder, ClickHouseClientOption.SESSION_TIMEOUT.getKey(),
                        String.valueOf(config.getSessionTimeout()));
            }
        }

        optionalValue = request.getQueryId();
        if (optionalValue.isPresent()) {
            appendQueryParameter(builder, "query_id", optionalValue.get());
        }

        for (Map.Entry<String, Serializable> entry : settings.entrySet()) {
            appendQueryParameter(builder, entry.getKey(), String.valueOf(entry.getValue()));
        }

        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    static String buildUrl(String baseUrl, ClickHouseRequest<?> request) {
        ClickHouseConfig config = request.getConfig();

        StringBuilder builder = new StringBuilder().append(baseUrl);
        String context = config.getStrOption(ClickHouseHttpOption.WEB_CONTEXT);
        if (!ClickHouseChecker.isNullOrEmpty(context)) {
            char prev = '/';
            for (int i = 0, len = context.length(); i < len; i++) {
                char ch = context.charAt(i);
                if (ch == '?' || ch == '#') {
                    break;
                } else if (ch != '/' || ch != prev) {
                    builder.append(ch);
                }
                prev = ch;
            }
        }

        String query = buildQueryParams(request);
        if (!query.isEmpty()) {
            builder.append('?').append(query);
        }

        return builder.toString();
    }

    protected static Map<String, String> createDefaultHeaders(ClickHouseConfig config, ClickHouseNode server) {
        Map<String, String> map = new LinkedHashMap<>();
        boolean hasAuthorizationHeader = false;
        // add customer headers
        for (Entry<String, String> header : ClickHouseOption
                .toKeyValuePairs(config.getStrOption(ClickHouseHttpOption.CUSTOM_HEADERS)).entrySet()) {
            String name = header.getKey().toLowerCase(Locale.ROOT);
            String value = header.getValue();
            if (value == null) {
                continue;
            }
            if ("authorization".equals(name)) {
                hasAuthorizationHeader = true;
            }
            map.put(name, value);
        }

        map.put("accept", "*/*");
        if (!config.getBoolOption(ClickHouseHttpOption.KEEP_ALIVE)) {
            map.put("connection", "Close");
        }
        map.put("user-agent", config.getClientName());

        ClickHouseCredentials credentials = server.getCredentials(config);
        if (credentials.useAccessToken()) {
            // TODO check if auth-scheme is available and supported
            map.put("authorization", credentials.getAccessToken());
        } else if (!hasAuthorizationHeader) {
            map.put("x-clickhouse-user", credentials.getUserName());
            if (config.isSsl() && !ClickHouseChecker.isNullOrEmpty(config.getSslCert())) {
                map.put("x-clickhouse-ssl-certificate-auth", "on");
            } else if (!ClickHouseChecker.isNullOrEmpty(credentials.getPassword())) {
                map.put("x-clickhouse-key", credentials.getPassword());
            }
        }

        String database = server.getDatabase(config);
        if (!ClickHouseChecker.isNullOrEmpty(database)) {
            map.put("x-clickhouse-database", database);
        }
        // Also, you can use the ‘default_format’ URL parameter
        map.put("x-clickhouse-format", config.getFormat().name());
        if (config.isResponseCompressed()) {
            map.put("accept-encoding", config.getResponseCompressAlgorithm().encoding());
        }
        if (config.isRequestCompressed()
                && config.getRequestCompressAlgorithm() != ClickHouseCompression.LZ4) {
            map.put("content-encoding", config.getRequestCompressAlgorithm().encoding());
        }
        return map;
    }

    protected final ClickHouseConfig config;
    protected final ClickHouseNode server;
    protected final ClickHouseOutputStream output;
    protected final String url;
    protected final Map<String, String> defaultHeaders;

    protected ClickHouseHttpConnection(ClickHouseNode server, ClickHouseRequest<?> request) {
        if (server == null || request == null) {
            throw new IllegalArgumentException("Non-null server and request are required");
        }

        this.config = request.getConfig();
        this.server = server;
        this.output = request.getOutputStream().orElse(null);
        this.url = buildUrl(server.getBaseUri(), request);
        this.defaultHeaders = Collections.unmodifiableMap(createDefaultHeaders(config, server));
    }

    protected void closeQuietly() {
        try {
            close();
        } catch (Exception e) {
            // ignore
        }
    }

    protected String getBaseUrl() {
        int index = url.indexOf('?');
        if (index < 1) {
            index = url.length();
        }

        String baseUrl = url.substring(0, index);
        if (url.charAt(index - 1) != '/') {
            baseUrl = baseUrl.concat("/");
        }

        return baseUrl;
    }

    /**
     * Creates a merged map.
     *
     * @param requestHeaders request headers
     * @return non-null merged headers
     */
    protected Map<String, String> mergeHeaders(Map<String, String> requestHeaders) {
        if (requestHeaders == null || requestHeaders.isEmpty()) {
            return defaultHeaders;
        } else if (isReusable()) {
            return requestHeaders;
        }

        Map<String, String> merged = new LinkedHashMap<>();
        merged.putAll(defaultHeaders);
        for (Entry<String, String> header : requestHeaders.entrySet()) {
            String name = header.getKey().toLowerCase(Locale.ROOT);
            String value = header.getValue();
            if (value == null) {
                merged.remove(name);
            } else {
                merged.put(name, value);
            }
        }
        return merged;
    }

    /**
     * Posts query and data to server.
     *
     * @param query           non-blank query
     * @param data            optionally input stream for batch updating
     * @param tables          optionally external tables for query
     * @param url             optionally url
     * @param headers         optionally request headers
     * @param config          optionally configuration
     * @param postCloseAction optionally post action
     * @return response
     * @throws IOException when error occured posting request and/or server failed
     *                     to respond
     */
    protected abstract ClickHouseHttpResponse post(String query, ClickHouseInputStream data,
            List<ClickHouseExternalTable> tables, String url, Map<String, String> headers, ClickHouseConfig config,
            Runnable postCloseAction) throws IOException;

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
        return post(query, null, null, null, null, null, null);
    }

    public ClickHouseHttpResponse update(String query, Map<String, String> headers) throws IOException {
        return post(query, null, null, null, headers, null, null);
    }

    public ClickHouseHttpResponse update(String query, ClickHouseInputStream data) throws IOException {
        return post(query, data, null, null, null, null, null);
    }

    public ClickHouseHttpResponse update(String query, ClickHouseInputStream data, Map<String, String> headers)
            throws IOException {
        return post(query, data, null, null, headers, null, null);
    }

    public ClickHouseHttpResponse query(String query) throws IOException {
        return post(query, null, null, null, null, null, null);
    }

    public ClickHouseHttpResponse query(String query, Map<String, String> headers) throws IOException {
        return post(query, null, null, null, headers, null, null);
    }

    public ClickHouseHttpResponse query(String query, List<ClickHouseExternalTable> tables) throws IOException {
        return post(query, null, tables, null, null, null, null);
    }

    public ClickHouseHttpResponse query(String query, List<ClickHouseExternalTable> tables, Map<String, String> headers)
            throws IOException {
        return post(query, null, tables, null, headers, null, null);
    }
}
