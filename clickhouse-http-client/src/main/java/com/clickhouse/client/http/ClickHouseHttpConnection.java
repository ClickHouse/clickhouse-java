package com.clickhouse.client.http;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.*;
import java.util.Map.Entry;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseRequestManager;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseByteUtils;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseExternalTable;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;
import org.apache.hc.core5.http.HttpHeaders;

@Deprecated
public abstract class ClickHouseHttpConnection implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseHttpConnection.class);

    private static final byte[] HEADER_CONTENT_DISPOSITION = "content-disposition: form-data; name=\""
            .getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HEADER_OCTET_STREAM = "content-type: application/octet-stream\r\n"
            .getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HEADER_BINARY_ENCODING = "content-transfer-encoding: binary\r\n\r\n"
            .getBytes(StandardCharsets.US_ASCII);

    private static final byte[] ERROR_MSG_PREFIX = "ode: ".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] DOUBLE_DASH = new byte[] { '-', '-' };
    private static final byte[] END_OF_NAME = new byte[] { '"', '\r', '\n' };
    private static final byte[] LINE_PREFIX = new byte[] { '\r', '\n', '-', '-' };
    private static final byte[] LINE_SUFFIX = new byte[] { '\r', '\n' };

    private static final byte[] SUFFIX_QUERY = "query\"\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUFFIX_FORMAT = "_format\"\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUFFIX_STRUCTURE = "_structure\"\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUFFIX_FILENAME = "\"; filename=\"".getBytes(StandardCharsets.US_ASCII);

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

    static String buildQueryParams(ClickHouseRequest<?> request, Map<String, Serializable> additionalParams) {
        if (request == null) {
            return "";
        }

        if (additionalParams == null) {
            additionalParams = Collections.emptyMap();
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
        if (chIn != null && chIn.getUnderlyingStream().hasInput()) {
            appendQueryParameter(builder, "query", request.getStatements(false).get(0));
        }
        if (config.isRequestCompressed() && config.getRequestCompressAlgorithm() == ClickHouseCompression.LZ4) {
            // inform server to decompress client request
            appendQueryParameter(builder, "decompress", "1");
        }

        if (config.isResponseCompressed()) {
            if (config.getResponseCompressAlgorithm() == ClickHouseCompression.LZ4) {
                // request server to compress response
                appendQueryParameter(builder, "compress", "1");
            } else {
                appendQueryParameter(builder, "enable_http_compression", "1");
            }
        }

        Map<String, Serializable> settings = request.getSettings();
        List<String> stmts = request.getStatements(false);
        String settingKey = ClickHouseClientOption.MAX_EXECUTION_TIME.getKey();
        if (config.getMaxExecutionTime() > 0 && !settings.containsKey(settingKey)) {
            appendQueryParameter(builder, settingKey, String.valueOf(config.getMaxExecutionTime()));
        }
        settingKey = ClickHouseClientOption.MAX_RESULT_ROWS.getKey();
        boolean hasRequestSetting = settings.containsKey(settingKey);
        if (config.getMaxResultRows() > 0L && !hasRequestSetting) {
            // set on client level
            appendQueryParameter(builder, settingKey, String.valueOf(config.getMaxResultRows()));
        } else if (hasRequestSetting) {
            // set on request level
            Object value = settings.get(settingKey);
            if (value instanceof Number && ((Number) value).longValue() > 0L) {
                appendQueryParameter(builder, settingKey, String.valueOf(value));
            } else if (value instanceof String && !(((String) value).isEmpty() || "0".equals(value))) {
                appendQueryParameter(builder, settingKey, (String) value);
            }
        }

        if (config.hasOption(ClickHouseClientOption.RESULT_OVERFLOW_MODE)) {
            appendQueryParameter(builder, ClickHouseClientOption.RESULT_OVERFLOW_MODE.getKey(),
                    config.getStrOption(ClickHouseClientOption.RESULT_OVERFLOW_MODE));
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

        // Handle additional parameters
        if (additionalParams.containsKey("_roles")) {
            Serializable value = additionalParams.get("_roles");
            Set<String> roles = !(value instanceof Set) ? Collections.emptySet() : (Set<String>) value;
            roles.forEach(role -> appendQueryParameter(builder, "role", role));
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

        for (Entry<String, Serializable> entry : settings.entrySet()) {
            if (!processedSettings.contains(entry.getKey())) {
                appendQueryParameter(builder, entry.getKey(), String.valueOf(entry.getValue()));
            }
        }

        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    // Settings that are processed by the client and appropriate parameters are set
    private static final HashSet<String> processedSettings = new HashSet<>(Arrays.asList(
            "_set_roles_stmt",
            ClickHouseClientOption.MAX_RESULT_ROWS.getKey()
    ));

    static String buildUrl(String baseUrl, ClickHouseRequest<?> request, Map<String, Serializable> additionalParams) {
        StringBuilder builder = new StringBuilder().append(baseUrl);
        // TODO: Using default until we will remove
        String context = "/";
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

        String query = buildQueryParams(request, additionalParams);
        if (!query.isEmpty()) {
            builder.append('?').append(query);
        }

        return builder.toString();
    }

    protected static Map<String, String> createDefaultHeaders(ClickHouseConfig config, ClickHouseNode server,
            String userAgent, String referer) {
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

        if (!ClickHouseChecker.isNullOrEmpty(referer)) {
            map.put("referer", referer);
        }

        map.put("accept", "*/*");
        if (!config.getBoolOption(ClickHouseHttpOption.KEEP_ALIVE)) {
            map.put("connection", "Close");
        }
        map.put("user-agent", !ClickHouseChecker.isNullOrEmpty(userAgent) ? userAgent : config.getClientName());

        ClickHouseCredentials credentials = config.getDefaultCredentials();
        if (credentials.useAccessToken()) {
            // TODO check if auth-scheme is available and supported
            map.put("authorization", credentials.getAccessToken());
        } else if (!hasAuthorizationHeader) {
            if (config.isSsl() && !ClickHouseChecker.isNullOrEmpty(config.getSslCert())) {
                map.put(ClickHouseHttpProto.HEADER_DB_USER, credentials.getUserName());
                map.put(ClickHouseHttpProto.HEADER_SSL_CERT_AUTH, "on");
            } else {
                boolean useBasicAuthentication = config.getBoolOption(ClickHouseHttpOption.USE_BASIC_AUTHENTICATION);
                if (useBasicAuthentication) {
                    String password = credentials.getPassword() == null ? "" : credentials.getPassword();
                    map.put(HttpHeaders.AUTHORIZATION, "Basic " + Base64.getEncoder()
                            .encodeToString((credentials.getUserName() + ":" + password).getBytes(StandardCharsets.UTF_8)));
                } else {
                    map.put(ClickHouseHttpProto.HEADER_DB_USER, credentials.getUserName());
                    map.put(ClickHouseHttpProto.HEADER_DB_PASSWORD, credentials.getPassword());
                }
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

    protected static Proxy getProxy(ClickHouseConfig config) {
        final ClickHouseProxyType proxyType = config.getProxyType();

        Proxy proxy;
        switch (proxyType) {
            case HTTP:
                proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(config.getProxyHost(), config.getProxyPort()));
                break;
            case DIRECT:
                proxy = Proxy.NO_PROXY;
                break;
            case SOCKS:
                proxy = new Proxy(Proxy.Type.SOCKS,
                        new InetSocketAddress(config.getProxyHost(), config.getProxyPort()));
                break;
            default:
                proxy = null;
                break;
        }
        return proxy;
    }

    protected static String getProxyAuth(ClickHouseConfig config) {
        String authHeader = null;
        if (config.getProxyType() == ClickHouseProxyType.HTTP) {
            String userName = config.getProxyUserName();
            if (!ClickHouseChecker.isNullOrEmpty(userName)) {
                String auth = userName + ":" + new String(config.getProxyPassword());
                byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
                authHeader = "Basic " + new String(encodedAuth);
            }
        }
        return authHeader;
    }

    protected static String parseErrorFromException(String errorCode, String serverName, IOException e, byte[] bytes) {
        log.debug("Failed to read error message[code=%s] from server [%s] due to: %s", errorCode, serverName,
                e.getMessage());

        int index = ClickHouseByteUtils.indexOf(bytes, ERROR_MSG_PREFIX);
        final String errorMsg;
        if (index > 0) {
            bytes[--index] = (byte) 'C';
            errorMsg = new String(bytes, index, bytes.length - index, StandardCharsets.UTF_8);
        } else if (!ClickHouseChecker.isNullOrBlank(errorCode)) {
            errorMsg = new StringBuilder().append("Code: ").append(errorCode).append(", server: ").append(serverName)
                    .append(", ").append(new String(bytes, StandardCharsets.UTF_8)).toString();
        } else {
            // uncomment to debug
            // log.debug("Unparsable error message[code=%s] returned from server[%s]: %s",
            // errorCode, serverName, ClickHouseUtils.toJavaByteArrayExpression(bytes));
            errorMsg = new String(bytes, StandardCharsets.UTF_8);
        }
        return errorMsg;
    }

    protected static void postData(ClickHouseConfig config, byte[] boundary, String sql, ClickHouseInputStream data,
            List<ClickHouseExternalTable> tables, OutputStream requestStream) throws IOException {
        final boolean hasCustomInput = data != null && data.getUnderlyingStream().hasInput();

        try (OutputStream rawOut = requestStream;
                ClickHouseOutputStream out = hasCustomInput
                        ? ClickHouseOutputStream.of(rawOut, config.getWriteBufferSize())
                        : (data != null || boundary != null // NOSONAR
                                ? ClickHouseClient.getAsyncRequestOutputStream(config, rawOut, null) // latch::countDown)
                                : ClickHouseClient.getRequestOutputStream(config, rawOut, null))) {
            byte[] sqlBytes = hasCustomInput ? new byte[0] : sql.getBytes(StandardCharsets.UTF_8);
            if (boundary != null) {
                rawOut.write(LINE_PREFIX);
                rawOut.write(boundary);
                rawOut.write(LINE_SUFFIX);
                rawOut.write(HEADER_CONTENT_DISPOSITION);
                rawOut.write(SUFFIX_QUERY);
                rawOut.write(sqlBytes);

                final int writeBufferSize = config.getWriteBufferSize();
                for (ClickHouseExternalTable t : tables) {
                    byte[] tableName = t.getName().getBytes(StandardCharsets.UTF_8);
                    for (int i = 0; i < 3; i++) {
                        rawOut.write(LINE_PREFIX);
                        rawOut.write(boundary);
                        rawOut.write(LINE_SUFFIX);
                        rawOut.write(HEADER_CONTENT_DISPOSITION);
                        rawOut.write(tableName);
                        if (i == 0) {
                            rawOut.write(SUFFIX_FORMAT);
                            rawOut.write(t.getFormat().name().getBytes(StandardCharsets.US_ASCII));
                        } else if (i == 1) {
                            rawOut.write(SUFFIX_STRUCTURE);
                            rawOut.write(t.getStructure().getBytes(StandardCharsets.UTF_8));
                        } else {
                            rawOut.write(SUFFIX_FILENAME);
                            rawOut.write(tableName);
                            rawOut.write(END_OF_NAME);
                            break;
                        }
                    }
                    rawOut.write(HEADER_OCTET_STREAM);
                    rawOut.write(HEADER_BINARY_ENCODING);
                    ClickHouseInputStream.pipe(t.getContent(), rawOut, writeBufferSize);
                }
                rawOut.write(LINE_PREFIX);
                rawOut.write(boundary);
                rawOut.write(DOUBLE_DASH);
                rawOut.write(LINE_SUFFIX);
            } else {
                out.writeBytes(sqlBytes);
                if (data != null && data.available() > 0) {
                    // append \n
                    if (sqlBytes.length > 0 && sqlBytes[sqlBytes.length - 1] != (byte) '\n') {
                        out.write(10);
                    }
                    ClickHouseInputStream.pipe(data, out, config.getWriteBufferSize());
                }
            }
        }
    }

    protected final ClickHouseNode server;
    protected final ClickHouseRequestManager rm;

    protected final ClickHouseConfig config;
    protected final Map<String, String> defaultHeaders;
    protected final String url;

    protected ClickHouseHttpConnection(ClickHouseNode server, ClickHouseRequest<?> request,
                                       Map<String, Serializable> additionalParams) {
        if (server == null || request == null) {
            throw new IllegalArgumentException("Non-null server and request are required");
        }

        this.server = server;
        this.rm = request.getManager();

        ClickHouseConfig c = request.getConfig();
        this.config = c;
        this.defaultHeaders = Collections.unmodifiableMap(createDefaultHeaders(c, server, getUserAgent(), ClickHouseHttpClient.getReferer(config)));
        this.url = buildUrl(server.getBaseUri(), request, additionalParams);
        log.debug("url [%s]", this.url);
    }

    protected void closeQuietly() {
        try {
            close();
        } catch (Exception e) {
            // ignore
        }
    }

    protected String getBaseUrl() {
        String u = url;
        int index = u.indexOf('?');
        if (index < 1) {
            index = u.length();
        }

        String baseUrl = u.substring(0, index);
        if (u.charAt(index - 1) != '/') {
            baseUrl = baseUrl.concat("/");
        }

        return baseUrl;
    }

    protected String getDefaultUserAgent() {
        return config.getClientName();
    }

    protected final String getUserAgent() {
        final ClickHouseConfig c = config;
        String name = c.getClientName();
        String userAgent = getDefaultUserAgent();

        if (!ClickHouseClientOption.CLIENT_NAME.getDefaultValue().equals(name)) {
            return name + " " + userAgent;
        }

        name = c.getProductName();
        String version = c.getProductVersion();
        if (!ClickHouseClientOption.PRODUCT_VERSION.equals(version)) {
            name = name + "/" + c.getProductVersion();
        }
        return ClickHouseClientOption.PRODUCT_NAME.getDefaultValue().equals(name) ? userAgent : name + " " + userAgent;
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
     * @param config          non-null configuration
     * @param data            optionally input stream for batch updating
     * @param tables          optionally external tables for query
     * @param output          optionally output stream
     * @param url             optionally url
     * @param headers         optionally request headers
     * @param postCloseAction optionally post action
     * @return response
     * @throws IOException when error occured posting request and/or server failed
     *                     to respond
     */
    protected abstract ClickHouseHttpResponse post(ClickHouseConfig config, String query, ClickHouseInputStream data,
            List<ClickHouseExternalTable> tables, ClickHouseOutputStream output, String url,
            Map<String, String> headers, Runnable postCloseAction) throws IOException;

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
        return post(config, query, null, null, null, null, null, null);
    }

    public ClickHouseHttpResponse update(String query, Map<String, String> headers) throws IOException {
        return post(config, query, null, null, null, null, headers, null);
    }

    public ClickHouseHttpResponse update(String query, ClickHouseInputStream data) throws IOException {
        return post(config, query, data, null, null, null, null, null);
    }

    public ClickHouseHttpResponse update(String query, ClickHouseInputStream data, Map<String, String> headers)
            throws IOException {
        return post(config, query, data, null, null, null, headers, null);
    }

    public ClickHouseHttpResponse query(String query) throws IOException {
        return post(config, query, null, null, null, null, null, null);
    }

    public ClickHouseHttpResponse query(String query, Map<String, String> headers) throws IOException {
        return post(config, query, null, null, null, null, headers, null);
    }

    public ClickHouseHttpResponse query(String query, List<ClickHouseExternalTable> tables) throws IOException {
        return post(config, query, null, tables, null, null, null, null);
    }

    public ClickHouseHttpResponse query(String query, List<ClickHouseExternalTable> tables, Map<String, String> headers)
            throws IOException {
        return post(config, query, null, tables, null, null, headers, null);
    }

    @Override
    public void close() throws IOException {
        // ignore
    }
}
