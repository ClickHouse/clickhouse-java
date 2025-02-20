package com.clickhouse.client.http;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseSslContextProvider;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseSslMode;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseExternalTable;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;

@Deprecated
public class HttpUrlConnectionImpl extends ClickHouseHttpConnection {
    private static final Logger log = LoggerFactory.getLogger(HttpUrlConnectionImpl.class);

    private static final String USER_AGENT = ClickHouseClientOption.buildUserAgent(null, "HttpURLConnection");

    private final HttpURLConnection conn;

    private ClickHouseHttpResponse buildResponse(ClickHouseOutputStream output, Runnable postCloseAction)
            throws IOException {
        // X-ClickHouse-Server-Display-Name: xxx
        // X-ClickHouse-Query-Id: xxx
        // X-ClickHouse-Format: RowBinaryWithNamesAndTypes
        // X-ClickHouse-Timezone: UTC
        // X-ClickHouse-Summary:
        // {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
        String displayName = getResponseHeader("X-ClickHouse-Server-Display-Name", server.getHost());
        String queryId = getResponseHeader("X-ClickHouse-Query-Id", "");
        String summary = getResponseHeader("X-ClickHouse-Summary", "{}");

        ClickHouseConfig c = config;
        ClickHouseFormat format = c.getFormat();
        TimeZone timeZone = c.getServerTimeZone();
        boolean hasCustomOutput = output != null && output.getUnderlyingStream().hasOutput();
        boolean hasQueryResult = false;
        // queryId, format and timeZone are only available for queries
        if (!ClickHouseChecker.isNullOrEmpty(queryId)) {
            String value = getResponseHeader("X-ClickHouse-Format", "");
            if (!ClickHouseChecker.isNullOrEmpty(value)) {
                format = ClickHouseFormat.valueOf(value);
                hasQueryResult = true;
            }
            value = getResponseHeader("X-ClickHouse-Timezone", "");
            timeZone = !ClickHouseChecker.isNullOrEmpty(value) ? TimeZone.getTimeZone(value)
                    : timeZone;
        }

        final InputStream source;
        final Runnable action;
        if (output != null) {
            source = ClickHouseInputStream.empty();
            action = () -> {
                try (OutputStream o = output) {
                    ClickHouseInputStream.pipe(conn.getInputStream(), o, c.getWriteBufferSize());
                    if (postCloseAction != null) {
                        postCloseAction.run();
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to redirect response to given output stream", e);
                }
            };
        } else {
            source = conn.getInputStream();
            action = postCloseAction;
        }
        return new ClickHouseHttpResponse(this,
                hasCustomOutput ? ClickHouseInputStream.of(source, c.getReadBufferSize(), action)
                        : (hasQueryResult ? ClickHouseClient.getAsyncResponseInputStream(c, source, action)
                                : ClickHouseClient.getResponseInputStream(c, source, action)),
                displayName, queryId, summary, format, timeZone);
    }

    private HttpURLConnection newConnection(String url, boolean post) throws IOException {
        ClickHouseConfig c = config;
        HttpURLConnection newConn;
        Proxy proxy = getProxy(c);
        if (proxy != null) {
            log.debug("using proxy type [%s] address [%s]", proxy.type().name(), proxy.address().toString());
            newConn = (HttpURLConnection) new URL(url).openConnection(proxy);
            String authString = getProxyAuth(c);
            if (authString != null) {
                newConn.setRequestProperty("Proxy-Authorization", authString);
            }
        } else {
            newConn = (HttpURLConnection) new URL(url).openConnection();
        }

        if ((newConn instanceof HttpsURLConnection) && c.isSsl()) {
            HttpsURLConnection secureConn = (HttpsURLConnection) newConn;
            SSLContext sslContext = ClickHouseSslContextProvider.getProvider().getSslContext(SSLContext.class, c)
                    .orElse(null);
            HostnameVerifier verifier = c.getSslMode() == ClickHouseSslMode.STRICT
                    ? HttpsURLConnection.getDefaultHostnameVerifier()
                    : (hostname, session) -> true; // NOSONAR

            secureConn.setHostnameVerifier(verifier);
            if (sslContext != null) {
                secureConn.setSSLSocketFactory(sslContext.getSocketFactory());
            }
        }

        if (post) {
            newConn.setInstanceFollowRedirects(true);
            newConn.setRequestMethod("POST");
        }
        newConn.setUseCaches(false);
        newConn.setAllowUserInteraction(false);
        newConn.setDoInput(true);
        newConn.setDoOutput(true);
        newConn.setConnectTimeout(c.getConnectionTimeout());
        newConn.setReadTimeout(c.getSocketTimeout());

        return newConn;
    }

    private String getResponseHeader(String header, String defaultValue) {
        String value = conn.getHeaderField(header);
        return value != null ? value : defaultValue;
    }

    private static final Set<String> maskedHeaders = new HashSet<>(Arrays.asList("authorization",
            "proxy-authorization","x-clickhouse-key"));

    private void setHeaders(HttpURLConnection conn, Map<String, String> headers) {
        headers = mergeHeaders(headers);

        if (headers != null && !headers.isEmpty()) {
            for (Entry<String, String> header : headers.entrySet()) {
                if (log.isDebugEnabled()) {
                    String value = header.getValue();
                    if (value != null && maskedHeaders.contains(header.getKey().toLowerCase())) {
                        value = "******";
                    }
                    log.debug("Adding header key [%s] value [%s]", header.getKey(), value);
                }
                conn.setRequestProperty(header.getKey(), header.getValue());
            }
        }
    }

    private void checkResponse(HttpURLConnection conn) throws IOException {
        log.debug("http response code [%d]", conn.getResponseCode());
        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
            String errorCode = conn.getHeaderField("X-ClickHouse-Exception-Code");
            // String encoding = conn.getHeaderField("Content-Encoding");
            String serverName = conn.getHeaderField("X-ClickHouse-Server-Display-Name");

            InputStream errorInput = conn.getErrorStream();
            if (errorInput == null) {
                // TODO follow redirects?
                throw new ConnectException(ClickHouseUtils.format(
                        "HTTP response %d %s (ClickHouse error %s returned from %s)", conn.getResponseCode(),
                        conn.getResponseMessage(), errorCode, serverName));
            }

            String errorMsg;
            int bufferSize = (int) ClickHouseClientOption.BUFFER_SIZE.getDefaultValue();
            ByteArrayOutputStream output = new ByteArrayOutputStream(bufferSize);
            ClickHouseInputStream.pipe(errorInput, output, bufferSize);
            byte[] bytes = output.toByteArray();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    ClickHouseClient.getResponseInputStream(config, new ByteArrayInputStream(bytes), null),
                    StandardCharsets.UTF_8))) {
                StringBuilder builder = new StringBuilder();
                while ((errorMsg = reader.readLine()) != null) {
                    builder.append(errorMsg).append('\n');
                }
                errorMsg = builder.toString();
            } catch (IOException e) {
                errorMsg = parseErrorFromException(errorCode, serverName, e, bytes);
            }
            throw new IOException(errorMsg);
        }
    }

    protected HttpUrlConnectionImpl(ClickHouseNode server, ClickHouseRequest<?> request, ExecutorService executor,
                                    Map<String, Serializable> additionalParams) throws IOException {
        super(server, request, additionalParams);

        conn = newConnection(url, true);
    }

    @Override
    protected final String getDefaultUserAgent() {
        return USER_AGENT;
    }

    @Override
    protected boolean isReusable() {
        return false;
    }

    @Override
    protected ClickHouseHttpResponse post(ClickHouseConfig config, String sql, ClickHouseInputStream data,
            List<ClickHouseExternalTable> tables, ClickHouseOutputStream output, String url,
            Map<String, String> headers, Runnable postCloseAction) throws IOException {
        byte[] boundary = null;
        if (tables != null && !tables.isEmpty()) {
            String uuid = rm.createUniqueId();
            conn.setRequestProperty("content-type", "multipart/form-data; boundary=".concat(uuid));
            boundary = uuid.getBytes(StandardCharsets.US_ASCII);
        } else {
            conn.setRequestProperty("content-type", "text/plain; charset=UTF-8");
        }
        setHeaders(conn, headers);

        if (data != null || boundary != null) {
            conn.setChunkedStreamingMode(config.getRequestChunkSize());
        } else {
            // TODO conn.setFixedLengthStreamingMode(contentLength);
        }

        postData(config, boundary, sql, data, tables, conn.getOutputStream());

        checkResponse(conn);

        return buildResponse(output, postCloseAction);
    }

    @Override
    public boolean ping(int timeout) {
        String response = config.getStrOption(ClickHouseHttpOption.DEFAULT_RESPONSE);
        String url = null;
        HttpURLConnection c = null;
        try {
            url = getBaseUrl().concat("ping");
            c = newConnection(url, false);
            c.setConnectTimeout(timeout);
            c.setReadTimeout(timeout);

            checkResponse(c);

            int size = 12;
            try (ByteArrayOutputStream out = new ByteArrayOutputStream(size)) {
                ClickHouseInputStream.pipe(c.getInputStream(), out, size);

                c.disconnect();
                c = null;
                return response.equals(new String(out.toByteArray(), StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            log.debug("Failed to ping url %s due to: %s", url, e.getMessage());
        } finally {
            if (c != null) {
                c.disconnect();
            }
        }

        return false;
    }

    @Override
    public void close() {
        conn.disconnect();
    }
}
