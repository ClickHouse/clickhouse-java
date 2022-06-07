package com.clickhouse.client.http;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseOutputStream;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseSslContextProvider;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseSslMode;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

public class HttpUrlConnectionImpl extends ClickHouseHttpConnection {
    private static final Logger log = LoggerFactory.getLogger(HttpUrlConnectionImpl.class);

    private static final byte[] HEADER_CONTENT_DISPOSITION = "Content-Disposition: form-data; name=\""
            .getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HEADER_OCTET_STREAM = "Content-Type: application/octet-stream\r\n"
            .getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HEADER_BINARY_ENCODING = "Content-Transfer-Encoding: binary\r\n\r\n"
            .getBytes(StandardCharsets.US_ASCII);

    private static final byte[] SUFFIX_QUERY = "query\"\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUFFIX_FORMAT = "_format\"\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUFFIX_STRUCTURE = "_structure\"\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUFFIX_FILENAME = "\"; filename=\"".getBytes(StandardCharsets.US_ASCII);

    private final HttpURLConnection conn;

    private ClickHouseHttpResponse buildResponse() throws IOException {
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

        return new ClickHouseHttpResponse(this,
                hasQueryResult ? ClickHouseClient.getAsyncResponseInputStream(c, conn.getInputStream(), null)
                        : ClickHouseClient.getResponseInputStream(c, conn.getInputStream(), null),
                displayName, queryId, summary, format, timeZone);
    }

    private HttpURLConnection newConnection(String url, boolean post) throws IOException {
        HttpURLConnection newConn = (HttpURLConnection) new URL(url).openConnection();

        if ((newConn instanceof HttpsURLConnection) && config.isSsl()) {
            HttpsURLConnection secureConn = (HttpsURLConnection) newConn;
            SSLContext sslContext = ClickHouseSslContextProvider.getProvider().getSslContext(SSLContext.class, config)
                    .orElse(null);
            HostnameVerifier verifier = config.getSslMode() == ClickHouseSslMode.STRICT
                    ? HttpsURLConnection.getDefaultHostnameVerifier()
                    : (hostname, session) -> true;

            secureConn.setHostnameVerifier(verifier);
            secureConn.setSSLSocketFactory(sslContext.getSocketFactory());
        }

        if (post) {
            newConn.setInstanceFollowRedirects(true);
            newConn.setRequestMethod("POST");
        }
        newConn.setUseCaches(false);
        newConn.setAllowUserInteraction(false);
        newConn.setDoInput(true);
        newConn.setDoOutput(true);
        newConn.setConnectTimeout(config.getConnectionTimeout());
        newConn.setReadTimeout(config.getSocketTimeout());

        return newConn;
    }

    private String getResponseHeader(String header, String defaultValue) {
        String value = conn.getHeaderField(header);
        return value != null ? value : defaultValue;
    }

    private void setHeaders(HttpURLConnection conn, Map<String, String> headers) {
        headers = mergeHeaders(headers);

        if (headers == null || headers.isEmpty()) {
            return;
        }

        for (Entry<String, String> header : headers.entrySet()) {
            conn.setRequestProperty(header.getKey(), header.getValue());
        }
    }

    private void checkResponse(HttpURLConnection conn) throws IOException {
        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
            String errorCode = conn.getHeaderField("X-ClickHouse-Exception-Code");
            // String encoding = conn.getHeaderField("Content-Encoding");
            String serverName = conn.getHeaderField("X-ClickHouse-Server-Display-Name");

            int bufferSize = (int) ClickHouseClientOption.BUFFER_SIZE.getDefaultValue();
            ByteArrayOutputStream output = new ByteArrayOutputStream(bufferSize);
            ClickHouseInputStream.pipe(conn.getErrorStream(), output, bufferSize);
            byte[] bytes = output.toByteArray();
            String errorMsg;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    ClickHouseClient.getResponseInputStream(config, new ByteArrayInputStream(bytes), null),
                    StandardCharsets.UTF_8))) {
                StringBuilder builder = new StringBuilder();
                while ((errorMsg = reader.readLine()) != null) {
                    builder.append(errorMsg).append('\n');
                }
                errorMsg = builder.toString();
            } catch (IOException e) {
                log.warn("Error while reading error message[code=%s] from server [%s]", errorCode, serverName, e);
                errorMsg = new String(bytes, StandardCharsets.UTF_8);
            }

            throw new IOException(errorMsg);
        }
    }

    protected HttpUrlConnectionImpl(ClickHouseNode server, ClickHouseRequest<?> request, ExecutorService executor)
            throws IOException {
        super(server, request);

        conn = newConnection(url, true);
    }

    @Override
    protected boolean isReusable() {
        return false;
    }

    @Override
    protected ClickHouseHttpResponse post(String sql, InputStream data, List<ClickHouseExternalTable> tables,
            Map<String, String> headers) throws IOException {
        Charset charset = StandardCharsets.US_ASCII;
        byte[] boundary = null;
        if (tables != null && !tables.isEmpty()) {
            String uuid = UUID.randomUUID().toString();
            conn.setRequestProperty("Content-Type", "multipart/form-data; boundary=".concat(uuid));
            boundary = uuid.getBytes(charset);
        } else {
            conn.setRequestProperty("Content-Type", "text/plain; charset=UTF-8");
        }
        setHeaders(conn, headers);

        ClickHouseConfig c = config;
        final boolean hasInput = data != null || boundary != null;
        if (hasInput) {
            conn.setChunkedStreamingMode(config.getRequestChunkSize());
        } else {
            // TODO conn.setFixedLengthStreamingMode(contentLength);
        }
        try (ClickHouseOutputStream out = hasInput
                ? ClickHouseClient.getAsyncRequestOutputStream(config, conn.getOutputStream(), null) // latch::countDown)
                : ClickHouseClient.getRequestOutputStream(c, conn.getOutputStream(), null)) {
            byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
            if (boundary != null) {
                byte[] linePrefix = new byte[] { '\r', '\n', '-', '-' };
                byte[] lineSuffix = new byte[] { '\r', '\n' };
                out.writeBytes(linePrefix);
                out.writeBytes(boundary);
                out.writeBytes(lineSuffix);
                out.writeBytes(HEADER_CONTENT_DISPOSITION);
                out.writeBytes(SUFFIX_QUERY);
                out.writeBytes(sqlBytes);
                for (ClickHouseExternalTable t : tables) {
                    byte[] tableName = t.getName().getBytes(StandardCharsets.UTF_8);
                    for (int i = 0; i < 3; i++) {
                        out.writeBytes(linePrefix);
                        out.writeBytes(boundary);
                        out.writeBytes(lineSuffix);
                        out.writeBytes(HEADER_CONTENT_DISPOSITION);
                        out.writeBytes(tableName);
                        if (i == 0) {
                            out.writeBytes(SUFFIX_FORMAT);
                            out.writeBytes(t.getFormat().name().getBytes(charset));
                        } else if (i == 1) {
                            out.writeBytes(SUFFIX_STRUCTURE);
                            out.writeBytes(t.getStructure().getBytes(StandardCharsets.UTF_8));
                        } else {
                            out.writeBytes(SUFFIX_FILENAME);
                            out.writeBytes(tableName);
                            out.writeBytes(new byte[] { '"', '\r', '\n' });
                            break;
                        }
                    }
                    out.writeBytes(HEADER_OCTET_STREAM);
                    out.writeBytes(HEADER_BINARY_ENCODING);
                    ClickHouseInputStream.pipe(t.getContent(), out, c.getWriteBufferSize());
                }
                out.writeBytes(linePrefix);
                out.writeBytes(boundary);
                out.writeBytes(new byte[] { '-', '-' });
                out.writeBytes(lineSuffix);
            } else {
                out.writeBytes(sqlBytes);
                if (data != null && data.available() > 0) {
                    // append \n
                    if (sqlBytes[sqlBytes.length - 1] != (byte) '\n') {
                        out.write(10);
                    }
                    ClickHouseInputStream.pipe(data, out, c.getWriteBufferSize());
                }
            }
        }

        checkResponse(conn);

        return buildResponse();
    }

    @Override
    public boolean ping(int timeout) {
        String response = (String) config.getOption(ClickHouseHttpOption.DEFAULT_RESPONSE);
        HttpURLConnection c = null;
        try {
            String pingUrl = getPingUrl();
            c = newConnection(pingUrl, false);
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
            log.debug("Failed to ping server: ", e.getMessage());
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
