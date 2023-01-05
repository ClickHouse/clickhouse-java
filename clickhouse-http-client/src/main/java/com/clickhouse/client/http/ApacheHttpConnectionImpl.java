package com.clickhouse.client.http;

import com.clickhouse.client.AbstractSocketClient;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseSslContextProvider;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseSslMode;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.Timeout;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

/**
 * Created by wujianchao on 2022/12/1.
 */
public class ApacheHttpConnectionImpl extends ClickHouseHttpConnection {

    private static final Logger log = LoggerFactory.getLogger(ApacheHttpConnectionImpl.class);

    private static final byte[] HEADER_CONTENT_DISPOSITION = "content-disposition: form-data; name=\""
            .getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HEADER_OCTET_STREAM = "content-type: application/octet-stream\r\n"
            .getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HEADER_BINARY_ENCODING = "content-transfer-encoding: binary\r\n\r\n"
            .getBytes(StandardCharsets.US_ASCII);

    private static final byte[] ERROR_MSG_PREFIX = "Code: ".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] DOUBLE_DASH = new byte[] { '-', '-' };
    private static final byte[] END_OF_NAME = new byte[] { '"', '\r', '\n' };
    private static final byte[] LINE_PREFIX = new byte[] { '\r', '\n', '-', '-' };
    private static final byte[] LINE_SUFFIX = new byte[] { '\r', '\n' };

    private static final byte[] SUFFIX_QUERY = "query\"\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUFFIX_FORMAT = "_format\"\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUFFIX_STRUCTURE = "_structure\"\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUFFIX_FILENAME = "\"; filename=\"".getBytes(StandardCharsets.US_ASCII);

    private final CloseableHttpClient client;

    protected ApacheHttpConnectionImpl(ClickHouseNode server, ClickHouseRequest<?> request, ExecutorService executor)
            throws IOException, URISyntaxException {
        super(server, request);
        client = newConnection();
    }

    private CloseableHttpClient newConnection() throws IOException {
        RegistryBuilder<ConnectionSocketFactory> r = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", SocketFactory.create(config));
        if (config.isSsl()) {
            r.register("https", SSLSocketFactory.create(config));
        }
        return HttpClientBuilder.create()
                .setConnectionManager(new HttpConnectionManager(r.build(), config)).disableContentCompression().build();
    }

    private ClickHouseHttpResponse buildResponse(CloseableHttpResponse response, ClickHouseConfig config,
            Runnable postCloseAction) throws IOException {
        // X-ClickHouse-Server-Display-Name: xxx
        // X-ClickHouse-Query-Id: xxx
        // X-ClickHouse-Format: RowBinaryWithNamesAndTypes
        // X-ClickHouse-Timezone: UTC
        // X-ClickHouse-Summary:
        // {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
        String displayName = getResponseHeader(response, "X-ClickHouse-Server-Display-Name", server.getHost());
        String queryId = getResponseHeader(response, "X-ClickHouse-Query-Id", "");
        String summary = getResponseHeader(response, "X-ClickHouse-Summary", "{}");

        ClickHouseConfig c = config;
        ClickHouseFormat format = c.getFormat();
        TimeZone timeZone = c.getServerTimeZone();
        boolean hasOutputFile = output != null && output.getUnderlyingFile().isAvailable();
        boolean hasQueryResult = false;
        // queryId, format and timeZone are only available for queries
        if (!ClickHouseChecker.isNullOrEmpty(queryId)) {
            String value = getResponseHeader(response, "X-ClickHouse-Format", "");
            if (!ClickHouseChecker.isNullOrEmpty(value)) {
                format = ClickHouseFormat.valueOf(value);
                hasQueryResult = true;
            }
            value = getResponseHeader(response, "X-ClickHouse-Timezone", "");
            timeZone = !ClickHouseChecker.isNullOrEmpty(value) ? TimeZone.getTimeZone(value)
                    : timeZone;
        }

        final InputStream source;
        final Runnable action;
        if (output != null) {
            source = ClickHouseInputStream.empty();
            action = () -> {
                try (OutputStream o = output) {
                    ClickHouseInputStream.pipe(response.getEntity().getContent(), o, c.getWriteBufferSize());
                    if (postCloseAction != null) {
                        postCloseAction.run();
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to redirect response to given output stream", e);
                }
            };
        } else {
            source = response.getEntity().getContent();
            action = postCloseAction;
        }
        return new ClickHouseHttpResponse(this,
                hasOutputFile ? ClickHouseInputStream.of(source, c.getReadBufferSize(), action)
                        : (hasQueryResult ? ClickHouseClient.getAsyncResponseInputStream(c, source, action)
                                : ClickHouseClient.getResponseInputStream(c, source, action)),
                displayName, queryId, summary, format, timeZone);
    }

    private String getResponseHeader(CloseableHttpResponse response, String header, String defaultValue) {
        Header h = response.getFirstHeader(header);
        return h == null ? defaultValue : h.getValue();
    }

    private void setHeaders(HttpRequest request, Map<String, String> headers) {
        headers = mergeHeaders(headers);

        if (headers != null && !headers.isEmpty()) {
            for (Map.Entry<String, String> header : headers.entrySet()) {
                request.setHeader(header.getKey(), header.getValue());
            }
        }
    }

    private void checkResponse(CloseableHttpResponse response) throws IOException {
        if (response.getEntity() == null) {
            throw new ConnectException(
                    ClickHouseUtils.format("HTTP response %d, %s", response.getCode(), response.getReasonPhrase()));
        }

        if (response.getCode() == 200) {
            return;
        }

        Header errorCode = response.getFirstHeader("X-ClickHouse-Exception-Code");
        Header serverName = response.getFirstHeader("X-ClickHouse-Server-Display-Name");

        String errorMsg;

        int bufferSize = (int) ClickHouseClientOption.BUFFER_SIZE.getDefaultValue();
        ByteArrayOutputStream output = new ByteArrayOutputStream(bufferSize);
        ClickHouseInputStream.pipe(response.getEntity().getContent(), output, bufferSize);
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
            log.debug("Failed to read error message[code=%s] from server [%s] due to: %s",
                    errorCode.getValue(),
                    serverName.getValue(),
                    e.getMessage());
            int index = ClickHouseUtils.indexOf(bytes, ERROR_MSG_PREFIX);
            errorMsg = index > 0 ? new String(bytes, index, bytes.length - index, StandardCharsets.UTF_8)
                    : new String(bytes, StandardCharsets.UTF_8);
        }
        throw new IOException(errorMsg);
    }

    @Override
    protected boolean isReusable() {
        return true;
    }

    protected ClickHouseHttpResponse post(String sql, ClickHouseInputStream data, List<ClickHouseExternalTable> tables,
            String url, Map<String, String> headers, ClickHouseConfig config,
            Runnable postCloseAction) throws IOException {
        HttpPost post = new HttpPost(url == null ? this.url : url);
        setHeaders(post, headers);
        byte[] boundary = null;
        String contentType = "text/plain; charset=UTF-8";

        if (tables != null && !tables.isEmpty()) {
            String uuid = rm.createUniqueId();
            contentType = "multipart/form-data; boundary=".concat(uuid);
            boundary = uuid.getBytes(StandardCharsets.US_ASCII);
        }

        post.setHeader("Content-Type", contentType);

        final boolean hasFile = data != null && data.getUnderlyingFile().isAvailable();
        final boolean hasInput = data != null || boundary != null;

        Charset ascii = StandardCharsets.US_ASCII;
        Charset utf8 = StandardCharsets.UTF_8;

        List<InputStream> inputParts = new ArrayList<>();

        byte[] sqlBytes = hasFile ? new byte[0] : sql.getBytes(utf8);
        if (boundary != null) {
            // head
            inputParts.add(ClickHouseInputStream.of(LINE_PREFIX, boundary, LINE_SUFFIX, HEADER_CONTENT_DISPOSITION,
                    SUFFIX_QUERY, sqlBytes));

            for (ClickHouseExternalTable t : tables) {
                byte[] tableName = t.getName().getBytes(utf8);
                // table head
                List<byte[]> tableHead = new ArrayList<>();
                for (int i = 0; i < 3; i++) {
                    tableHead.add(LINE_PREFIX);
                    tableHead.add(boundary);
                    tableHead.add(LINE_SUFFIX);
                    tableHead.add(HEADER_CONTENT_DISPOSITION);
                    tableHead.add(tableName);
                    if (i == 0) {
                        tableHead.add(SUFFIX_FORMAT);
                        tableHead.add(t.getFormat().name().getBytes(ascii));
                    } else if (i == 1) {
                        tableHead.add(SUFFIX_STRUCTURE);
                        tableHead.add(t.getStructure().getBytes(utf8));
                    } else {
                        tableHead.add(SUFFIX_FILENAME);
                        tableHead.add(tableName);
                        tableHead.add(END_OF_NAME);
                        break;
                    }
                }
                tableHead.add(HEADER_OCTET_STREAM);
                tableHead.add(HEADER_BINARY_ENCODING);
                inputParts.add(ClickHouseInputStream.of(tableHead, byte[].class, null, null));

                // table content
                inputParts.add(t.getContent());
            }
            // tail
            inputParts.add(ClickHouseInputStream.of(LINE_PREFIX, boundary, DOUBLE_DASH, LINE_SUFFIX));
        } else {
            List<byte[]> content = new ArrayList<>(2);
            content.add(sqlBytes);
            if (data != null && data.available() > 0) {
                // append \n
                if (sqlBytes.length > 0 && sqlBytes[sqlBytes.length - 1] != (byte) '\n') {
                    content.add(new byte[] { '\n' });
                }
                inputParts.add(ClickHouseInputStream.of(content, byte[].class, null, null));
                inputParts.add(data);
            } else {
                inputParts.add(ClickHouseInputStream.of(content, byte[].class, null, null));
            }
        }

        ClickHouseInputStream input = ClickHouseInputStream.of(inputParts, InputStream.class, null, null);

        String contentEncoding = headers == null ? null : headers.getOrDefault("content-encoding", null);
        ClickHouseHttpEntity postBody = new ClickHouseHttpEntity(input, config, contentType, contentEncoding, hasFile,
                hasInput);

        post.setEntity(postBody);
        CloseableHttpResponse response = client.execute(post);

        checkResponse(response);
        // buildResponse should use the config of current request in case of reusable
        // connection.
        return buildResponse(response, config, postCloseAction);
    }

    @Override
    public boolean ping(int timeout) {
        String url = getBaseUrl().concat("ping");
        HttpGet ping = new HttpGet(url);

        try (CloseableHttpClient httpClient = newConnection();
                CloseableHttpResponse response = httpClient.execute(ping)) {
            // TODO set timeout
            checkResponse(response);
            String ok = config.getStrOption(ClickHouseHttpOption.DEFAULT_RESPONSE);
            return ok.equals(EntityUtils.toString(response.getEntity()));

        } catch (Exception e) {
            log.debug("Failed to ping url %s due to: %s", url, e.getMessage());
        }

        return false;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    static class SocketFactory extends PlainConnectionSocketFactory {
        private final ClickHouseConfig config;

        private SocketFactory(ClickHouseConfig config) {
            this.config = config;
        }

        @Override
        public Socket createSocket(final HttpContext context) throws IOException {
            return AbstractSocketClient.setSocketOptions(config, new Socket());
        }

        public static SocketFactory create(ClickHouseConfig config) {
            return new SocketFactory(config);
        }
    }

    static class SSLSocketFactory extends SSLConnectionSocketFactory {
        private final ClickHouseConfig config;

        private SSLSocketFactory(ClickHouseConfig config) throws SSLException {
            super(Objects.requireNonNull(
                    ClickHouseSslContextProvider.getProvider().getSslContext(SSLContext.class, config)
                            .orElse(SSLContexts.createDefault())),
                    config.getSslMode() == ClickHouseSslMode.STRICT
                            ? HttpsURLConnection.getDefaultHostnameVerifier()
                            : (hostname, session) -> true);
            this.config = config;
        }

        @Override
        public Socket createSocket(HttpContext context) throws IOException {
            return AbstractSocketClient.setSocketOptions(config, new Socket());
        }

        public static SSLSocketFactory create(ClickHouseConfig config) throws SSLException {
            return new SSLSocketFactory(config);
        }
    }

    static class HttpConnectionManager extends PoolingHttpClientConnectionManager {
        public HttpConnectionManager(Registry<ConnectionSocketFactory> socketFactory, ClickHouseConfig config)
                throws SSLException {
            super(socketFactory);

            ConnectionConfig connConfig = ConnectionConfig.custom()
                    .setConnectTimeout(Timeout.of(config.getConnectionTimeout(), TimeUnit.MILLISECONDS))
                    .build();
            setDefaultConnectionConfig(connConfig);
            SocketConfig socketConfig = SocketConfig.custom()
                    .setSoTimeout(Timeout.of(config.getSocketTimeout(), TimeUnit.MILLISECONDS))
                    .setRcvBufSize(config.getReadBufferSize())
                    .setSndBufSize(config.getWriteBufferSize())
                    .build();
            setDefaultSocketConfig(socketConfig);
        }
    }
}
