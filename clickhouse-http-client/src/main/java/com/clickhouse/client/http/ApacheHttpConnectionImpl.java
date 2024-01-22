package com.clickhouse.client.http;

import com.clickhouse.client.AbstractSocketClient;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseSocketFactory;
import com.clickhouse.client.ClickHouseSslContextProvider;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseProxyType;
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
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.DefaultHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.Timeout;
import org.apache.hc.core5.util.VersionInfo;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by wujianchao on 2022/12/1.
 */
public class ApacheHttpConnectionImpl extends ClickHouseHttpConnection {
    private static final Logger log = LoggerFactory.getLogger(ApacheHttpConnectionImpl.class);

    private final CloseableHttpClient client;

    protected ApacheHttpConnectionImpl(ClickHouseNode server, ClickHouseRequest<?> request, ExecutorService executor)
            throws IOException {
        super(server, request);

        client = newConnection(config);
    }

    private CloseableHttpClient newConnection(ClickHouseConfig c) throws IOException {
        final ClickHouseSocketFactory socketFactory = AbstractSocketClient.getCustomSocketFactory(
                c.getCustomSocketFactory(), ApacheHttpClientSocketFactory.instance, PlainConnectionSocketFactory.class);

        RegistryBuilder<ConnectionSocketFactory> r = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", socketFactory.create(c, PlainConnectionSocketFactory.class));
        if (c.isSsl()) {
            r.register("https", socketFactory.create(c, SSLConnectionSocketFactory.class));
        }

        HttpConnectionManager connManager = new HttpConnectionManager(r.build(), c);
        int maxConnection = config.getIntOption(ClickHouseHttpOption.MAX_OPEN_CONNECTIONS);

        connManager.setMaxTotal(maxConnection);
        connManager.setDefaultMaxPerRoute(maxConnection);

        HttpClientBuilder builder = HttpClientBuilder.create().setConnectionManager(connManager)
                .disableContentCompression();
        if (c.getProxyType() == ClickHouseProxyType.HTTP) {
            builder.setProxy(new HttpHost(c.getProxyHost(), c.getProxyPort()));
        }
        return builder.build();
    }

    private ClickHouseHttpResponse buildResponse(ClickHouseConfig config, CloseableHttpResponse response,
            ClickHouseOutputStream output, Runnable postCloseAction) throws IOException {
        // X-ClickHouse-Server-Display-Name: xxx
        // X-ClickHouse-Query-Id: xxx
        // X-ClickHouse-Format: RowBinaryWithNamesAndTypes
        // X-ClickHouse-Timezone: UTC
        // X-ClickHouse-Summary:
        // {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
        String displayName = getResponseHeader(response, "X-ClickHouse-Server-Display-Name", server.getHost());
        String queryId = getResponseHeader(response, "X-ClickHouse-Query-Id", "");
        String summary = getResponseHeader(response, "X-ClickHouse-Summary", "{}");

        ClickHouseFormat format = config.getFormat();
        TimeZone timeZone = config.getServerTimeZone();
        boolean hasCustomOutput = output != null && output.getUnderlyingStream().hasOutput();
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
                    ClickHouseInputStream.pipe(response.getEntity().getContent(), o, config.getWriteBufferSize());
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
                hasCustomOutput ? ClickHouseInputStream.of(source, config.getReadBufferSize(), action)
                        : (hasQueryResult ? ClickHouseClient.getAsyncResponseInputStream(config, source, action)
                                : ClickHouseClient.getResponseInputStream(config, source, action)),
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

    private void checkResponse(ClickHouseConfig config, CloseableHttpResponse response) throws IOException {
        if (response.getCode() == HttpURLConnection.HTTP_OK) {
            return;
        }

        final Header errorCode = response.getFirstHeader("X-ClickHouse-Exception-Code");
        final Header serverName = response.getFirstHeader("X-ClickHouse-Server-Display-Name");
        if (response.getEntity() == null) {
            throw new ConnectException(
                    ClickHouseUtils.format("HTTP response %d %s(code %s returned from server %s)",
                            response.getCode(), response.getReasonPhrase(),
                            errorCode == null ? null : errorCode.getValue(),
                            serverName == null ? null : serverName.getValue()));
        }

        String errorMsg;

        int bufferSize = config.getReadBufferSize();
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
            errorMsg = parseErrorFromException(errorCode != null ? errorCode.getValue() : null,
                    serverName != null ? serverName.getValue() : null, e, bytes);
        }
        throw new IOException(errorMsg);
    }

    @Override
    protected final String getDefaultUserAgent() {
        return HttpConnectionManager.USER_AGENT;
    }

    @Override
    protected boolean isReusable() {
        return true;
    }

    @Override
    protected ClickHouseHttpResponse post(ClickHouseConfig config, String sql, ClickHouseInputStream data,
            List<ClickHouseExternalTable> tables, ClickHouseOutputStream output, String url,
            Map<String, String> headers, Runnable postCloseAction) throws IOException {
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

        String contentEncoding = headers == null ? null : headers.getOrDefault("content-encoding", null);
        ClickHouseHttpEntity postBody = new ClickHouseHttpEntity(config, contentType, contentEncoding, boundary,
                sql, data, tables);
        post.setEntity(postBody);
        CloseableHttpResponse response = client.execute(post);

        checkResponse(config, response);
        // buildResponse should use the config of current request in case of reusable
        // connection.
        return buildResponse(config, response, output, postCloseAction);
    }

    @Override
    public boolean ping(int timeout) {
        String url = getBaseUrl().concat("ping");
        HttpGet ping = new HttpGet(url);

        ClickHouseConfig c = config;
        try (CloseableHttpClient httpClient = newConnection(c);
                CloseableHttpResponse response = httpClient.execute(ping)) {
            // TODO set timeout
            checkResponse(c, response);
            String ok = c.getStrOption(ClickHouseHttpOption.DEFAULT_RESPONSE);
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

    static class ApacheHttpClientSocketFactory implements ClickHouseSocketFactory {
        static final ApacheHttpClientSocketFactory instance = new ApacheHttpClientSocketFactory();

        @Override
        public <T> T create(ClickHouseConfig config, Class<T> clazz) throws IOException, UnsupportedOperationException {
            if (config == null || clazz == null) {
                throw new IllegalArgumentException("Non-null configuration and class are required");
            } else if (SSLConnectionSocketFactory.class.equals(clazz)) {
                return clazz.cast(new SSLSocketFactory(config));
            } else if (PlainConnectionSocketFactory.class.equals(clazz)) {
                return clazz.cast(new SocketFactory(config));
            }

            throw new UnsupportedOperationException(ClickHouseUtils.format("Class %s is not supported", clazz));
        }

        @Override
        public boolean supports(Class<?> clazz) {
            return PlainConnectionSocketFactory.class.equals(clazz) || SSLConnectionSocketFactory.class.equals(clazz);
        }

        private ApacheHttpClientSocketFactory() {
        }
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
            super(ClickHouseSslContextProvider.getProvider().getSslContext(SSLContext.class, config)
                    .orElse(SSLContexts.createDefault()),
                    config.getSslMode() == ClickHouseSslMode.STRICT
                            ? new DefaultHostnameVerifier()
                            : (hostname, session) -> true); // NOSONAR
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
        private static final String PROVIDER = "Apache-HttpClient";
        private static final String USER_AGENT;

        static {
            String versionInfo = null;
            try {
                String pkg = VersionInfo.class.getPackage().getName();
                pkg = pkg.substring(0, pkg.lastIndexOf('.'));
                versionInfo = VersionInfo.getSoftwareInfo(PROVIDER, pkg, HttpClientBuilder.class).split("\\s")[0];
            } catch (Throwable e) { // NOSONAR
                // ignore
            }

            USER_AGENT = ClickHouseClientOption.buildUserAgent(null,
                    versionInfo != null && !versionInfo.isEmpty() ? versionInfo : PROVIDER);
        }

        public HttpConnectionManager(Registry<ConnectionSocketFactory> socketFactory, ClickHouseConfig config) {
            super(socketFactory);

            ConnectionConfig connConfig = ConnectionConfig.custom()
                    .setConnectTimeout(Timeout.of(config.getConnectionTimeout(), TimeUnit.MILLISECONDS))
                    .build();
            setDefaultConnectionConfig(connConfig);

            SocketConfig.Builder builder = SocketConfig.custom()
                    .setSoTimeout(Timeout.of(config.getSocketTimeout(), TimeUnit.MILLISECONDS))
                    .setRcvBufSize(config.getReadBufferSize())
                    .setSndBufSize(config.getWriteBufferSize());
            if (config.hasOption(ClickHouseClientOption.SOCKET_KEEPALIVE)) {
                builder.setSoKeepAlive(config.getBoolOption(ClickHouseClientOption.SOCKET_KEEPALIVE));
            }
            if (config.hasOption(ClickHouseClientOption.SOCKET_LINGER)) {
                int solinger = config.getIntOption(ClickHouseClientOption.SOCKET_LINGER);
                builder.setSoLinger(solinger, TimeUnit.SECONDS);
            }
            if (config.hasOption(ClickHouseClientOption.SOCKET_REUSEADDR)) {
                builder.setSoReuseAddress(config.getBoolOption(ClickHouseClientOption.SOCKET_REUSEADDR));
            }
            if (config.hasOption(ClickHouseClientOption.SOCKET_TCP_NODELAY)) {
                builder.setTcpNoDelay(config.getBoolOption(ClickHouseClientOption.SOCKET_TCP_NODELAY));
            }
            if (config.getProxyType() == ClickHouseProxyType.SOCKS) {
                builder.setSocksProxyAddress(new InetSocketAddress(config.getProxyHost(), config.getProxyPort()));
            }
            if (config.hasOption(ClickHouseClientOption.SOCKET_RCVBUF)) {
                int bufferSize = config.getIntOption(ClickHouseClientOption.SOCKET_RCVBUF);
                builder.setRcvBufSize(bufferSize > 0 ? bufferSize : config.getReadBufferSize());
            } else {
                int bufferSize = config.getBufferSize();
                int maxQueuedBuffers = config.getMaxQueuedBuffers();
                builder.setRcvBufSize(bufferSize * maxQueuedBuffers);
            }
            if (config.hasOption(ClickHouseClientOption.SOCKET_SNDBUF)) {
                int bufferSize = config.getIntOption(ClickHouseClientOption.SOCKET_SNDBUF);
                builder.setSndBufSize(bufferSize > 0 ? bufferSize : config.getWriteBufferSize());
            } else {
                int bufferSize = config.getBufferSize();
                int maxQueuedBuffers = config.getMaxQueuedBuffers();
                builder.setSndBufSize(bufferSize * maxQueuedBuffers);
            }
            setDefaultSocketConfig(builder.build());
        }
    }
}
