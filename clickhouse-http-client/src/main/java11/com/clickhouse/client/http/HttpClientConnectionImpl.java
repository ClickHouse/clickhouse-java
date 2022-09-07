package com.clickhouse.client.http;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseDataStreamFactory;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseOutputStream;
import com.clickhouse.client.ClickHousePipedOutputStream;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseSslContextProvider;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpClient.Version;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import javax.net.ssl.SSLContext;

public class HttpClientConnectionImpl extends ClickHouseHttpConnection {
    static class NoProxySelector extends ProxySelector {
        static final NoProxySelector INSTANCE = new NoProxySelector();

        private static final List<Proxy> NO_PROXY_LIST = List.of(Proxy.NO_PROXY);

        private NoProxySelector() {
        }

        @Override
        public void connectFailed(URI uri, SocketAddress sa, IOException e) {
            // ignore
        }

        @Override
        public List<Proxy> select(URI uri) {
            return NO_PROXY_LIST;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(HttpClientConnectionImpl.class);

    private static final byte[] HEADER_CONTENT_DISPOSITION = "content-disposition: form-data; name=\""
            .getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HEADER_OCTET_STREAM = "content-type: application/octet-stream\r\n"
            .getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HEADER_BINARY_ENCODING = "content-transfer-encoding: binary\r\n\r\n"
            .getBytes(StandardCharsets.US_ASCII);

    private static final byte[] DOUBLE_DASH = new byte[] { '-', '-' };
    private static final byte[] END_OF_NAME = new byte[] { '"', '\r', '\n' };
    private static final byte[] LINE_PREFIX = new byte[] { '\r', '\n', '-', '-' };
    private static final byte[] LINE_SUFFIX = new byte[] { '\r', '\n' };

    private static final byte[] SUFFIX_QUERY = "query\"\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUFFIX_FORMAT = "_format\"\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUFFIX_STRUCTURE = "_structure\"\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUFFIX_FILENAME = "\"; filename=\"".getBytes(StandardCharsets.US_ASCII);

    private final HttpClient httpClient;
    private final HttpRequest pingRequest;

    private ClickHouseHttpResponse buildResponse(ClickHouseConfig config, HttpResponse<InputStream> r,
            Runnable postAction) throws IOException {
        HttpHeaders headers = r.headers();
        String displayName = headers.firstValue("X-ClickHouse-Server-Display-Name").orElse(server.getHost());
        String queryId = headers.firstValue("X-ClickHouse-Query-Id").orElse("");
        String summary = headers.firstValue("X-ClickHouse-Summary").orElse("{}");

        ClickHouseFormat format = config.getFormat();
        TimeZone timeZone = config.getServerTimeZone();
        // queryId, format and timeZone are only available for queries
        if (!ClickHouseChecker.isNullOrEmpty(queryId)) {
            String value = headers.firstValue("X-ClickHouse-Format").orElse("");
            format = !ClickHouseChecker.isNullOrEmpty(value) ? ClickHouseFormat.valueOf(value)
                    : format;
            value = headers.firstValue("X-ClickHouse-Timezone").orElse("");
            timeZone = !ClickHouseChecker.isNullOrEmpty(value) ? TimeZone.getTimeZone(value)
                    : timeZone;
        }

        boolean hasOutputFile = output != null && output.getUnderlyingFile().isAvailable();
        final InputStream source;
        final Runnable action;
        if (output != null) {
            source = ClickHouseInputStream.empty();
            action = () -> {
                try (OutputStream o = output) {
                    ClickHouseInputStream.pipe(checkResponse(config, r).body(), o, config.getWriteBufferSize());
                    if (postAction != null) {
                        postAction.run();
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to redirect response to given output stream", e);
                } finally {
                    closeQuietly();
                }
            };
        } else {
            source = checkResponse(config, r).body();
            action = () -> {
                if (postAction != null) {
                    postAction.run();
                }
                closeQuietly();
            };
        }

        return new ClickHouseHttpResponse(this,
                hasOutputFile ? ClickHouseInputStream.of(source, config.getReadBufferSize(), action)
                        : ClickHouseInputStream.wrap(null, source, config.getReadBufferSize(), action,
                                config.getResponseCompressAlgorithm(), config.getResponseCompressLevel()),
                displayName, queryId, summary, format, timeZone);
    }

    private HttpResponse<InputStream> checkResponse(ClickHouseConfig config, HttpResponse<InputStream> r)
            throws IOException {
        if (r.statusCode() != HttpURLConnection.HTTP_OK) {
            String errorCode = r.headers().firstValue("X-ClickHouse-Exception-Code").orElse("");
            // String encoding = r.headers().firstValue("Content-Encoding");
            String serverName = r.headers().firstValue("X-ClickHouse-Server-Display-Name").orElse("");

            String errorMsg;
            int bufferSize = (int) ClickHouseClientOption.BUFFER_SIZE.getDefaultValue();
            ByteArrayOutputStream output = new ByteArrayOutputStream(bufferSize);
            ClickHouseInputStream.pipe(r.body(), output, bufferSize);
            byte[] bytes = output.toByteArray();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    ClickHouseClient.getResponseInputStream(config, new ByteArrayInputStream(bytes),
                            this::closeQuietly),
                    StandardCharsets.UTF_8))) {
                StringBuilder builder = new StringBuilder();
                while ((errorMsg = reader.readLine()) != null) {
                    builder.append(errorMsg).append('\n');
                }
                errorMsg = builder.toString();
            } catch (IOException e) {
                log.debug("Failed to read error message[code=%s] from server [%s] due to: %s", errorCode, serverName,
                        e.getMessage());
                errorMsg = new String(bytes, StandardCharsets.UTF_8);
            }

            throw new IOException(errorMsg);
        }

        return r;
    }

    private HttpRequest newRequest(String url) {
        return HttpRequest.newBuilder().uri(URI.create(url)).version(Version.HTTP_1_1)
                .timeout(Duration.ofMillis(config.getSocketTimeout())).build();
    }

    protected HttpClientConnectionImpl(ClickHouseNode server, ClickHouseRequest<?> request, ExecutorService executor)
            throws IOException {
        super(server, request);

        HttpClient.Builder builder = HttpClient.newBuilder().version(Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(config.getConnectionTimeout())).followRedirects(Redirect.NORMAL);
        if (executor != null) {
            builder.executor(executor);
        }
        if (config.isUseNoProxy()) {
            builder.proxy(NoProxySelector.INSTANCE);
        }
        if (config.isSsl()) {
            builder.sslContext(ClickHouseSslContextProvider.getProvider().getSslContext(SSLContext.class, config)
                    .orElse(null));
        }

        httpClient = builder.build();
        pingRequest = newRequest(getBaseUrl() + "ping");
    }

    @Override
    protected boolean isReusable() {
        return true;
    }

    private CompletableFuture<HttpResponse<InputStream>> postRequest(HttpRequest request) {
        // either change system property jdk.httpclient.keepalive.timeout or increase
        // keep_alive_timeout on server
        return httpClient.sendAsync(request,
                responseInfo -> new ClickHouseResponseHandler(config.getMaxQueuedBuffers(), config.getSocketTimeout()));
    }

    private ClickHouseHttpResponse postStream(ClickHouseConfig config, HttpRequest.Builder reqBuilder, byte[] boundary,
            String sql, ClickHouseInputStream data, List<ClickHouseExternalTable> tables, Runnable postAction)
            throws IOException {
        Charset ascii = StandardCharsets.US_ASCII;
        ClickHouseConfig c = config;
        final boolean hasFile = data != null && data.getUnderlyingFile().isAvailable();

        ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(c,
                null);
        reqBuilder.POST(HttpRequest.BodyPublishers.ofInputStream(stream::getInputStream));
        // running in async is necessary to avoid deadlock of the piped stream
        CompletableFuture<HttpResponse<InputStream>> f = postRequest(reqBuilder.build());

        try (ClickHouseOutputStream out = stream) {
            Charset utf8 = StandardCharsets.UTF_8;
            byte[] sqlBytes = hasFile ? new byte[0] : sql.getBytes(utf8);

            if (boundary != null) {
                out.writeBytes(LINE_PREFIX);
                out.writeBytes(boundary);
                out.writeBytes(LINE_SUFFIX);
                out.writeBytes(HEADER_CONTENT_DISPOSITION);
                out.writeBytes(SUFFIX_QUERY);
                out.writeBytes(sqlBytes);
                for (ClickHouseExternalTable t : tables) {
                    byte[] tableName = t.getName().getBytes(utf8);
                    for (int i = 0; i < 3; i++) {
                        out.writeBytes(LINE_PREFIX);
                        out.writeBytes(boundary);
                        out.writeBytes(LINE_SUFFIX);
                        out.writeBytes(HEADER_CONTENT_DISPOSITION);
                        out.writeBytes(tableName);
                        if (i == 0) {
                            out.writeBytes(SUFFIX_FORMAT);
                            out.writeBytes(t.getFormat().name().getBytes(ascii));
                        } else if (i == 1) {
                            out.writeBytes(SUFFIX_STRUCTURE);
                            out.writeBytes(t.getStructure().getBytes(utf8));
                        } else {
                            out.writeBytes(SUFFIX_FILENAME);
                            out.writeBytes(tableName);
                            out.writeBytes(END_OF_NAME);
                            break;
                        }
                    }
                    out.writeBytes(HEADER_OCTET_STREAM);
                    out.writeBytes(HEADER_BINARY_ENCODING);
                    ClickHouseInputStream.pipe(t.getContent(), out, c.getWriteBufferSize());
                }

                out.writeBytes(LINE_PREFIX);
                out.writeBytes(boundary);
                out.writeBytes(DOUBLE_DASH);
                out.writeBytes(LINE_SUFFIX);
            } else {
                out.writeBytes(sqlBytes);
                if (data != null && data.available() > 0) {
                    // append \n
                    if (sqlBytes.length > 0 && sqlBytes[sqlBytes.length - 1] != (byte) '\n') {
                        out.write(10);
                    }
                    ClickHouseInputStream.pipe(data, out, c.getWriteBufferSize());
                }
            }
        }

        HttpResponse<InputStream> r;
        try {
            r = f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread was interrupted when posting request or receiving response", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof HttpConnectTimeoutException) {
                throw new ConnectException(cause.getMessage());
            } else {
                throw new IOException("Failed to post request", cause);
            }
        }

        return buildResponse(config, r, postAction);
    }

    private ClickHouseHttpResponse postString(ClickHouseConfig config, HttpRequest.Builder reqBuilder, String sql,
            Runnable postAction) throws IOException {
        reqBuilder.POST(HttpRequest.BodyPublishers.ofString(sql));
        HttpResponse<InputStream> r;
        try {
            r = postRequest(reqBuilder.build()).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread was interrupted when posting request or receiving response", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof HttpConnectTimeoutException) {
                throw new ConnectException(cause.getMessage());
            } else {
                throw new IOException("Failed to post query", cause);
            }
        }
        return buildResponse(config, r, postAction);
    }

    @Override
    protected ClickHouseHttpResponse post(String sql, ClickHouseInputStream data, List<ClickHouseExternalTable> tables,
            String url, Map<String, String> headers, ClickHouseConfig config, Runnable postAction) throws IOException {
        ClickHouseConfig c = config == null ? this.config : config;
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(ClickHouseChecker.isNullOrEmpty(url) ? this.url : url))
                .timeout(Duration.ofMillis(c.getSocketTimeout()));
        byte[] boundary = null;
        if (tables != null && !tables.isEmpty()) {
            String uuid = UUID.randomUUID().toString();
            reqBuilder.setHeader("content-type", "multipart/form-data; boundary=" + uuid);
            boundary = uuid.getBytes(StandardCharsets.US_ASCII);
        } else {
            reqBuilder.setHeader("content-type", "text/plain; charset=UTF-8");
        }

        headers = mergeHeaders(headers);
        if (headers != null && !headers.isEmpty()) {
            for (Entry<String, String> header : headers.entrySet()) {
                reqBuilder.setHeader(header.getKey(), header.getValue());
            }
        }

        return boundary != null || data != null ? postStream(c, reqBuilder, boundary, sql, data, tables, postAction)
                : postString(c, reqBuilder, sql, postAction);
    }

    @Override
    public boolean ping(int timeout) {
        String response = config.getStrOption(ClickHouseHttpOption.DEFAULT_RESPONSE);
        try {
            HttpResponse<String> r = httpClient.send(pingRequest, HttpResponse.BodyHandlers.ofString());
            return r.statusCode() == HttpURLConnection.HTTP_OK && response.equals(r.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            log.debug("Failed to ping server: %s", e.getMessage());
        }

        return false;
    }

    @Override
    public void close() {
        // nothing
    }
}
