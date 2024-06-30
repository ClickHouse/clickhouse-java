package com.clickhouse.client.http;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseSslContextProvider;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseExternalTable;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.Serializable;
import java.util.Map;

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

    private static final String USER_AGENT = ClickHouseClientOption.buildUserAgent(null, "HttpClient");

    private final HttpClient httpClient;
    private final HttpRequest pingRequest;

    private ClickHouseHttpResponse buildResponse(ClickHouseConfig config, HttpResponse<InputStream> r,
            ClickHouseOutputStream output, Runnable postAction) throws IOException {
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

        boolean hasCustomOutput = output != null && output.getUnderlyingStream().hasOutput();
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
                hasCustomOutput ? ClickHouseInputStream.of(source, config.getReadBufferSize(), action)
                        : ClickHouseInputStream.wrap(null, source, config.getReadBufferSize(),
                                config.getResponseCompressAlgorithm(), config.getResponseCompressLevel(), action),
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
                errorMsg = parseErrorFromException(errorCode, serverName, e, bytes);
            }

            throw new IOException(errorMsg);
        }

        return r;
    }

    private HttpRequest newRequest(String url) {
        return HttpRequest.newBuilder().uri(URI.create(url)).version(Version.HTTP_1_1)
                .timeout(Duration.ofMillis(config.getSocketTimeout())).build();
    }

    protected HttpClientConnectionImpl(ClickHouseNode server, ClickHouseRequest<?> request, ExecutorService executor,
                                       Map<String, Serializable> additionalParams) throws IOException {
        super(server, request, additionalParams);

        HttpClient.Builder builder = HttpClient.newBuilder().version(Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(config.getConnectionTimeout())).followRedirects(Redirect.NORMAL);
        if (executor != null) {
            builder.executor(executor);
        }
        ClickHouseProxyType proxyType = config.getProxyType();
        if (proxyType == ClickHouseProxyType.DIRECT) {
            builder.proxy(NoProxySelector.INSTANCE);
        } else if (proxyType == ClickHouseProxyType.HTTP) {
            builder.proxy(ProxySelector.of(new InetSocketAddress(config.getProxyHost(), config.getProxyPort())));
        } else if (proxyType != ClickHouseProxyType.IGNORE) {
            throw new IllegalArgumentException(
                    "Only HTTP(s) proxy is supported by HttpClient but we got: " + proxyType);
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
        return true; // httpClient is stateless and can be reused
    }

    private CompletableFuture<HttpResponse<InputStream>> postRequest(HttpRequest request) {
        // either change system property jdk.httpclient.keepalive.timeout or increase
        // keep_alive_timeout on server
        return httpClient.sendAsync(request,
                responseInfo -> new ClickHouseResponseHandler(config.getMaxQueuedBuffers(), config.getSocketTimeout()));
    }

    private ClickHouseHttpResponse postStream(ClickHouseConfig config, HttpRequest.Builder reqBuilder, byte[] boundary,
            String sql, ClickHouseInputStream data, List<ClickHouseExternalTable> tables, ClickHouseOutputStream output,
            Runnable postAction) throws IOException {

            ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                    .createPipedOutputStream(config);
            reqBuilder.POST(HttpRequest.BodyPublishers.ofInputStream(stream::getInputStream));
            // running in async is necessary to avoid deadlock of the piped stream
            CompletableFuture<HttpResponse<InputStream>> f = postRequest(reqBuilder.build());

            postData(config, boundary, sql, data, tables, stream);

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

            return buildResponse(config, r, output, postAction);
    }

    private ClickHouseHttpResponse postString(ClickHouseConfig config, HttpRequest.Builder reqBuilder, String sql,
            ClickHouseOutputStream output, Runnable postAction) throws IOException {

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
            return buildResponse(config, r, output, postAction);
    }

    @Override
    protected final String getDefaultUserAgent() {
        return USER_AGENT;
    }

    @Override
    protected ClickHouseHttpResponse post(ClickHouseConfig config, String sql, ClickHouseInputStream data,
            List<ClickHouseExternalTable> tables, ClickHouseOutputStream output, String url,
            Map<String, String> headers, Runnable postAction) throws IOException {

        ClickHouseConfig c = config == null ? this.config : config;
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(ClickHouseChecker.isNullOrEmpty(url) ? this.url : url))
                .timeout(Duration.ofMillis(c.getSocketTimeout()));
        byte[] boundary = null;
        if (tables != null && !tables.isEmpty()) {
            String uuid = rm.createUniqueId();
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

        return boundary != null || data != null || c.isRequestCompressed()
                ? postStream(c, reqBuilder, boundary, sql, data, tables, output, postAction)
                : postString(c, reqBuilder, sql, output, postAction);
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
