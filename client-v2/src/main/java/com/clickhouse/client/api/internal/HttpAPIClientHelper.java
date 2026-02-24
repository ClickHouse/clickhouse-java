package com.clickhouse.client.api.internal;

import com.clickhouse.client.ClickHouseSslContextProvider;
import com.clickhouse.client.api.ClickHouseException;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ClientFaultCause;
import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.client.api.ConnectionInitiationException;
import com.clickhouse.client.api.ConnectionReuseStrategy;
import com.clickhouse.client.api.DataTransferException;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.enums.ProxyType;
import com.clickhouse.client.api.http.ClickHouseHttpProto;
import com.clickhouse.client.api.transport.Endpoint;
import com.clickhouse.data.ClickHouseFormat;
import net.jpountz.lz4.LZ4Factory;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hc.client5.http.ConnectTimeoutException;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.entity.mime.MultipartPartBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.BasicHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.ManagedHttpClientConnectionFactory;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.io.ManagedHttpClientConnection;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.socket.LayeredConnectionSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ConnectionRequestTimeoutException;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.NoHttpResponseException;
import org.apache.hc.core5.http.config.CharCodingConfig;
import org.apache.hc.core5.http.config.Http1Config;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.http.impl.io.DefaultHttpResponseParserFactory;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityTemplate;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.io.IOCallback;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.pool.ConnPoolControl;
import org.apache.hc.core5.pool.PoolConcurrencyPolicy;
import org.apache.hc.core5.pool.PoolReusePolicy;
import org.apache.hc.core5.util.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Pattern;

public class HttpAPIClientHelper {

    public static final String KEY_STATEMENT_PARAMS = "statement_params";

    private static final Logger LOG = LoggerFactory.getLogger(HttpAPIClientHelper.class);

    private static final int ERROR_BODY_BUFFER_SIZE = 1024; // Error messages are usually small

    private final String DEFAULT_HTTP_COMPRESSION_ALGO = "lz4";

    private static final Pattern PATTERN_HEADER_VALUE_ASCII = Pattern.compile(
        "\\p{Graph}+(?:[ ]\\p{Graph}+)*");

    private final CloseableHttpClient httpClient;

    private String proxyAuthHeaderValue;

    private final Set<ClientFaultCause> defaultRetryCauses;

    private final String defaultUserAgent;
    private final Object metricsRegistry;

    ConnPoolControl<?> poolControl;

    LZ4Factory lz4Factory;

    public HttpAPIClientHelper(Map<String, Object> configuration, Object metricsRegistry, boolean initSslContext, LZ4Factory lz4Factory) {
        this.metricsRegistry = metricsRegistry;
        this.httpClient = createHttpClient(initSslContext, configuration);
        this.lz4Factory = lz4Factory;
        assert this.lz4Factory != null;

        boolean usingClientCompression = ClientConfigProperties.COMPRESS_CLIENT_REQUEST.getOrDefault(configuration);
        boolean usingServerCompression = ClientConfigProperties.COMPRESS_SERVER_RESPONSE.getOrDefault(configuration);
        boolean useHttpCompression = ClientConfigProperties.USE_HTTP_COMPRESSION.getOrDefault(configuration);

        LOG.debug("client compression: {}, server compression: {}, http compression: {}", usingClientCompression, usingServerCompression, useHttpCompression);

        defaultRetryCauses = new HashSet<>(ClientConfigProperties.CLIENT_RETRY_ON_FAILURE.getOrDefault(configuration));
        if (defaultRetryCauses.contains(ClientFaultCause.None)) {
            defaultRetryCauses.removeIf(c -> c != ClientFaultCause.None);
        }

        this.defaultUserAgent = buildDefaultUserAgent();
    }

    /**
     * Creates or returns default SSL context.
     *
     * @return SSLContext
     */
    public SSLContext createSSLContext(Map<String, Object> configuration) {
        SSLContext sslContext;
        try {
            sslContext = SSLContext.getDefault();
        } catch (NoSuchAlgorithmException e) {
            throw new ClientException("Failed to create default SSL context", e);
        }
        ClickHouseSslContextProvider sslContextProvider = ClickHouseSslContextProvider.getProvider();
        String trustStorePath = (String) configuration.get(ClientConfigProperties.SSL_TRUST_STORE.getKey());
        if (trustStorePath != null) {
            try {
                sslContext = sslContextProvider.getSslContextFromKeyStore(
                        trustStorePath,
                        (String) configuration.get(ClientConfigProperties.SSL_KEY_STORE_PASSWORD.getKey()),
                        (String) configuration.get(ClientConfigProperties.SSL_KEYSTORE_TYPE.getKey())
                );
            } catch (SSLException e) {
                throw new ClientMisconfigurationException("Failed to create SSL context from a keystore", e);
            }
        } else if (configuration.get(ClientConfigProperties.CA_CERTIFICATE.getKey()) != null ||
                configuration.get(ClientConfigProperties.SSL_CERTIFICATE.getKey()) != null ||
                configuration.get(ClientConfigProperties.SSL_KEY.getKey()) != null) {

            try {
                sslContext = sslContextProvider.getSslContextFromCerts(
                        (String) configuration.get(ClientConfigProperties.SSL_CERTIFICATE.getKey()),
                        (String) configuration.get(ClientConfigProperties.SSL_KEY.getKey()),
                        (String) configuration.get(ClientConfigProperties.CA_CERTIFICATE.getKey())
                );
            } catch (SSLException e) {
                throw new ClientMisconfigurationException("Failed to create SSL context from certificates", e);
            }
        }
        return sslContext;
    }

    private static final long CONNECTION_INACTIVITY_CHECK = 5000L;

    private ConnectionConfig createConnectionConfig(Map<String, Object> configuration) {
        ConnectionConfig.Builder connConfig = ConnectionConfig.custom();

        ClientConfigProperties.CONNECTION_TTL.<Long>applyIfSet(configuration, (t) -> connConfig.setTimeToLive(t,  TimeUnit.MILLISECONDS));
        ClientConfigProperties.CONNECTION_TIMEOUT.<Long>applyIfSet(configuration, (t) -> connConfig.setConnectTimeout(t,  TimeUnit.MILLISECONDS));
        connConfig.setValidateAfterInactivity(CONNECTION_INACTIVITY_CHECK, TimeUnit.MILLISECONDS); // non-configurable for now

        return connConfig.build();
    }

    private HttpClientConnectionManager basicConnectionManager(LayeredConnectionSocketFactory sslConnectionSocketFactory, SocketConfig socketConfig, Map<String, Object> configuration) {
        RegistryBuilder<ConnectionSocketFactory> registryBuilder = RegistryBuilder.create();
        registryBuilder.register("http", PlainConnectionSocketFactory.getSocketFactory());
        registryBuilder.register("https", sslConnectionSocketFactory);

        BasicHttpClientConnectionManager connManager = new BasicHttpClientConnectionManager(registryBuilder.build());
        connManager.setConnectionConfig(createConnectionConfig(configuration));
        connManager.setSocketConfig(socketConfig);

        return connManager;
    }

    private HttpClientConnectionManager poolConnectionManager(LayeredConnectionSocketFactory sslConnectionSocketFactory, SocketConfig socketConfig, Map<String, Object> configuration) {
        PoolingHttpClientConnectionManagerBuilder connMgrBuilder = PoolingHttpClientConnectionManagerBuilder.create()
                .setPoolConcurrencyPolicy(PoolConcurrencyPolicy.LAX);

        ConnectionReuseStrategy connectionReuseStrategy = ClientConfigProperties.CONNECTION_REUSE_STRATEGY.getOrDefault(configuration);
        switch (connectionReuseStrategy) {
            case LIFO:
                connMgrBuilder.setConnPoolPolicy(PoolReusePolicy.LIFO);
                break;
            case FIFO:
                connMgrBuilder.setConnPoolPolicy(PoolReusePolicy.FIFO);
                break;
            default:
                throw new ClientMisconfigurationException("Unknown connection reuse strategy: " + connectionReuseStrategy);
        }
        LOG.debug("Connection reuse strategy: {}", connectionReuseStrategy);

        connMgrBuilder.setDefaultConnectionConfig(createConnectionConfig(configuration));
        connMgrBuilder.setMaxConnTotal(Integer.MAX_VALUE); // as we do not know how many routes we will have
        ClientConfigProperties.HTTP_MAX_OPEN_CONNECTIONS.applyIfSet(configuration, connMgrBuilder::setMaxConnPerRoute);

        int networkBufferSize = ClientConfigProperties.CLIENT_NETWORK_BUFFER_SIZE.getOrDefault(configuration);
        MeteredManagedHttpClientConnectionFactory connectionFactory = new MeteredManagedHttpClientConnectionFactory(
                Http1Config.custom()
                        .setBufferSize(networkBufferSize)
                        .build(),
                CharCodingConfig.DEFAULT,
                DefaultHttpResponseParserFactory.INSTANCE);

        connMgrBuilder.setConnectionFactory(connectionFactory);
        connMgrBuilder.setSSLSocketFactory(sslConnectionSocketFactory);
        connMgrBuilder.setDefaultSocketConfig(socketConfig);
        PoolingHttpClientConnectionManager phccm = connMgrBuilder.build();
        poolControl = phccm;
        if (metricsRegistry != null) {
            try {
                String mGroupName = ClientConfigProperties.METRICS_GROUP_NAME.getOrDefault(configuration);
                Class<?> micrometerLoader = getClass().getClassLoader().loadClass("com.clickhouse.client.api.metrics.MicrometerLoader");
                Method applyMethod = micrometerLoader.getDeclaredMethod("applyPoolingMetricsBinder", Object.class, String.class, PoolingHttpClientConnectionManager.class);
                applyMethod.invoke(micrometerLoader, metricsRegistry, mGroupName, phccm);

                applyMethod = micrometerLoader.getDeclaredMethod("applyConnectionMetricsBinder", Object.class, String.class, MeteredManagedHttpClientConnectionFactory.class);
                applyMethod.invoke(micrometerLoader, metricsRegistry, mGroupName, connectionFactory);
            } catch (Exception e) {
                LOG.error("Failed to register metrics", e);
            }
        }
        return phccm;
    }

    public CloseableHttpClient createHttpClient(boolean initSslContext, Map<String, Object> configuration) {
        // Top Level builders
        HttpClientBuilder clientBuilder = HttpClientBuilder.create();
        SSLContext sslContext = initSslContext ? createSSLContext(configuration) : null;
        LayeredConnectionSocketFactory sslConnectionSocketFactory;
        if (sslContext != null) {
            String socketSNI = (String)configuration.get(ClientConfigProperties.SSL_SOCKET_SNI.getKey());
            if (socketSNI != null && !socketSNI.trim().isEmpty()) {
                sslConnectionSocketFactory = new CustomSSLConnectionFactory(socketSNI, sslContext, (hostname, session) -> true);
            } else {
                sslConnectionSocketFactory = new SSLConnectionSocketFactory(sslContext);
            }
        } else {
            sslConnectionSocketFactory = new DummySSLConnectionSocketFactory();
        }
        // Socket configuration
        SocketConfig.Builder soCfgBuilder = SocketConfig.custom();
        ClientConfigProperties.SOCKET_OPERATION_TIMEOUT.<Integer>applyIfSet(configuration,
                (t) -> soCfgBuilder.setSoTimeout(t, TimeUnit.MILLISECONDS));

        ClientConfigProperties.SOCKET_RCVBUF_OPT.applyIfSet(configuration,
                soCfgBuilder::setRcvBufSize);

        ClientConfigProperties.SOCKET_SNDBUF_OPT.applyIfSet(configuration,
                soCfgBuilder::setSndBufSize);

        ClientConfigProperties.SOCKET_LINGER_OPT.<Integer>applyIfSet(configuration,
                (v) -> soCfgBuilder.setSoLinger(v, TimeUnit.SECONDS));

        ClientConfigProperties.SOCKET_TCP_NO_DELAY_OPT.applyIfSet(configuration,
                soCfgBuilder::setTcpNoDelay);
        // Proxy
        String proxyHost = (String) configuration.get(ClientConfigProperties.PROXY_HOST.getKey());
        Integer proxyPort = (Integer) configuration.get(ClientConfigProperties.PROXY_PORT.getKey());
        HttpHost proxy = null;
        if (proxyHost != null && proxyPort != null) {
            proxy = new HttpHost(proxyHost, proxyPort);
        }

        String proxyTypeVal = (String) configuration.get(ClientConfigProperties.PROXY_TYPE.getKey());
        ProxyType proxyType = proxyTypeVal == null ? null : ProxyType.valueOf(proxyTypeVal);
        if (proxyType == ProxyType.HTTP) {
            clientBuilder.setProxy(proxy);
            String proxyUser = (String) configuration.get(ClientConfigProperties.PROXY_USER.getKey());
            String proxyPassword = (String) configuration.get(ClientConfigProperties.PROXY_PASSWORD.getKey());
            if (proxyUser != null && proxyPassword != null) {
                proxyAuthHeaderValue = "Basic " + Base64.getEncoder().encodeToString((proxyUser + ":" + proxyPassword).getBytes(StandardCharsets.UTF_8));
            }

        } else if (proxyType == ProxyType.SOCKS) {
            soCfgBuilder.setSocksProxyAddress(new InetSocketAddress(proxyHost, proxyPort));
        }

        boolean disableCookies = !((Boolean)ClientConfigProperties.HTTP_SAVE_COOKIES.getOrDefault(configuration));
        if (disableCookies) {
            clientBuilder.disableCookieManagement();
        }
        SocketConfig socketConfig = soCfgBuilder.build();

        // Connection manager
        if (ClientConfigProperties.CONNECTION_POOL_ENABLED.<Boolean>getOrDefault(configuration)) {
            clientBuilder.setConnectionManager(poolConnectionManager(sslConnectionSocketFactory, socketConfig, configuration));
        } else {
            clientBuilder.setConnectionManager(basicConnectionManager(sslConnectionSocketFactory, socketConfig, configuration));
        }
        Long keepAliveTimeout = ClientConfigProperties.HTTP_KEEP_ALIVE_TIMEOUT.getOrDefault(configuration);
        if (keepAliveTimeout != null && keepAliveTimeout > 0) {
            clientBuilder.setKeepAliveStrategy((response, context) -> TimeValue.ofMilliseconds(keepAliveTimeout));
        }

        clientBuilder.disableContentCompression(); // will handle ourselves

        return clientBuilder.build();
    }

//    private static final String ERROR_CODE_PREFIX_PATTERN = "Code: %d. DB::Exception:";
    private static final String ERROR_CODE_PREFIX_PATTERN = "%d. DB::Exception:";


    /**
     * Reads status line and if error tries to parse response body to get server error message.
     *
     * @param httpResponse - HTTP response
     * @return exception object with server code
     */
    public Exception readError(ClassicHttpResponse httpResponse) {
        final Header qIdHeader = httpResponse.getFirstHeader(ClickHouseHttpProto.HEADER_QUERY_ID);
        final String queryId = qIdHeader == null ? "" : qIdHeader.getValue();
        int serverCode = getHeaderInt(httpResponse.getFirstHeader(ClickHouseHttpProto.HEADER_EXCEPTION_CODE), 0);
        InputStream body = null;
        try {
            body = httpResponse.getEntity().getContent();
            byte[] buffer = new byte[ERROR_BODY_BUFFER_SIZE];
            byte[] lookUpStr = String.format(ERROR_CODE_PREFIX_PATTERN, serverCode).getBytes(StandardCharsets.UTF_8);
            StringBuilder msgBuilder = new StringBuilder();
            boolean found = false;
            while (true) {
                int rBytes = -1;
                try {
                    rBytes = body.read(buffer);
                } catch (ClientException e) {
                    // Invalid LZ4 Magic
                    if (body instanceof ClickHouseLZ4InputStream) {
                        ClickHouseLZ4InputStream stream = (ClickHouseLZ4InputStream) body;
                        body = stream.getInputStream();
                        byte[] headerBuffer = stream.getHeaderBuffer();
                        System.arraycopy(headerBuffer, 0, buffer, 0, headerBuffer.length);
                        rBytes = headerBuffer.length;
                    }
                }
                if (rBytes == -1) {
                    break;
                }

                for (int i = 0; i < rBytes; i++) {
                    if (buffer[i] == lookUpStr[0]) {
                        found = true;
                        for (int j = 1; j < Math.min(rBytes - i, lookUpStr.length); j++) {
                            if (buffer[i + j] != lookUpStr[j]) {
                                found = false;
                                break;
                            }
                        }
                        if (found) {
                            msgBuilder.append(new String(buffer, i, rBytes - i, StandardCharsets.UTF_8));
                            break;
                        }
                    }
                }

                if (found) {
                    break;
                }
            }

            while (true) {
                int rBytes = body.read(buffer);
                if (rBytes == -1) {
                    break;
                }
                msgBuilder.append(new String(buffer, 0, rBytes, StandardCharsets.UTF_8));
            }

            String msg = msgBuilder.toString().replaceAll("\\s+", " ").replaceAll("\\\\n", " ")
                    .replaceAll("\\\\/", "/");
            if (msg.trim().isEmpty()) {
                msg = String.format(ERROR_CODE_PREFIX_PATTERN, serverCode) + " <Unreadable error message> (transport error: " + httpResponse.getCode() + ")";
            }
            return new ServerException(serverCode, "Code: " + msg + " (queryId= " + queryId + ")", httpResponse.getCode(), queryId);
        } catch (Exception e) {
            LOG.error("Failed to read error message", e);
            String msg = String.format(ERROR_CODE_PREFIX_PATTERN, serverCode) + " <Unreadable error message> (transport error: " + httpResponse.getCode() + ")";
            return new ServerException(serverCode, msg + " (queryId= " + queryId + ")", httpResponse.getCode(), queryId);
        }
    }

    private static final long POOL_VENT_TIMEOUT = 10000L;
    private final AtomicLong timeToPoolVent = new AtomicLong(0);

    private void doPoolVent() {
        if (poolControl != null && timeToPoolVent.get() < System.currentTimeMillis()) {
            timeToPoolVent.set(System.currentTimeMillis() + POOL_VENT_TIMEOUT);
            poolControl.closeExpired();
        }
    }

    private HttpContext createRequestHttpContext(Map<String, Object> requestConfig) {
        HttpClientContext context = HttpClientContext.create();
        Number responseTimeout = ClientConfigProperties.SOCKET_OPERATION_TIMEOUT.getOrDefault(requestConfig);
        Number connectionReqTimeout =  ClientConfigProperties.CONNECTION_REQUEST_TIMEOUT.getOrDefault(requestConfig);
        RequestConfig reqHttpConf = RequestConfig.custom()
                .setResponseTimeout(responseTimeout.longValue(),  TimeUnit.MILLISECONDS)
                .setConnectionRequestTimeout(connectionReqTimeout.longValue(),  TimeUnit.MILLISECONDS)
                .build();
        context.setRequestConfig(reqHttpConf);

        return context;
    }

    private URI createRequestURI(Endpoint server, Map<String,Object> requestConfig, boolean addParameters) {
        URI uri;
        try {
            URIBuilder uriBuilder = new URIBuilder(server.getURI());
            addRequestParams(requestConfig, uriBuilder::addParameter);

            if (addParameters) {
                addStatementParams(requestConfig, uriBuilder::addParameter);
            }

            uri = uriBuilder.optimize().build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        return uri;
    }

    private HttpPost createPostRequest(URI uri, Map<String, Object> requestConfig) {
        HttpPost req = new HttpPost(uri);
//        req.setVersion(new ProtocolVersion("HTTP", 1, 0)); // to disable chunk transfer encoding
        addHeaders(req, requestConfig);
        return req;
    }

    public ClassicHttpResponse executeRequest(Endpoint server, Map<String, Object> requestConfig,
                                              String body) throws Exception {

        final URI uri = createRequestURI(server, requestConfig, true);
        final HttpPost req = createPostRequest(uri, requestConfig);
        final String contentEncoding = req.containsHeader(HttpHeaders.CONTENT_ENCODING) ? req.getHeader(HttpHeaders.CONTENT_ENCODING).getValue() : null;

        HttpEntity httpEntity = new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8.name()), CONTENT_TYPE, contentEncoding);
        req.setEntity(wrapRequestEntity(httpEntity, requestConfig));

        return doPostRequest(requestConfig, req);
    }

    public ClassicHttpResponse executeMultiPartRequest(Endpoint server, Map<String, Object> requestConfig, String sqlQuery) throws Exception {

        requestConfig.put(ClientConfigProperties.COMPRESS_CLIENT_REQUEST.getKey(), false);

        final URI uri = createRequestURI(server, requestConfig, false);
        final HttpPost req = createPostRequest(uri, requestConfig);

        MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create();
        addStatementParams(requestConfig, multipartEntityBuilder::addTextBody);
        multipartEntityBuilder.addTextBody(ClickHouseHttpProto.QPARAM_QUERY_STMT, sqlQuery);


        HttpEntity httpEntity = multipartEntityBuilder.build();
        req.setHeader(HttpHeaders.CONTENT_TYPE, httpEntity.getContentType()); // set proper content type with generated boundary value
        req.setEntity(wrapRequestEntity(httpEntity, requestConfig));

        return doPostRequest(requestConfig, req);


    }

    public ClassicHttpResponse executeRequest(Endpoint server, Map<String, Object> requestConfig,
                                              IOCallback<OutputStream> writeCallback) throws Exception {

        final URI uri = createRequestURI(server, requestConfig, true);
        final HttpPost req = createPostRequest(uri, requestConfig);
        String contentEncoding = req.containsHeader(HttpHeaders.CONTENT_ENCODING) ? req.getHeader(HttpHeaders.CONTENT_ENCODING).getValue() : null;
        req.setEntity(wrapRequestEntity(
                new EntityTemplate(-1, CONTENT_TYPE, contentEncoding , writeCallback),
                requestConfig));

        return doPostRequest(requestConfig, req);
    }

    private ClassicHttpResponse doPostRequest(Map<String, Object> requestConfig, HttpPost req) throws Exception {

        doPoolVent();

        ClassicHttpResponse httpResponse = null;
        HttpContext context = createRequestHttpContext(requestConfig);
        try {
            httpResponse = httpClient.executeOpen(null, req, context);

            httpResponse.setEntity(wrapResponseEntity(httpResponse.getEntity(),
                    httpResponse.getCode(),
                    requestConfig));

            if (httpResponse.getCode() == HttpStatus.SC_PROXY_AUTHENTICATION_REQUIRED) {
                throw new ClientMisconfigurationException("Proxy authentication required. Please check your proxy settings.");
            } else if (httpResponse.getCode() == HttpStatus.SC_BAD_GATEWAY) {
                httpResponse.close();
                throw new ClientException("Server returned '502 Bad gateway'. Check network and proxy settings.");
            } else if (httpResponse.getCode() >= HttpStatus.SC_BAD_REQUEST || httpResponse.containsHeader(ClickHouseHttpProto.HEADER_EXCEPTION_CODE)) {
                try {
                    throw readError(httpResponse);
                } finally {
                    httpResponse.close();
                }
            }
            return httpResponse;

        } catch (UnknownHostException e) {
            closeQuietly(httpResponse);
            LOG.warn("Host '{}' unknown", req.getAuthority());
            throw e;
        } catch (ConnectException | NoRouteToHostException e) {
            closeQuietly(httpResponse);
            LOG.warn("Failed to connect to '{}': {}", req.getAuthority(), e.getMessage());
            throw e;
        } catch (Exception e) {
            closeQuietly(httpResponse);
            LOG.debug("Failed to execute request to '{}': {}", req.getAuthority(), e.getMessage(), e);
            throw e;
        }
    }

    public static void closeQuietly(ClassicHttpResponse httpResponse) {
        if (httpResponse != null) {
            try {
                httpResponse.close();
            } catch (IOException e) {
                LOG.warn("Failed to close response");
            }
        }
    }

    private static final ContentType CONTENT_TYPE = ContentType.create(ContentType.TEXT_PLAIN.getMimeType(), "UTF-8");

    private void addHeaders(HttpPost req, Map<String, Object> requestConfig) {
        setHeader(req, HttpHeaders.CONTENT_TYPE, CONTENT_TYPE.getMimeType());
        if (requestConfig.containsKey(ClientConfigProperties.INPUT_OUTPUT_FORMAT.getKey())) {
            setHeader(
                req,
                ClickHouseHttpProto.HEADER_FORMAT,
                    ((ClickHouseFormat) requestConfig.get(ClientConfigProperties.INPUT_OUTPUT_FORMAT.getKey())).name());
        }
        if (requestConfig.containsKey(ClientConfigProperties.QUERY_ID.getKey())) {
            setHeader(
                req,
                ClickHouseHttpProto.HEADER_QUERY_ID,
                    (String) requestConfig.get(ClientConfigProperties.QUERY_ID.getKey()));
        }
        setHeader(
            req,
            ClickHouseHttpProto.HEADER_DATABASE,
            ClientConfigProperties.DATABASE.getOrDefault(requestConfig));

        if (ClientConfigProperties.SSL_AUTH.<Boolean>getOrDefault(requestConfig).booleanValue()) {
            setHeader(
                req,
                ClickHouseHttpProto.HEADER_DB_USER,
                ClientConfigProperties.USER.getOrDefault(requestConfig));
            setHeader(
                req,
                ClickHouseHttpProto.HEADER_SSL_CERT_AUTH,
                "on");
        } else if (ClientConfigProperties.HTTP_USE_BASIC_AUTH.<Boolean>getOrDefault(requestConfig).booleanValue()) {
            String user = ClientConfigProperties.USER.getOrDefault(requestConfig);
            String password = ClientConfigProperties.PASSWORD.getOrDefault(requestConfig);
            // Use as-is, no encoding allowed
            req.addHeader(
                HttpHeaders.AUTHORIZATION,
                "Basic " + Base64.getEncoder().encodeToString(
                    (user + ":" + password).getBytes(StandardCharsets.UTF_8)));
        } else {
            setHeader(
                req,
                ClickHouseHttpProto.HEADER_DB_USER,
                ClientConfigProperties.USER.getOrDefault(requestConfig));
            setHeader(
                req,
                ClickHouseHttpProto.HEADER_DB_PASSWORD,
                ClientConfigProperties.PASSWORD.getOrDefault(requestConfig));
        }
        if (proxyAuthHeaderValue != null) {
            req.addHeader(
                HttpHeaders.PROXY_AUTHORIZATION,
                proxyAuthHeaderValue);
        }

        boolean clientCompression = ClientConfigProperties.COMPRESS_CLIENT_REQUEST.getOrDefault(requestConfig);
        boolean serverCompression = ClientConfigProperties.COMPRESS_SERVER_RESPONSE.getOrDefault(requestConfig);
        boolean useHttpCompression = ClientConfigProperties.USE_HTTP_COMPRESSION.getOrDefault(requestConfig);
        boolean appCompressedData = ClientConfigProperties.APP_COMPRESSED_DATA.getOrDefault(requestConfig);

        if (useHttpCompression) {
            if (serverCompression) {
                setHeader(req, HttpHeaders.ACCEPT_ENCODING, DEFAULT_HTTP_COMPRESSION_ALGO);
            }

            if (clientCompression && !appCompressedData) {
                setHeader(req, HttpHeaders.CONTENT_ENCODING, DEFAULT_HTTP_COMPRESSION_ALGO);
            }
        }

        for (String key : requestConfig.keySet()) {
            if (key.startsWith(ClientConfigProperties.HTTP_HEADER_PREFIX)) {
                Object val = requestConfig.get(key);
                if (val != null) {
                    setHeader(
                        req,
                        key.substring(ClientConfigProperties.HTTP_HEADER_PREFIX.length()),
                        String.valueOf(val));
                }
            }
        }

        // Special cases
        if (req.containsHeader(HttpHeaders.AUTHORIZATION)
            && (req.containsHeader(ClickHouseHttpProto.HEADER_DB_USER) ||
                req.containsHeader(ClickHouseHttpProto.HEADER_DB_PASSWORD)))
        {
            // user has set auth header for purpose, lets remove ours
            req.removeHeaders(ClickHouseHttpProto.HEADER_DB_USER);
            req.removeHeaders(ClickHouseHttpProto.HEADER_DB_PASSWORD);
        }

        // -- keep last
        correctUserAgentHeader(req, requestConfig);
    }

    private void addRequestParams(Map<String, Object> requestConfig, BiConsumer<String, String> consumer) {
        if (requestConfig.containsKey(ClientConfigProperties.QUERY_ID.getKey())) {
            consumer.accept(ClickHouseHttpProto.QPARAM_QUERY_ID, requestConfig.get(ClientConfigProperties.QUERY_ID.getKey()).toString());
        }

        boolean clientCompression = ClientConfigProperties.COMPRESS_CLIENT_REQUEST.getOrDefault(requestConfig);
        boolean serverCompression = ClientConfigProperties.COMPRESS_SERVER_RESPONSE.getOrDefault(requestConfig);
        boolean useHttpCompression = ClientConfigProperties.USE_HTTP_COMPRESSION.getOrDefault(requestConfig);

        if (useHttpCompression) {
            // enable_http_compression make server react on http header
            // for client side compression Content-Encoding should be set
            // for server side compression Accept-Encoding should be set
            consumer.accept("enable_http_compression", "1");
        } else {
            if (serverCompression) {
                consumer.accept("compress", "1");
            }
            if (clientCompression) {
                consumer.accept("decompress", "1");
            }
        }

        Collection<String> sessionRoles = ClientConfigProperties.SESSION_DB_ROLES.getOrDefault(requestConfig);
        if (!(sessionRoles == null || sessionRoles.isEmpty())) {
            sessionRoles.forEach(r -> consumer.accept(ClickHouseHttpProto.QPARAM_ROLE, r));
        }

        for (String key : requestConfig.keySet()) {
            if (key.startsWith(ClientConfigProperties.SERVER_SETTING_PREFIX)) {
                Object val = requestConfig.get(key);
                if (val != null) {
                    consumer.accept(key.substring(ClientConfigProperties.SERVER_SETTING_PREFIX.length()), String.valueOf(requestConfig.get(key)));
                }
            }
        }
    }

    private void addStatementParams(Map<String, Object> requestConfig, BiConsumer<String, String> consumer) {
        if (requestConfig.containsKey(KEY_STATEMENT_PARAMS)) {
            Map<?, ?> params = (Map<?, ?>) requestConfig.get(KEY_STATEMENT_PARAMS);
            params.forEach((k, v) -> consumer.accept("param_" + k, String.valueOf(v)));
        }
    }

    private HttpEntity wrapRequestEntity(HttpEntity httpEntity, Map<String, Object> requestConfig) {

        boolean clientCompression = ClientConfigProperties.COMPRESS_CLIENT_REQUEST.getOrDefault(requestConfig);
        boolean useHttpCompression = ClientConfigProperties.USE_HTTP_COMPRESSION.getOrDefault(requestConfig);
        boolean appCompressedData = ClientConfigProperties.APP_COMPRESSED_DATA.getOrDefault(requestConfig);

        LOG.debug("wrapRequestEntity: client compression: {}, http compression: {}, content encoding: {}",
                clientCompression, useHttpCompression, httpEntity.getContentEncoding());

        if (httpEntity.getContentEncoding() != null && !appCompressedData) {
            // http header is set and data is not compressed
            return new CompressedEntity(httpEntity, false, CompressorStreamFactory.getSingleton());
        } else if (clientCompression && !appCompressedData) {
            int buffSize = ClientConfigProperties.COMPRESSION_LZ4_UNCOMPRESSED_BUF_SIZE.getOrDefault(requestConfig);
            return new LZ4Entity(httpEntity, useHttpCompression, false, true,
                    buffSize, false, lz4Factory);
        } else {
            return httpEntity;
        }
    }

    private HttpEntity wrapResponseEntity(HttpEntity httpEntity, int httpStatus, Map<String, Object> requestConfig) {
        boolean serverCompression = ClientConfigProperties.COMPRESS_SERVER_RESPONSE.getOrDefault(requestConfig);
        boolean useHttpCompression = ClientConfigProperties.USE_HTTP_COMPRESSION.getOrDefault(requestConfig);

        LOG.debug("wrapResponseEntity: server compression: {}, http compression: {}, content encoding: {}",
                serverCompression, useHttpCompression, httpEntity.getContentEncoding());

        if (httpEntity.getContentEncoding() != null) {
            // http compressed response
            return new CompressedEntity(httpEntity, true, CompressorStreamFactory.getSingleton());
        }

        // data compression
        if (serverCompression && !(httpStatus == HttpStatus.SC_FORBIDDEN || httpStatus == HttpStatus.SC_UNAUTHORIZED)) {
            int buffSize = ClientConfigProperties.COMPRESSION_LZ4_UNCOMPRESSED_BUF_SIZE.getOrDefault(requestConfig);
            return new LZ4Entity(httpEntity, useHttpCompression, true, false, buffSize, true, lz4Factory);
        }

        return httpEntity;
    }

    public static int getHeaderInt(Header header, int defaultValue) {
        return getHeaderVal(header, defaultValue, Integer::parseInt);
    }

    private static final Set<String> RESPONSE_HEADER_WHITELIST = new HashSet<>(Arrays.asList(
            ClickHouseHttpProto.HEADER_QUERY_ID,
            ClickHouseHttpProto.HEADER_SRV_SUMMARY,
            ClickHouseHttpProto.HEADER_SRV_DISPLAY_NAME,
            ClickHouseHttpProto.HEADER_DATABASE,
            ClickHouseHttpProto.HEADER_DB_USER
    ));

    /**
     * Collects whitelisted response headers from an HTTP response into a map.
     *
     * @param response the HTTP response
     * @return unmodifiable map of header name to header value for whitelisted headers present in the response
     */
    public static Map<String, String> collectResponseHeaders(ClassicHttpResponse response) {
        Map<String, String> headers = new HashMap<>();
        for (String name : RESPONSE_HEADER_WHITELIST) {
            Header header = response.getFirstHeader(name);
            if (header != null) {
                headers.put(name, header.getValue());
            }
        }
        return Collections.unmodifiableMap(headers);
    }

    public static String getHeaderVal(Header header, String defaultValue) {
        return getHeaderVal(header, defaultValue, Function.identity());
    }

    public static <T> T getHeaderVal(Header header, T defaultValue, Function<String, T> converter) {
        if (header == null) {
            return defaultValue;
        }

        return converter.apply(header.getValue());
    }

    public boolean shouldRetry(Throwable ex, Map<String, Object> requestSettings) {
        List<ClientFaultCause> retryCauses = ClientConfigProperties.CLIENT_RETRY_ON_FAILURE.getOrDefault(requestSettings);

        if (retryCauses.contains(ClientFaultCause.None)) {
            return false;
        }

        if (ex instanceof NoHttpResponseException
                || ex.getCause() instanceof NoHttpResponseException) {
            return retryCauses.contains(ClientFaultCause.NoHttpResponse);
        }

        if (ex instanceof ConnectException
                || ex instanceof ConnectTimeoutException
                || ex.getCause() instanceof ConnectException
                || ex.getCause() instanceof ConnectTimeoutException) {
            return retryCauses.contains(ClientFaultCause.ConnectTimeout);
        }

        if (ex instanceof ConnectionRequestTimeoutException
                || ex.getCause() instanceof ConnectionRequestTimeoutException) {
            return retryCauses.contains(ClientFaultCause.ConnectionRequestTimeout);
        }

        if (ex instanceof SocketTimeoutException
                || ex.getCause() instanceof SocketTimeoutException) {
            return retryCauses.contains(ClientFaultCause.SocketTimeout);
        }

        // there are some db retryable error codes
        if (ex instanceof ServerException || ex.getCause() instanceof ServerException) {
            ServerException se = (ServerException) ex;
            return se.isRetryable() && retryCauses.contains(ClientFaultCause.ServerRetryable);
        }

        return false;
    }

    // This method wraps some client specific exceptions into specific ClientException or just ClientException
    // ClientException will be also wrapped
    public RuntimeException wrapException(String message, Exception cause, String queryId) {
        if (cause instanceof ClientException || cause instanceof ServerException) {
            return (RuntimeException) cause;
        }

        if (cause instanceof ConnectionRequestTimeoutException ||
                cause instanceof NoHttpResponseException ||
                cause instanceof ConnectTimeoutException ||
                cause instanceof ConnectException ||
                cause instanceof UnknownHostException ||
                cause instanceof NoRouteToHostException) {
            ConnectionInitiationException ex = new ConnectionInitiationException(message, cause);
            ex.setQueryId(queryId);
            return ex;
        }

        if (cause instanceof SocketTimeoutException || cause instanceof IOException) {
            DataTransferException ex =  new DataTransferException(message, cause);
            ex.setQueryId(queryId);
            return ex;
        }
        // if we can not identify the exception explicitly we catch as our base exception ClickHouseException
        return new ClickHouseException(message, cause, queryId);
    }

    private void correctUserAgentHeader(HttpRequest request, Map<String, Object> requestConfig) {
        //TODO: implement cache for user-agent
        Header userAgentHeader = request.getLastHeader(HttpHeaders.USER_AGENT);
        request.removeHeaders(HttpHeaders.USER_AGENT);

        String clientName = ClientConfigProperties.CLIENT_NAME.getOrDefault(requestConfig);
        String userAgentValue = defaultUserAgent;
        if (userAgentHeader == null && clientName != null && !clientName.isEmpty()) {
            userAgentValue = clientName + " " + defaultUserAgent;
        } else if (userAgentHeader != null) {
            userAgentValue = userAgentHeader.getValue() + " " + defaultUserAgent;
        }
        request.setHeader(HttpHeaders.USER_AGENT, userAgentValue);
    }

    private String buildDefaultUserAgent() {
        StringBuilder userAgent = new StringBuilder();
        userAgent.append(Client.CLIENT_USER_AGENT);

        String clientVersion = Client.clientVersion;

        userAgent.append(clientVersion);

        userAgent.append(" (");
        userAgent.append(System.getProperty("os.name"));
        userAgent.append("; ");
        userAgent.append("jvm:").append(System.getProperty("java.version"));
        userAgent.append("; ");

        userAgent.setLength(userAgent.length() - 2);
        userAgent.append(')');

        try {
            String httpClientVersion = this.httpClient.getClass().getPackage().getImplementationVersion();
            if (Objects.equals(this.httpClient.getClass().getPackage().getImplementationTitle(), this.getClass().getPackage().getImplementationTitle())) {
                // shaded jar - all packages have same implementation title
                httpClientVersion = "unknown";
                try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("client-v2-version.properties")) {
                    Properties p = new Properties();
                    p.load(in);

                    String tmp = p.getProperty("apache.http.client.version");
                    if (tmp != null && !tmp.isEmpty() && !tmp.equals("${apache.httpclient.version}")) {
                        httpClientVersion = tmp;
                    }
                } catch (Exception e) {
                    // ignore
                }
            }
            userAgent.append(" ")
                    .append("Apache-HttpClient")
                    .append('/')
                    .append(httpClientVersion);
        } catch (Exception e) {
            LOG.info("failed to construct http client version string");
        }
        return userAgent.toString();
    }

    public void close() {
        httpClient.close(CloseMode.IMMEDIATE);
    }

    private static <T> void setHeader(HttpRequest req, String headerName,
                                      String value)
    {
        if (value == null) {
            return;
        }

        if (value.trim().isEmpty()) {
            return;
        }
        if (PATTERN_HEADER_VALUE_ASCII.matcher(value).matches()) {
            req.setHeader(headerName, value);
        } else {
            try {
                req.setHeader(
                        headerName + "*",
                        "UTF-8''" + URLEncoder.encode(value, StandardCharsets.UTF_8.name()));
            } catch (UnsupportedEncodingException e) {
                throw new ClientException("Failed to convert string to UTF8" , e);
            }
        }
    }

    /**
     * This factory is used only when no ssl connections are required (no https endpoints).
     * Internally http client would create factory and spend time if no supplied.
     */
    private static class DummySSLConnectionSocketFactory implements LayeredConnectionSocketFactory {
        @Override
        public Socket createLayeredSocket(Socket socket, String target, int port, HttpContext context) throws IOException {
            return null;
        }

        @Override
        public Socket createSocket(HttpContext context) throws IOException {
            return null;
        }

        @Override
        public Socket connectSocket(TimeValue connectTimeout, Socket socket, HttpHost host, InetSocketAddress remoteAddress, InetSocketAddress localAddress, HttpContext context) throws IOException {
            return null;
        }
    }

    public static class MeteredManagedHttpClientConnectionFactory extends ManagedHttpClientConnectionFactory {
        public MeteredManagedHttpClientConnectionFactory(Http1Config http1Config, CharCodingConfig charCodingConfig, DefaultHttpResponseParserFactory defaultHttpResponseParserFactory) {
            super(http1Config, charCodingConfig, defaultHttpResponseParserFactory);
        }

        ConcurrentLinkedQueue<Long> times = new ConcurrentLinkedQueue<>();


        @Override
        public ManagedHttpClientConnection createConnection(Socket socket) throws IOException {
            long startT = System.currentTimeMillis();
            try {
                return super.createConnection(socket);
            } finally {
                long endT = System.currentTimeMillis();
                times.add(endT - startT);
            }
        }

        public long getTime() {
            int count = times.size();
            long runningAverage = 0;
            for (int i = 0; i < count; i++) {
                Long t = times.poll();
                if (t != null) {
                    runningAverage += t;
                }
            }

            return count > 0 ? runningAverage / count : 0;
        }
    }

    public static class CustomSSLConnectionFactory extends SSLConnectionSocketFactory {

        private final SNIHostName defaultSNI;

        public CustomSSLConnectionFactory(String defaultSNI, SSLContext sslContext, HostnameVerifier hostnameVerifier) {
            super(sslContext, hostnameVerifier);
            this.defaultSNI = defaultSNI == null || defaultSNI.trim().isEmpty() ? null : new SNIHostName(defaultSNI);
        }

        @Override
        protected void prepareSocket(SSLSocket socket, HttpContext context) throws IOException {
            super.prepareSocket(socket, context);

            if (defaultSNI != null) {
                SSLParameters sslParams = socket.getSSLParameters();
                sslParams.setServerNames(Collections.singletonList(defaultSNI));
                socket.setSSLParameters(sslParams);
            }
        }
    }
}
