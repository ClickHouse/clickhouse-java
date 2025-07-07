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
import net.jpountz.lz4.LZ4Factory;
import org.apache.hc.client5.http.ConnectTimeoutException;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.regex.Pattern;

public class HttpAPIClientHelper {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private static final int ERROR_BODY_BUFFER_SIZE = 1024; // Error messages are usually small

    private static final Pattern PATTERN_HEADER_VALUE_ASCII = Pattern.compile(
        "\\p{Graph}+(?:[ ]\\p{Graph}+)*");

    private final CloseableHttpClient httpClient;

    private final RequestConfig baseRequestConfig;

    private String proxyAuthHeaderValue;

    private final Set<ClientFaultCause> defaultRetryCauses;

    private final String defaultUserAgent;
    private final Object metricsRegistry;

    ConnPoolControl<?> poolControl;

    public HttpAPIClientHelper(Map<String, Object> configuration, Object metricsRegistry, boolean initSslContext) {
        this.metricsRegistry = metricsRegistry;
        this.httpClient = createHttpClient(initSslContext, configuration);

        RequestConfig.Builder reqConfBuilder = RequestConfig.custom();
        reqConfBuilder.setConnectionRequestTimeout(ClientConfigProperties.CONNECTION_REQUEST_TIMEOUT.getOrDefault(configuration), TimeUnit.MILLISECONDS);

        this.baseRequestConfig = reqConfBuilder.build();

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
        LayeredConnectionSocketFactory sslConnectionSocketFactory = sslContext == null ? new DummySSLConnectionSocketFactory()
                : new SSLConnectionSocketFactory(sslContext);
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

        return clientBuilder.build();
    }

    private static final String ERROR_CODE_PREFIX_PATTERN = "Code: %d. DB::Exception:";

    /**
     * Reads status line and if error tries to parse response body to get server error message.
     *
     * @param httpResponse - HTTP response
     * @return exception object with server code
     */
    public Exception readError(ClassicHttpResponse httpResponse) {
        int serverCode = getHeaderInt(httpResponse.getFirstHeader(ClickHouseHttpProto.HEADER_EXCEPTION_CODE), 0);
        try (InputStream body = httpResponse.getEntity().getContent()) {

            byte[] buffer = new byte[ERROR_BODY_BUFFER_SIZE];
            byte[] lookUpStr = String.format(ERROR_CODE_PREFIX_PATTERN, serverCode).getBytes(StandardCharsets.UTF_8);
            StringBuilder msgBuilder = new StringBuilder();
            boolean found = false;
            while (true) {
                int rBytes = body.read(buffer);
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
            return new ServerException(serverCode, msg, httpResponse.getCode());
        } catch (Exception e) {
            LOG.error("Failed to read error message", e);
            return new ServerException(serverCode, String.format(ERROR_CODE_PREFIX_PATTERN, serverCode) + " <Unreadable error message> (transport error: " + httpResponse.getCode() + ")", httpResponse.getCode());
        }
    }

    private static final long POOL_VENT_TIMEOUT = 10000L;
    private final AtomicLong timeToPoolVent = new AtomicLong(0);

    public ClassicHttpResponse executeRequest(Endpoint server, Map<String, Object> requestConfig, LZ4Factory lz4Factory,
                                              IOCallback<OutputStream> writeCallback) throws Exception {
        if (poolControl != null && timeToPoolVent.get() < System.currentTimeMillis()) {
            timeToPoolVent.set(System.currentTimeMillis() + POOL_VENT_TIMEOUT);
            poolControl.closeExpired();
        }

        if (requestConfig == null) {
            requestConfig = Collections.emptyMap();
        }
        URI uri;
        try {
            URIBuilder uriBuilder = new URIBuilder(server.getBaseURL());
            addQueryParams(uriBuilder, requestConfig);
            uri = uriBuilder.normalizeSyntax().build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        HttpPost req = new HttpPost(uri);
//        req.setVersion(new ProtocolVersion("HTTP", 1, 0)); // to disable chunk transfer encoding
        addHeaders(req, requestConfig);

        boolean clientCompression = ClientConfigProperties.COMPRESS_CLIENT_REQUEST.getOrDefault(requestConfig);
        boolean useHttpCompression = ClientConfigProperties.USE_HTTP_COMPRESSION.getOrDefault(requestConfig);
        boolean appCompressedData = ClientConfigProperties.APP_COMPRESSED_DATA.getOrDefault(requestConfig);

        req.setConfig(baseRequestConfig);
        // setting entity. wrapping if compression is enabled
        req.setEntity(wrapRequestEntity(new EntityTemplate(-1, CONTENT_TYPE, null, writeCallback),
                clientCompression, useHttpCompression, appCompressedData, lz4Factory, requestConfig));

        HttpClientContext context = HttpClientContext.create();

        try {
            ClassicHttpResponse httpResponse = httpClient.executeOpen(null, req, context);
            boolean serverCompression = ClientConfigProperties.COMPRESS_SERVER_RESPONSE.getOrDefault(requestConfig);
            httpResponse.setEntity(wrapResponseEntity(httpResponse.getEntity(), httpResponse.getCode(), serverCompression, useHttpCompression, lz4Factory, requestConfig));

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
            LOG.warn("Host '{}' unknown", server.getBaseURL());
            throw e;
        } catch (ConnectException | NoRouteToHostException e) {
            LOG.warn("Failed to connect to '{}': {}", server.getBaseURL(), e.getMessage());
            throw e;
        }
    }

    private static final ContentType CONTENT_TYPE = ContentType.create(ContentType.TEXT_PLAIN.getMimeType(), "UTF-8");

    private void addHeaders(HttpPost req, Map<String, Object> requestConfig) {
        addHeader(req, HttpHeaders.CONTENT_TYPE, CONTENT_TYPE.getMimeType());
        if (requestConfig.containsKey(ClientConfigProperties.INPUT_OUTPUT_FORMAT.getKey())) {
            addHeader(
                req,
                ClickHouseHttpProto.HEADER_FORMAT,
                requestConfig.get(ClientConfigProperties.INPUT_OUTPUT_FORMAT.getKey()));
        }
        if (requestConfig.containsKey(ClientConfigProperties.QUERY_ID.getKey())) {
            addHeader(
                req,
                ClickHouseHttpProto.HEADER_QUERY_ID,
                requestConfig.get(ClientConfigProperties.QUERY_ID.getKey()));
        }
        addHeader(
            req,
            ClickHouseHttpProto.HEADER_DATABASE,
            ClientConfigProperties.DATABASE.getOrDefault(requestConfig));

        if (ClientConfigProperties.SSL_AUTH.<Boolean>getOrDefault(requestConfig).booleanValue()) {
            addHeader(
                req,
                ClickHouseHttpProto.HEADER_DB_USER,
                ClientConfigProperties.USER.getOrDefault(requestConfig));
            addHeader(
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
            addHeader(
                req,
                ClickHouseHttpProto.HEADER_DB_USER,
                ClientConfigProperties.USER.getOrDefault(requestConfig));
            addHeader(
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
                addHeader(req, HttpHeaders.ACCEPT_ENCODING, "lz4");
            }
            if (clientCompression && !appCompressedData) {
                addHeader(req, HttpHeaders.CONTENT_ENCODING, "lz4");
            }
        }

        for (String key : requestConfig.keySet()) {
            if (key.startsWith(ClientConfigProperties.HTTP_HEADER_PREFIX)) {
                Object val = requestConfig.get(key);
                if (val != null) {
                    addHeader(
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

    private void addQueryParams(URIBuilder req, Map<String, Object> requestConfig) {
        if (requestConfig.containsKey(ClientConfigProperties.QUERY_ID.getKey())) {
            req.addParameter(ClickHouseHttpProto.QPARAM_QUERY_ID, requestConfig.get(ClientConfigProperties.QUERY_ID.getKey()).toString());
        }
        if (requestConfig.containsKey("statement_params")) {
            Map<String, Object> params = (Map<String, Object>) requestConfig.get("statement_params");
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                req.addParameter("param_" + entry.getKey(), String.valueOf(entry.getValue()));
            }
        }

        boolean clientCompression = ClientConfigProperties.COMPRESS_CLIENT_REQUEST.getOrDefault(requestConfig);
        boolean serverCompression = ClientConfigProperties.COMPRESS_SERVER_RESPONSE.getOrDefault(requestConfig);
        boolean useHttpCompression = ClientConfigProperties.USE_HTTP_COMPRESSION.getOrDefault(requestConfig);

        if (useHttpCompression) {
            // enable_http_compression make server react on http header
            // for client side compression Content-Encoding should be set
            // for server side compression Accept-Encoding should be set
            req.addParameter("enable_http_compression", "1");
        } else {
            if (serverCompression) {
                req.addParameter("compress", "1");
            }
            if (clientCompression) {
                req.addParameter("decompress", "1");
            }
        }

        Collection<String> sessionRoles = ClientConfigProperties.SESSION_DB_ROLES.getOrDefault(requestConfig);
        if (!(sessionRoles == null || sessionRoles.isEmpty())) {
            sessionRoles.forEach(r -> req.addParameter(ClickHouseHttpProto.QPARAM_ROLE, r));
        }

        for (String key : requestConfig.keySet()) {
            if (key.startsWith(ClientConfigProperties.SERVER_SETTING_PREFIX)) {
                Object val = requestConfig.get(key);
                if (val != null) {
                    req.addParameter(key.substring(ClientConfigProperties.SERVER_SETTING_PREFIX.length()), String.valueOf(requestConfig.get(key)));
                }
            }
        }
    }

    private HttpEntity wrapRequestEntity(HttpEntity httpEntity, boolean clientCompression, boolean useHttpCompression,
                                         boolean appControlledCompression, LZ4Factory lz4Factory, Map<String, Object> requestConfig) {
        LOG.debug("wrapRequestEntity: client compression: {}, http compression: {}", clientCompression, useHttpCompression);

        if (clientCompression && !appControlledCompression) {
            int buffSize = ClientConfigProperties.COMPRESSION_LZ4_UNCOMPRESSED_BUF_SIZE.getOrDefault(requestConfig);
            return new LZ4Entity(httpEntity, useHttpCompression, false, true,
                    buffSize, false, lz4Factory);
        } else {
            return httpEntity;
        }
    }

    private HttpEntity wrapResponseEntity(HttpEntity httpEntity, int httpStatus, boolean serverCompression, boolean useHttpCompression, LZ4Factory lz4Factory, Map<String, Object> requestConfig) {
        LOG.debug("wrapResponseEntity: server compression: {}, http compression: {}", serverCompression, useHttpCompression);

        if (serverCompression) {
            // Server doesn't compress certain errors like 403
            switch (httpStatus) {
                case HttpStatus.SC_OK:
                case HttpStatus.SC_CREATED:
                case HttpStatus.SC_ACCEPTED:
                case HttpStatus.SC_NO_CONTENT:
                case HttpStatus.SC_PARTIAL_CONTENT:
                case HttpStatus.SC_RESET_CONTENT:
                case HttpStatus.SC_NOT_MODIFIED:
                case HttpStatus.SC_BAD_REQUEST:
                case HttpStatus.SC_INTERNAL_SERVER_ERROR:
                case HttpStatus.SC_NOT_FOUND:
                    int buffSize = ClientConfigProperties.COMPRESSION_LZ4_UNCOMPRESSED_BUF_SIZE.getOrDefault(requestConfig);
                    return new LZ4Entity(httpEntity, useHttpCompression, true, false, buffSize, true, lz4Factory);
            }
        }

        return httpEntity;
    }

    public static int getHeaderInt(Header header, int defaultValue) {
        return getHeaderVal(header, defaultValue, Integer::parseInt);
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
    public RuntimeException wrapException(String message, Exception cause) {
        if (cause instanceof ClientException || cause instanceof ServerException) {
            return (RuntimeException) cause;
        }

        if (cause instanceof ConnectionRequestTimeoutException ||
                cause instanceof NoHttpResponseException ||
                cause instanceof ConnectTimeoutException ||
                cause instanceof ConnectException ||
                cause instanceof UnknownHostException ||
                cause instanceof NoRouteToHostException) {
            return new ConnectionInitiationException(message, cause);
        }

        if (cause instanceof SocketTimeoutException || cause instanceof IOException) {
            return new DataTransferException(message, cause);
        }
        // if we can not identify the exception explicitly we catch as our base exception ClickHouseException
        return new ClickHouseException(message, cause);
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

    private static <T> void addHeader(HttpRequest req, String headerName,
        T value)
    {
        if (value == null) {
            return;
        }
        String tString = value.toString();
        if (tString.isBlank()) {
            return;
        }
        if (PATTERN_HEADER_VALUE_ASCII.matcher(tString).matches()) {
            req.addHeader(headerName, tString);
        } else {
            req.addHeader(
                headerName + "*",
                "UTF-8''" + URLEncoder.encode(tString, StandardCharsets.UTF_8));
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
}
