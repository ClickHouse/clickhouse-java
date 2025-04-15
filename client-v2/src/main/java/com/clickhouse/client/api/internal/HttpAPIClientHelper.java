package com.clickhouse.client.api.internal;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseSslContextProvider;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ClientFaultCause;
import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.client.api.ConnectionInitiationException;
import com.clickhouse.client.api.ConnectionReuseStrategy;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import com.clickhouse.client.api.enums.ProxyType;
import com.clickhouse.client.api.http.ClickHouseHttpProto;
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
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class HttpAPIClientHelper {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private static int ERROR_BODY_BUFFER_SIZE = 1024; // Error messages are usually small

    private CloseableHttpClient httpClient;

    private Map<String, String> chConfiguration;

    private RequestConfig baseRequestConfig;

    private String proxyAuthHeaderValue;

    private final Set<ClientFaultCause> defaultRetryCauses;

    private String defaultUserAgent;
    private Object metricsRegistry;

    ConnPoolControl<?> poolControl;

    public HttpAPIClientHelper(Map<String, String> configuration, Object metricsRegistry, boolean initSslContext) {
        this.chConfiguration = configuration;
        this.metricsRegistry = metricsRegistry;
        this.httpClient = createHttpClient(initSslContext);

        RequestConfig.Builder reqConfBuilder = RequestConfig.custom();
        MapUtils.applyLong(chConfiguration, "connection_request_timeout",
                (t) -> reqConfBuilder
                        .setConnectionRequestTimeout(t, TimeUnit.MILLISECONDS));

        this.baseRequestConfig = reqConfBuilder.build();

        boolean usingClientCompression=  chConfiguration.getOrDefault(ClientConfigProperties.COMPRESS_CLIENT_REQUEST.getKey(), "false").equalsIgnoreCase("true");
        boolean usingServerCompression=  chConfiguration.getOrDefault(ClientConfigProperties.COMPRESS_SERVER_RESPONSE.getKey(), "false").equalsIgnoreCase("true");
        boolean useHttpCompression = chConfiguration.getOrDefault("client.use_http_compression", "false").equalsIgnoreCase("true");
        LOG.debug("client compression: {}, server compression: {}, http compression: {}", usingClientCompression, usingServerCompression, useHttpCompression);

        defaultRetryCauses = SerializerUtils.parseEnumList(chConfiguration.get(ClientConfigProperties.CLIENT_RETRY_ON_FAILURE.getKey()), ClientFaultCause.class);
        if (defaultRetryCauses.contains(ClientFaultCause.None)) {
            defaultRetryCauses.removeIf(c -> c != ClientFaultCause.None);
        }

        this.defaultUserAgent = buildDefaultUserAgent();
    }

    /**
     * Creates or returns default SSL context.
     * @return SSLContext
     */
    public SSLContext createSSLContext() {
        SSLContext sslContext;
        try {
            sslContext = SSLContext.getDefault();
        } catch (NoSuchAlgorithmException e) {
            throw new ClientException("Failed to create default SSL context", e);
        }
        ClickHouseSslContextProvider sslContextProvider = ClickHouseSslContextProvider.getProvider();
        String trustStorePath = chConfiguration.get(ClientConfigProperties.SSL_TRUST_STORE.getKey());
        if (trustStorePath != null ) {
            try {
                sslContext = sslContextProvider.getSslContextFromKeyStore(
                        trustStorePath,
                        chConfiguration.get(ClientConfigProperties.SSL_KEY_STORE_PASSWORD.getKey()),
                        chConfiguration.get(ClientConfigProperties.SSL_KEYSTORE_TYPE.getKey())
                );
            } catch (SSLException e) {
                throw new ClientMisconfigurationException("Failed to create SSL context from a keystore", e);
            }
        } else if (chConfiguration.get(ClientConfigProperties.CA_CERTIFICATE.getKey()) != null ||
                chConfiguration.get(ClientConfigProperties.SSL_CERTIFICATE.getKey()) != null ||
                chConfiguration.get(ClientConfigProperties.SSL_KEY.getKey()) != null) {

            try {
                sslContext = sslContextProvider.getSslContextFromCerts(
                        chConfiguration.get(ClientConfigProperties.SSL_CERTIFICATE.getKey()),
                        chConfiguration.get(ClientConfigProperties.SSL_KEY.getKey()),
                        chConfiguration.get(ClientConfigProperties.CA_CERTIFICATE.getKey())
                );
            } catch (SSLException e) {
                throw new ClientMisconfigurationException("Failed to create SSL context from certificates", e);
            }
        }
        return sslContext;
    }

    private long CONNECTION_INACTIVITY_CHECK = 5000L;

    private ConnectionConfig createConnectionConfig() {
        ConnectionConfig.Builder connConfig = ConnectionConfig.custom();
        connConfig.setTimeToLive(MapUtils.getLong(chConfiguration, ClientConfigProperties.CONNECTION_TTL.getKey()),
                TimeUnit.MILLISECONDS);
        connConfig.setConnectTimeout(MapUtils.getLong(chConfiguration, ClientConfigProperties.CONNECTION_TIMEOUT.getKey()),
                TimeUnit.MILLISECONDS);
        connConfig.setValidateAfterInactivity(CONNECTION_INACTIVITY_CHECK, TimeUnit.MILLISECONDS); // non-configurable for now

        return connConfig.build();
    }

    private HttpClientConnectionManager basicConnectionManager(LayeredConnectionSocketFactory sslConnectionSocketFactory, SocketConfig socketConfig) {
        RegistryBuilder<ConnectionSocketFactory> registryBuilder = RegistryBuilder.create();
        registryBuilder.register("http", PlainConnectionSocketFactory.getSocketFactory());
        registryBuilder.register("https", sslConnectionSocketFactory);

        BasicHttpClientConnectionManager connManager = new BasicHttpClientConnectionManager(registryBuilder.build());
        connManager.setConnectionConfig(createConnectionConfig());
        connManager.setSocketConfig(socketConfig);

        return connManager;
    }

    private HttpClientConnectionManager poolConnectionManager(LayeredConnectionSocketFactory sslConnectionSocketFactory, SocketConfig socketConfig) {
        PoolingHttpClientConnectionManagerBuilder connMgrBuilder = PoolingHttpClientConnectionManagerBuilder.create()
                .setPoolConcurrencyPolicy(PoolConcurrencyPolicy.LAX);

        ConnectionReuseStrategy connectionReuseStrategy =
                ConnectionReuseStrategy.valueOf(chConfiguration.get("connection_reuse_strategy"));
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

        connMgrBuilder.setDefaultConnectionConfig(createConnectionConfig());
        connMgrBuilder.setMaxConnTotal(Integer.MAX_VALUE); // as we do not know how many routes we will have
        MapUtils.applyInt(chConfiguration, ClientConfigProperties.HTTP_MAX_OPEN_CONNECTIONS.getKey(),
                connMgrBuilder::setMaxConnPerRoute);


        int networkBufferSize = MapUtils.getInt(chConfiguration, "client_network_buffer_size");
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
        if (metricsRegistry != null ) {
            try {
                String mGroupName = chConfiguration.getOrDefault(ClientConfigProperties.METRICS_GROUP_NAME.getKey(),
                        "ch-http-pool");
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

    public CloseableHttpClient createHttpClient(boolean initSslContext) {
        // Top Level builders
        HttpClientBuilder clientBuilder = HttpClientBuilder.create();
        SSLContext sslContext = initSslContext ? createSSLContext() : null;
        LayeredConnectionSocketFactory sslConnectionSocketFactory = sslContext == null ? new DummySSLConnectionSocketFactory()
                : new SSLConnectionSocketFactory(sslContext);
        // Socket configuration
        SocketConfig.Builder soCfgBuilder = SocketConfig.custom();
        MapUtils.applyInt(chConfiguration, ClientConfigProperties.SOCKET_OPERATION_TIMEOUT.getKey(),
                (t) -> soCfgBuilder.setSoTimeout(t, TimeUnit.MILLISECONDS));
        MapUtils.applyInt(chConfiguration, ClientConfigProperties.SOCKET_RCVBUF_OPT.getKey(),
                soCfgBuilder::setRcvBufSize);
        MapUtils.applyInt(chConfiguration, ClientConfigProperties.SOCKET_SNDBUF_OPT.getKey(),
                soCfgBuilder::setSndBufSize);
        MapUtils.applyInt(chConfiguration, ClientConfigProperties.SOCKET_LINGER_OPT.getKey(),
                    (v) -> soCfgBuilder.setSoLinger(v, TimeUnit.SECONDS));
        if (MapUtils.getFlag(chConfiguration, ClientConfigProperties.SOCKET_TCP_NO_DELAY_OPT.getKey(), false)) {
            soCfgBuilder.setTcpNoDelay(true);
        }

        // Proxy
        String proxyHost = chConfiguration.get(ClientConfigProperties.PROXY_HOST.getKey());
        String proxyPort = chConfiguration.get(ClientConfigProperties.PROXY_PORT.getKey());
        HttpHost proxy = null;
        if (proxyHost != null && proxyPort != null) {
            proxy = new HttpHost(proxyHost, Integer.parseInt(proxyPort));
        }

        String proxyTypeVal = chConfiguration.get(ClientConfigProperties.PROXY_TYPE.getKey());
        ProxyType proxyType = proxyTypeVal == null ? null : ProxyType.valueOf(proxyTypeVal);
        if (proxyType == ProxyType.HTTP) {
            clientBuilder.setProxy(proxy);
            if (chConfiguration.containsKey("proxy_password") && chConfiguration.containsKey("proxy_user")) {
                proxyAuthHeaderValue = "Basic " + Base64.getEncoder().encodeToString(
                        (chConfiguration.get("proxy_user") + ":" + chConfiguration.get("proxy_password")).getBytes());
            }
        } else if (proxyType == ProxyType.SOCKS) {
            soCfgBuilder.setSocksProxyAddress(new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)));
        }

        if (chConfiguration.getOrDefault("client.http.cookies_enabled", "true")
                .equalsIgnoreCase("false")) {
            clientBuilder.disableCookieManagement();
        }
        SocketConfig socketConfig = soCfgBuilder.build();

        // Connection manager
        boolean isConnectionPooling = MapUtils.getFlag(chConfiguration, "connection_pool_enabled");
        if (isConnectionPooling) {
            clientBuilder.setConnectionManager(poolConnectionManager(sslConnectionSocketFactory, socketConfig));
        } else {
            clientBuilder.setConnectionManager(basicConnectionManager(sslConnectionSocketFactory, socketConfig));
        }
        long keepAliveTimeout = MapUtils.getLong(chConfiguration, ClientConfigProperties.HTTP_KEEP_ALIVE_TIMEOUT.getKey());
        if (keepAliveTimeout > 0) {
            clientBuilder.setKeepAliveStrategy((response, context) -> TimeValue.ofMilliseconds(keepAliveTimeout));
        }

        return clientBuilder.build();
    }

    private static final String ERROR_CODE_PREFIX_PATTERN = "Code: %d. DB::Exception:";
    /**
     * Reads status line and if error tries to parse response body to get server error message.
     *
     * @param httpResponse - HTTP response
     * @return
     */
    public Exception readError(ClassicHttpResponse httpResponse) {
        int serverCode = getHeaderInt(httpResponse.getFirstHeader(ClickHouseHttpProto.HEADER_EXCEPTION_CODE), 0);
        try (InputStream body = httpResponse.getEntity().getContent()) {

            byte [] buffer = new byte[ERROR_BODY_BUFFER_SIZE];
            byte [] lookUpStr = String.format(ERROR_CODE_PREFIX_PATTERN, serverCode).getBytes(StandardCharsets.UTF_8);
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
    private AtomicLong timeToPoolVent = new AtomicLong(0);

    public ClassicHttpResponse executeRequest(ClickHouseNode server, Map<String, Object> requestConfig, LZ4Factory lz4Factory,
                                             IOCallback<OutputStream> writeCallback) throws IOException {
        if (poolControl != null && timeToPoolVent.get() < System.currentTimeMillis()) {
            timeToPoolVent.set(System.currentTimeMillis() + POOL_VENT_TIMEOUT);
            poolControl.closeExpired();
        }

        if (requestConfig == null) {
            requestConfig = Collections.emptyMap();
        }
        URI uri;
        try {
            URIBuilder uriBuilder = new URIBuilder(server.getBaseUri());
            addQueryParams(uriBuilder, chConfiguration, requestConfig);
            uri = uriBuilder.normalizeSyntax().build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        HttpPost req = new HttpPost(uri);
//        req.setVersion(new ProtocolVersion("HTTP", 1, 0)); // to disable chunk transfer encoding
        addHeaders(req, chConfiguration, requestConfig);

        boolean clientCompression = MapUtils.getFlag(requestConfig, chConfiguration, ClientConfigProperties.COMPRESS_CLIENT_REQUEST.getKey());
        boolean useHttpCompression = MapUtils.getFlag(requestConfig, chConfiguration, ClientConfigProperties.USE_HTTP_COMPRESSION.getKey());
        boolean appCompressedData = MapUtils.getFlag(requestConfig, chConfiguration, ClientConfigProperties.APP_COMPRESSED_DATA.getKey());

        req.setConfig(baseRequestConfig);
        // setting entity. wrapping if compression is enabled
        req.setEntity(wrapRequestEntity(new EntityTemplate(-1, CONTENT_TYPE, null, writeCallback),
                clientCompression, useHttpCompression, appCompressedData, lz4Factory));

        HttpClientContext context = HttpClientContext.create();

        try {
            ClassicHttpResponse httpResponse = httpClient.executeOpen(null, req, context);
            boolean serverCompression = MapUtils.getFlag(requestConfig, chConfiguration, ClientConfigProperties.COMPRESS_SERVER_RESPONSE.getKey());
            httpResponse.setEntity(wrapResponseEntity(httpResponse.getEntity(), httpResponse.getCode(), serverCompression, useHttpCompression, lz4Factory));

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
            LOG.warn("Host '{}' unknown", server.getHost());
            throw new ClientException("Unknown host", e);
        } catch (ConnectException | NoRouteToHostException e) {
            LOG.warn("Failed to connect to '{}': {}", server.getHost(), e.getMessage());
            throw new ClientException("Failed to connect", e);
        } catch (ConnectionRequestTimeoutException | ServerException | NoHttpResponseException | ClientException | SocketTimeoutException e) {
            throw e;
        } catch (Exception e) {
            throw new ClientException(e.getMessage(), e);
        }
    }

    private static final ContentType CONTENT_TYPE = ContentType.create(ContentType.TEXT_PLAIN.getMimeType(), "UTF-8");

    private void addHeaders(HttpPost req, Map<String, String> chConfig, Map<String, Object> requestConfig) {
        req.addHeader(HttpHeaders.CONTENT_TYPE, CONTENT_TYPE.getMimeType());
        if (requestConfig.containsKey(ClientConfigProperties.INPUT_OUTPUT_FORMAT.getKey())) {
            req.addHeader(ClickHouseHttpProto.HEADER_FORMAT, requestConfig.get(ClientConfigProperties.INPUT_OUTPUT_FORMAT.getKey()));
        }

        if (requestConfig.containsKey(ClientConfigProperties.QUERY_ID.getKey())) {
            req.addHeader(ClickHouseHttpProto.HEADER_QUERY_ID, requestConfig.get(ClientConfigProperties.QUERY_ID.getKey()).toString());
        }

        if(requestConfig.containsKey(ClientConfigProperties.DATABASE.getKey())) {
            req.addHeader(ClickHouseHttpProto.HEADER_DATABASE, requestConfig.get(ClientConfigProperties.DATABASE.getKey()));
        } else {
            req.addHeader(ClickHouseHttpProto.HEADER_DATABASE, chConfig.get(ClientConfigProperties.DATABASE.getKey()));
        }

        if (MapUtils.getFlag(chConfig, "ssl_authentication", false)) {
            req.addHeader(ClickHouseHttpProto.HEADER_DB_USER, chConfig.get(ClientConfigProperties.USER.getKey()));
            req.addHeader(ClickHouseHttpProto.HEADER_SSL_CERT_AUTH, "on");
        } else if (chConfig.getOrDefault(ClientConfigProperties.HTTP_USE_BASIC_AUTH.getKey(), "true").equalsIgnoreCase("true")) {
            req.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString(
                    (chConfig.get(ClientConfigProperties.USER.getKey()) + ":" + chConfig.get(ClientConfigProperties.PASSWORD.getKey())).getBytes(StandardCharsets.UTF_8)));
        } else {
            req.addHeader(ClickHouseHttpProto.HEADER_DB_USER, chConfig.get(ClientConfigProperties.USER.getKey()));
            req.addHeader(ClickHouseHttpProto.HEADER_DB_PASSWORD, chConfig.get(ClientConfigProperties.PASSWORD.getKey()));

        }
        if (proxyAuthHeaderValue != null) {
            req.addHeader(HttpHeaders.PROXY_AUTHORIZATION, proxyAuthHeaderValue);
        }

        boolean clientCompression = MapUtils.getFlag(requestConfig, chConfiguration, ClientConfigProperties.COMPRESS_CLIENT_REQUEST.getKey());
        boolean serverCompression = MapUtils.getFlag(requestConfig, chConfiguration, ClientConfigProperties.COMPRESS_SERVER_RESPONSE.getKey());
        boolean useHttpCompression = MapUtils.getFlag(requestConfig, chConfiguration, ClientConfigProperties.USE_HTTP_COMPRESSION.getKey());
        boolean appCompressedData = MapUtils.getFlag(requestConfig, chConfiguration, ClientConfigProperties.APP_COMPRESSED_DATA.getKey());

        if (useHttpCompression) {
            if (serverCompression) {
                req.addHeader(HttpHeaders.ACCEPT_ENCODING, "lz4");
            }
            if (clientCompression && !appCompressedData) {
                req.addHeader(HttpHeaders.CONTENT_ENCODING, "lz4");
            }
        }

        for (String key : chConfig.keySet()) {
            if (key.startsWith(ClientConfigProperties.HTTP_HEADER_PREFIX)) {
                req.setHeader(key.substring(ClientConfigProperties.HTTP_HEADER_PREFIX.length()), chConfig.get(key));
            }
        }

        for (String key : requestConfig.keySet()) {
            if (key.startsWith(ClientConfigProperties.HTTP_HEADER_PREFIX)) {
                Object val = requestConfig.get(key);
                if (val != null) {
                    req.setHeader(key.substring(ClientConfigProperties.HTTP_HEADER_PREFIX.length()), String.valueOf(val));
                }
            }
        }


        // Special cases
        if (req.containsHeader(HttpHeaders.AUTHORIZATION) && (req.containsHeader(ClickHouseHttpProto.HEADER_DB_USER) ||
                req.containsHeader(ClickHouseHttpProto.HEADER_DB_PASSWORD))) {
            // user has set auth header for purpose, lets remove ours
            req.removeHeaders(ClickHouseHttpProto.HEADER_DB_USER);
            req.removeHeaders(ClickHouseHttpProto.HEADER_DB_PASSWORD);
        }

        // -- keep last
        correctUserAgentHeader(req, requestConfig);
    }

    private void addQueryParams(URIBuilder req, Map<String, String> chConfig, Map<String, Object> requestConfig) {
        for (String key : chConfig.keySet()) {
            if (key.startsWith(ClientConfigProperties.SERVER_SETTING_PREFIX)) {
                req.addParameter(key.substring(ClientConfigProperties.SERVER_SETTING_PREFIX.length()), chConfig.get(key));
            }
        }

        if (requestConfig.containsKey(ClientConfigProperties.QUERY_ID.getKey())) {
            req.addParameter(ClickHouseHttpProto.QPARAM_QUERY_ID, requestConfig.get(ClientConfigProperties.QUERY_ID.getKey()).toString());
        }
        if (requestConfig.containsKey("statement_params")) {
            Map<String, Object> params = (Map<String, Object>) requestConfig.get("statement_params");
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                req.addParameter("param_" + entry.getKey(), String.valueOf(entry.getValue()));
            }
        }

        boolean clientCompression = MapUtils.getFlag(requestConfig, chConfiguration, ClientConfigProperties.COMPRESS_CLIENT_REQUEST.getKey());
        boolean serverCompression = MapUtils.getFlag(requestConfig, chConfiguration, ClientConfigProperties.COMPRESS_SERVER_RESPONSE.getKey());
        boolean useHttpCompression = MapUtils.getFlag(requestConfig, chConfiguration, ClientConfigProperties.USE_HTTP_COMPRESSION.getKey());

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

        Collection<String> sessionRoles = (Collection<String>) requestConfig.getOrDefault(ClientConfigProperties.SESSION_DB_ROLES.getKey(),
                ClientConfigProperties.valuesFromCommaSeparated(chConfiguration.getOrDefault(ClientConfigProperties.SESSION_DB_ROLES.getKey(), "")));
        if (!sessionRoles.isEmpty()) {

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
                                         boolean appControlledCompression, LZ4Factory lz4Factory) {
        LOG.debug("client compression: {}, http compression: {}", clientCompression, useHttpCompression);

        if (clientCompression && !appControlledCompression) {
            return new LZ4Entity(httpEntity, useHttpCompression, false, true,
                    MapUtils.getInt(chConfiguration, "compression.lz4.uncompressed_buffer_size"), false, lz4Factory);
        } else  {
            return httpEntity;
        }
    }

    private HttpEntity wrapResponseEntity(HttpEntity httpEntity, int httpStatus, boolean serverCompression, boolean useHttpCompression, LZ4Factory lz4Factory) {
        LOG.debug("server compression: {}, http compression: {}", serverCompression, useHttpCompression);

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
                    return new LZ4Entity(httpEntity, useHttpCompression, true, false,
                            MapUtils.getInt(chConfiguration, "compression.lz4.uncompressed_buffer_size"), true, lz4Factory);
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
        Set<ClientFaultCause> retryCauses = (Set<ClientFaultCause>)
                requestSettings.getOrDefault(ClientConfigProperties.CLIENT_RETRY_ON_FAILURE.getKey(), defaultRetryCauses);

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
                cause instanceof ConnectException) {
            return new ConnectionInitiationException(message, cause);
        }

        return new ClientException(message, cause);
    }


    /**
     * Parses URL parameters.
     * @param url
     * @return Map of parameters
     */
    public static Map<String, String> parseUrlParameters(URL url) {
        Map<String, String> params = new HashMap<>();

        try {
            String path = url.getPath();
            path = path.substring(path.indexOf('/') + 1);
            LOG.debug("path: {}", path);
            if (!path.trim().isEmpty()) {
                params.put("database", path);
            } else {
                params.put("database", "default");
            }

            String query = url.getQuery();
            if (query != null) {
                for (String pair : query.split("&")) {
                    int idx = pair.indexOf("=");
                    if (idx > 0) {
                        params.put(pair.substring(0, idx), pair.substring(idx + 1));
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to parse URL parameters", e);
        }

        return params;
    }


    private void correctUserAgentHeader(HttpRequest request, Map<String, Object> requestConfig) {
        //TODO: implement cache for user-agent
        Header userAgentHeader = request.getLastHeader(HttpHeaders.USER_AGENT);
        request.removeHeaders(HttpHeaders.USER_AGENT);

        String clientName = chConfiguration.getOrDefault(ClientConfigProperties.CLIENT_NAME.getKey(), "");
        if (requestConfig != null) {
            String reqClientName = (String) requestConfig.get(ClientConfigProperties.CLIENT_NAME.getKey());
            if (reqClientName != null && !reqClientName.isEmpty()) {
                clientName = reqClientName;
            }
        }
        String userAgentValue = defaultUserAgent;
        if (userAgentHeader == null && clientName != null && !clientName.isEmpty()) {
            userAgentValue = clientName + " " + defaultUserAgent;
        } else if (userAgentHeader != null) {
            userAgentValue = userAgentHeader.getValue() + " " + defaultUserAgent;
        }

        request.setHeader(HttpHeaders.USER_AGENT, userAgentValue);
    }

    private  String buildDefaultUserAgent() {
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

    public class MeteredManagedHttpClientConnectionFactory extends ManagedHttpClientConnectionFactory {
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
                runningAverage += times.poll();
            }

            return count > 0 ? runningAverage / count : 0;
        }
    }
}
