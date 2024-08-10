package com.clickhouse.client.api.internal;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseSslContextProvider;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.enums.ProxyType;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.http.ApacheHttpConnectionImpl;
import com.clickhouse.client.http.ClickHouseHttpProto;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.auth.CredentialsProviderBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
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
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.NoHttpResponseException;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.io.entity.EntityTemplate;
import org.apache.hc.core5.io.IOCallback;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocketFactory;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Base64;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class HttpAPIClientHelper {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private static int ERROR_BODY_BUFFER_SIZE = 1024; // Error messages are usually small

    private CloseableHttpClient httpClient;

    private Map<String, String> chConfiguration;

    private RequestConfig baseRequestConfig;

    private String proxyAuthHeaderValue;

    public HttpAPIClientHelper(Map<String, String> configuration) {
        this.chConfiguration = configuration;
        this.httpClient = createHttpClient();

        RequestConfig.Builder reqConfBuilder = RequestConfig.custom();
        MapUtils.applyLong(chConfiguration, ClickHouseClientOption.CONNECTION_TIMEOUT.getKey(),
                (t) -> reqConfBuilder.setConnectionRequestTimeout(t, TimeUnit.MILLISECONDS));

        this.baseRequestConfig = reqConfBuilder.build();

        boolean usingClientCompression=  chConfiguration.getOrDefault(ClickHouseClientOption.DECOMPRESS.getKey(), "false").equalsIgnoreCase("true");
        boolean usingServerCompression=  chConfiguration.getOrDefault(ClickHouseClientOption.COMPRESS.getKey(), "false").equalsIgnoreCase("true");
        boolean useHttpCompression = chConfiguration.getOrDefault("client.use_http_compression", "false").equalsIgnoreCase("true");
        LOG.info("client compression: {}, server compression: {}, http compression: {}", usingClientCompression, usingServerCompression, useHttpCompression);
    }

    public CloseableHttpClient createHttpClient() {

        // Top Level builders
        HttpClientBuilder clientBuilder = HttpClientBuilder.create();


        // Socket configuration
        SocketConfig.Builder soCfgBuilder = SocketConfig.custom();
        MapUtils.applyInt(chConfiguration, ClickHouseClientOption.SOCKET_TIMEOUT.getKey(),
                (t) -> soCfgBuilder.setSoTimeout(t, TimeUnit.MILLISECONDS));
        MapUtils.applyInt(chConfiguration, ClickHouseClientOption.SOCKET_RCVBUF.getKey(),
                soCfgBuilder::setRcvBufSize);
        MapUtils.applyInt(chConfiguration, ClickHouseClientOption.SOCKET_SNDBUF.getKey(),
                soCfgBuilder::setSndBufSize);


        // Connection manager
        PoolingHttpClientConnectionManagerBuilder connMgrBuilder = PoolingHttpClientConnectionManagerBuilder.create();

        ClickHouseSslContextProvider sslContextProvider = ClickHouseSslContextProvider.getProvider();

        String trustStorePath = chConfiguration.get(ClickHouseClientOption.TRUST_STORE.getKey());
        SSLContext sslContext = null;
        if (trustStorePath != null ) {
            try {
                sslContext = sslContextProvider.getSslContextFromKeyStore(
                        trustStorePath,
                        chConfiguration.get(ClickHouseClientOption.KEY_STORE_PASSWORD.getKey()),
                        chConfiguration.get(ClickHouseClientOption.KEY_STORE_TYPE.getKey())
                );
            } catch (SSLException e) {
                throw new ClientMisconfigurationException("Failed to create SSL context from a keystore", e);
            }
        } else if (chConfiguration.get(ClickHouseClientOption.SSL_ROOT_CERTIFICATE.getKey()) != null ||
                chConfiguration.get(ClickHouseClientOption.SSL_CERTIFICATE.getKey()) != null ||
                chConfiguration.get(ClickHouseClientOption.SSL_KEY.getKey()) != null) {

            try {
                sslContext = sslContextProvider.getSslContextFromCerts(
                        chConfiguration.get(ClickHouseClientOption.SSL_CERTIFICATE.getKey()),
                        chConfiguration.get(ClickHouseClientOption.SSL_KEY.getKey()),
                        chConfiguration.get(ClickHouseClientOption.SSL_ROOT_CERTIFICATE.getKey())
                );
            } catch (SSLException e) {
                throw new ClientMisconfigurationException("Failed to create SSL context from certificates", e);
            }
        }
        if (sslContext !=null) {
            connMgrBuilder.setSSLSocketFactory(new SSLConnectionSocketFactory(sslContext));
        }

        connMgrBuilder.setDefaultSocketConfig(soCfgBuilder.build());
        clientBuilder.setConnectionManager(connMgrBuilder.build());

        // Proxy
        String proxyHost = chConfiguration.get(ClickHouseClientOption.PROXY_HOST.getKey());
        String proxyPort = chConfiguration.get(ClickHouseClientOption.PROXY_PORT.getKey());
        HttpHost proxy = null;
        if (proxyHost != null && proxyPort != null) {
            proxy = new HttpHost(proxyHost, Integer.parseInt(proxyPort));
        }

        String proxyTypeVal = chConfiguration.get(ClickHouseClientOption.PROXY_TYPE.getKey());
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

        return clientBuilder.build();
    }

    /**
     * Reads status line and if error tries to parse response body to get server error message.
     *
     * @param httpResponse - HTTP response
     * @return
     */
    public Exception readError(ClassicHttpResponse httpResponse) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(ERROR_BODY_BUFFER_SIZE)) {
            httpResponse.getEntity().writeTo(out);
            String message = out.toString();
            int serverCode = getHeaderInt(httpResponse.getFirstHeader(ClickHouseHttpProto.HEADER_EXCEPTION_CODE), 0);
            return new ServerException(serverCode, message);
        } catch (IOException e) {
            throw new ClientException("Failed to read response body", e);
        }
    }

    public ClassicHttpResponse executeRequest(ClickHouseNode server, Map<String, Object> requestConfig,
                                             IOCallback<OutputStream> writeCallback) throws IOException {
//            HttpHost target = new HttpHost("https", server.getHost(), server.getPort());

        URI uri;
        try {
            URIBuilder uriBuilder = new URIBuilder(server.getBaseUri());
            addQueryParams(uriBuilder, chConfiguration, requestConfig);
            uri = uriBuilder.build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        HttpPost req = new HttpPost(uri);
        addHeaders(req, chConfiguration, requestConfig);


        RequestConfig httpReqConfig = RequestConfig.copy(baseRequestConfig)
                .build();
        req.setConfig(httpReqConfig);
        // setting entity. wrapping if compression is enabled
        req.setEntity(wrapEntity(new EntityTemplate(-1, CONTENT_TYPE, null, writeCallback)));

        HttpClientContext context = HttpClientContext.create();

        try {
            ClassicHttpResponse httpResponse = httpClient.executeOpen(null, req, context);
            if (httpResponse.getCode() == HttpStatus.SC_PROXY_AUTHENTICATION_REQUIRED) {
                throw new ClientMisconfigurationException("Proxy authentication required. Please check your proxy settings.");
            } else if (httpResponse.getCode() >= HttpStatus.SC_BAD_REQUEST &&
                    httpResponse.getCode() < HttpStatus.SC_SERVER_ERROR) {
                try {
                    throw readError(httpResponse);
                } finally {
                    httpResponse.close();
                }
            } else if (httpResponse.getCode() == HttpStatus.SC_BAD_GATEWAY) {
                httpResponse.close();
                throw new ClientException("Server returned '502 Bad gateway'. Check network and proxy settings.");
            } else if (httpResponse.getCode() >= HttpStatus.SC_INTERNAL_SERVER_ERROR) {
                httpResponse.close();
                return httpResponse;
            }

            httpResponse.setEntity(wrapEntity(httpResponse.getEntity()));
            return httpResponse;

        } catch (UnknownHostException e) {
            LOG.warn("Host '{}' unknown", server.getHost());
            throw new ClientException("Unknown host", e);
        } catch (ConnectException | NoRouteToHostException e) {
            LOG.warn("Failed to connect to '{}': {}", server.getHost(), e.getMessage());
            throw new ClientException("Failed to connect", e);
        } catch (ServerException | NoHttpResponseException | ConnectionRequestTimeoutException | ClientException e) {
            throw e;
        } catch (Exception e) {
            throw new ClientException("Failed to execute request", e);
        }
    }

    private static final ContentType CONTENT_TYPE = ContentType.create(ContentType.TEXT_PLAIN.getMimeType(), "UTF-8");

    private void addHeaders(HttpPost req, Map<String, String> chConfig, Map<String, Object> requestConfig) {
        req.addHeader(HttpHeaders.CONTENT_TYPE, CONTENT_TYPE.getMimeType());
        if (requestConfig != null) {
            if (requestConfig.containsKey(ClickHouseClientOption.FORMAT.getKey())) {
                req.addHeader(ClickHouseHttpProto.HEADER_FORMAT, requestConfig.get(ClickHouseClientOption.FORMAT.getKey()));
            }
        }
        req.addHeader(ClickHouseHttpProto.HEADER_DATABASE, chConfig.get(ClickHouseClientOption.DATABASE.getKey()));
        req.addHeader(ClickHouseHttpProto.HEADER_DB_USER, chConfig.get(ClickHouseDefaults.USER.getKey()));
        req.addHeader(ClickHouseHttpProto.HEADER_DB_PASSWORD, chConfig.get(ClickHouseDefaults.PASSWORD.getKey()));

        if (proxyAuthHeaderValue != null) {
            req.addHeader(HttpHeaders.PROXY_AUTHORIZATION, proxyAuthHeaderValue);
        }

        boolean serverCompression = chConfiguration.getOrDefault(ClickHouseClientOption.COMPRESS.getKey(), "false").equalsIgnoreCase("true");
        boolean clientCompression = chConfiguration.getOrDefault(ClickHouseClientOption.DECOMPRESS.getKey(), "false").equalsIgnoreCase("true");
        boolean useHttpCompression = chConfiguration.getOrDefault("client.use_http_compression", "false").equalsIgnoreCase("true");

        if (useHttpCompression) {
            if (serverCompression) {
                req.addHeader(HttpHeaders.ACCEPT_ENCODING, "lz4");
            }
            if (clientCompression) {
                req.addHeader(HttpHeaders.CONTENT_ENCODING, "lz4");
            }
        }
    }
    private void addQueryParams(URIBuilder req, Map<String, String> chConfig, Map<String, Object> requestConfig) {
        if (requestConfig != null) {
            if (requestConfig.containsKey(ClickHouseHttpOption.WAIT_END_OF_QUERY.getKey())) {
                req.addParameter(ClickHouseHttpOption.WAIT_END_OF_QUERY.getKey(),
                        requestConfig.get(ClickHouseHttpOption.WAIT_END_OF_QUERY.getKey()).toString());
            }
            if (requestConfig.containsKey(ClickHouseClientOption.QUERY_ID.getKey())) {
                req.addParameter(ClickHouseHttpProto.QPARAM_QUERY_ID, requestConfig.get(ClickHouseClientOption.QUERY_ID.getKey()).toString());
            }
            if (requestConfig.containsKey("statement_params")) {
                Map<String, Object> params = (Map<String, Object>) requestConfig.get("statement_params");
                for (Map.Entry<String, Object> entry : params.entrySet()) {
                    req.addParameter("param_" + entry.getKey(), String.valueOf(entry.getValue()));
                }
            }
        }

        boolean serverCompression = chConfiguration.getOrDefault(ClickHouseClientOption.COMPRESS.getKey(), "false").equalsIgnoreCase("true");
        boolean clientCompression = chConfiguration.getOrDefault(ClickHouseClientOption.DECOMPRESS.getKey(), "false").equalsIgnoreCase("true");
        boolean useHttpCompression = chConfiguration.getOrDefault("client.use_http_compression", "false").equalsIgnoreCase("true");


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
    }

    private HttpEntity wrapEntity(HttpEntity httpEntity) {
        boolean serverCompression = chConfiguration.getOrDefault(ClickHouseClientOption.COMPRESS.getKey(), "false").equalsIgnoreCase("true");
        boolean clientCompression = chConfiguration.getOrDefault(ClickHouseClientOption.DECOMPRESS.getKey(), "false").equalsIgnoreCase("true");
        boolean useHttpCompression = chConfiguration.getOrDefault("client.use_http_compression", "false").equalsIgnoreCase("true");
        if (serverCompression || clientCompression) {
            return new LZ4Entity(httpEntity, useHttpCompression, serverCompression, clientCompression,
                    MapUtils.getInt(chConfiguration, "compression.lz4.uncompressed_buffer_size"));
        } else {
            return httpEntity;
        }
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
}
