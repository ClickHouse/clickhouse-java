package ru.yandex.clickhouse.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;

import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ssl.NonValidatingTrustManager;

public class ClickHouseHttpClientBuilder {

    private static final ClickHouseCookieStoreProvider cookieStoreProvider = new ClickHouseCookieStoreProvider();
    private final ClickHouseProperties properties;

    public ClickHouseHttpClientBuilder(ClickHouseProperties properties) {
        this.properties = properties;
    }

    public CloseableHttpClient buildClient() throws Exception {
        HttpClientBuilder builder = HttpClientBuilder.create()
                .setConnectionManager(getConnectionManager())
                .setRetryHandler(getRequestRetryHandler())
                .setConnectionReuseStrategy(getConnectionReuseStrategy())
                .setDefaultConnectionConfig(getConnectionConfig())
                .setDefaultRequestConfig(getRequestConfig())
                .setDefaultHeaders(getDefaultHeaders())
                .setDefaultCredentialsProvider(getDefaultCredentialsProvider())
                .disableContentCompression() // gzip is not needed. Use lz4 when compress=1
                .setDefaultCookieStore(cookieStoreProvider.getCookieStore(properties))
                .disableRedirectHandling();

        String clientName = properties != null ? properties.getClientName() : null;
        if (!Utils.isNullOrEmptyString(clientName)) {
            builder.setUserAgent(clientName);
        }
        
        return builder.build();
    }

    private HttpRequestRetryHandler getRequestRetryHandler() {
        final int maxRetries = properties.getMaxRetries();
        return new DefaultHttpRequestRetryHandler(maxRetries, false) {
            @Override
            public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
                if (executionCount > maxRetries || context == null 
                    || !Boolean.TRUE.equals(context.getAttribute("is_idempotent"))) {
                    return false;
                }

                return (exception instanceof NoHttpResponseException) || super.retryRequest(exception, executionCount, context);
            }
        };
    }

    public static HttpClientContext createClientContext(ClickHouseProperties props) {
        if (props == null
            || !isConfigurationValidForAuth(props))
        {
            return HttpClientContext.create();
        }
        AuthCache authCache = new BasicAuthCache();
        BasicScheme basicAuth = new BasicScheme();
        authCache.put(getTargetHost(props), basicAuth);
        HttpClientContext ctx = HttpClientContext.create();
        ctx.setAuthCache(authCache);
        ctx.setCookieStore(cookieStoreProvider.getCookieStore(props));
        return ctx;
    }

    private ConnectionReuseStrategy getConnectionReuseStrategy() {
        return new DefaultConnectionReuseStrategy() {
            @Override
            public boolean keepAlive(HttpResponse httpResponse, HttpContext httpContext) {
                if (httpResponse.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
                    return false;
                }
                return super.keepAlive(httpResponse, httpContext);
            }
        };
    }

    private PoolingHttpClientConnectionManager getConnectionManager()
        throws CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException {
        RegistryBuilder<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
          .register("http", PlainConnectionSocketFactory.getSocketFactory());

        if (properties.getSsl()) {
            HostnameVerifier verifier = "strict".equals(properties.getSslMode()) ? SSLConnectionSocketFactory.getDefaultHostnameVerifier() : NoopHostnameVerifier.INSTANCE;
            registry.register("https", new SSLConnectionSocketFactory(getSSLContext(), verifier));
        }

        //noinspection resource
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(
            registry.build(),
            null,
            null,
            new IpVersionPriorityResolver(),
            properties.getTimeToLiveMillis(),
            TimeUnit.MILLISECONDS
        );

        connectionManager.setDefaultMaxPerRoute(properties.getDefaultMaxPerRoute());
        connectionManager.setMaxTotal(properties.getMaxTotal());
        connectionManager.setDefaultConnectionConfig(getConnectionConfig());
        return connectionManager;
    }

    private ConnectionConfig getConnectionConfig() {
        return ConnectionConfig.custom()
                .setBufferSize(properties.getApacheBufferSize())
                .build();
    }

    private RequestConfig getRequestConfig() {
        return RequestConfig.custom()
                .setSocketTimeout(properties.getSocketTimeout())
                .setConnectTimeout(properties.getConnectionTimeout())
                .setConnectionRequestTimeout(properties.getConnectionTimeout())
                .setCookieSpec(CookieSpecs.STANDARD)
                .build();
    }

    private Collection<Header> getDefaultHeaders() {
        List<Header> headers = new ArrayList<>();
        if (properties.getHttpAuthorization() != null) {
            headers.add(new BasicHeader(HttpHeaders.AUTHORIZATION, properties.getHttpAuthorization()));
        }
        return headers;
    }

    private SSLContext getSSLContext()
      throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException {
      SSLContext ctx = SSLContext.getInstance("TLS");
      TrustManager[] tms = null;
      KeyManager[] kms = null;
      SecureRandom sr = null;

      if(properties.getSslMode().equals("none")) {
          tms = new TrustManager[]{new NonValidatingTrustManager()};
          kms = new KeyManager[]{};
          sr = new SecureRandom();
      } else if (properties.getSslMode().equals("strict")) {
          if (!properties.getSslRootCertificate().isEmpty()) {
              TrustManagerFactory tmf = TrustManagerFactory
                  .getInstance(TrustManagerFactory.getDefaultAlgorithm());

              tmf.init(getKeyStore());
              tms = tmf.getTrustManagers();
              kms = new KeyManager[]{};
              sr = new SecureRandom();
          }
      } else {
          throw new IllegalArgumentException("unknown ssl mode '"+ properties.getSslMode() +"'");
      }

      ctx.init(kms, tms, sr);
      return ctx;
  }

  private KeyStore getKeyStore()
      throws NoSuchAlgorithmException, IOException, CertificateException, KeyStoreException {
      KeyStore ks;
      try {
          ks = KeyStore.getInstance("jks");
          ks.load(null, null); // needed to initialize the key store
      } catch (KeyStoreException e) {
          throw new NoSuchAlgorithmException("jks KeyStore not available");
      }

      InputStream caInputStream;
      try {
        caInputStream = new FileInputStream(properties.getSslRootCertificate());
      } catch (FileNotFoundException ex) {
          ClassLoader cl = Thread.currentThread().getContextClassLoader();
          caInputStream = cl.getResourceAsStream(properties.getSslRootCertificate());
          if(caInputStream == null) {
              throw new IOException(
                  "Could not open SSL/TLS root certificate file '" + properties
                      .getSslRootCertificate() + "'", ex);
          }
      }

      try {
          CertificateFactory cf = CertificateFactory.getInstance("X.509");
          Iterator<? extends Certificate> caIt = cf.generateCertificates(caInputStream).iterator();
          for (int i = 0; caIt.hasNext(); i++) {
              ks.setCertificateEntry("cert" + i, caIt.next());
          }
          return ks;
      } finally {
          caInputStream.close();
      }
  }

    private CredentialsProvider getDefaultCredentialsProvider() {
        if (!isConfigurationValidForAuth(properties)) {
            return null;
        }
        HttpHost targetHost = getTargetHost(properties);
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
            new AuthScope(targetHost.getHostName(), targetHost.getPort()),
            new UsernamePasswordCredentials(
                properties.getUser() != null  ? properties.getUser() : "default",
                properties.getPassword() != null ? properties.getPassword() : ""));
        return credsProvider;
    }

    private static HttpHost getTargetHost(ClickHouseProperties props) {
        return new HttpHost(
            props.getHost(),
            props.getPort(),
            props.getSsl() ? "https" : "http");
    }

    private static boolean isConfigurationValidForAuth(ClickHouseProperties props) {
        return props.getHost() != null
            && props.getHttpAuthorization() == null
            && (props.getUser() != null || props.getPassword() != null);
    }
}
