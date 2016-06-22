package ru.yandex.clickhouse.util;

import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import ru.yandex.clickhouse.settings.CHProperties;

import java.net.HttpURLConnection;
import java.util.concurrent.TimeUnit;

/**
 * Created by jkee on 19.03.15.
 */
public class CHHttpClientBuilder {

    private final CHProperties properties;

    public CHHttpClientBuilder(CHProperties properties) {
        this.properties = properties;
    }

    public CloseableHttpClient buildClient() {
        return HttpClientBuilder.create()
                .setConnectionManager(getConnectionManager())
                .setKeepAliveStrategy(createKeepAliveStrategy())
                .setDefaultConnectionConfig(getConnectionConfig())
                .setDefaultRequestConfig(getRequestConfig())
                .disableContentCompression() // gzip здесь ни к чему. Используется lz4 при compress=1
                .build();
    }

    private PoolingHttpClientConnectionManager getConnectionManager() {
        //noinspection resource
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(
                RegistryBuilder.<ConnectionSocketFactory>create()
                        .register("http", PlainConnectionSocketFactory.getSocketFactory())
                        .register("https", SSLConnectionSocketFactory.getSocketFactory())
                        .build(),
                null, null, new IpVersionPriorityResolver(), properties.getTimeToLiveMillis(), TimeUnit.MILLISECONDS);
        connectionManager.setDefaultMaxPerRoute(properties.getDefaultMaxPerRoute());
        connectionManager.setMaxTotal(properties.getMaxTotal());
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
                .build();
    }

    private ConnectionKeepAliveStrategy createKeepAliveStrategy() {
        return new ConnectionKeepAliveStrategy() {
            @Override
            public long getKeepAliveDuration(HttpResponse httpResponse, HttpContext httpContext) {
                // in case of errors keep-alive not always works. close connection just in case
                if (httpResponse.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
                    return -1;
                }
                HeaderElementIterator it = new BasicHeaderElementIterator(
                        httpResponse.headerIterator(HTTP.CONN_DIRECTIVE));
                while (it.hasNext()) {
                    HeaderElement he = it.nextElement();
                    String param = he.getName();
                    //String value = he.getValue();
                    if (param != null && param.equalsIgnoreCase(HTTP.CONN_KEEP_ALIVE)) {
                        return properties.getKeepAliveTimeout();
                    }
                }
                return -1;
            }
        };
    }

}
