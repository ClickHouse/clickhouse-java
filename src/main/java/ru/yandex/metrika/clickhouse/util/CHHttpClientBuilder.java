package ru.yandex.metrika.clickhouse.util;

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
import ru.yandex.metrika.clickhouse.copypaste.HttpConnectionProperties;
import ru.yandex.metrika.clickhouse.copypaste.IpVersionPriorityResolver;

import java.net.HttpURLConnection;
import java.util.concurrent.TimeUnit;

/**
 * Created by jkee on 19.03.15.
 */
public class CHHttpClientBuilder {

    private final HttpConnectionProperties properties;

    public CHHttpClientBuilder(HttpConnectionProperties properties) {
        this.properties = properties;
    }

    public CloseableHttpClient buildClient() {
        return HttpClientBuilder.create()
                .setConnectionManager(getConnectionManager())
                .setKeepAliveStrategy(createKeepAliveStrategy())
                .setDefaultConnectionConfig(getConnectionConfig())
                .setDefaultRequestConfig(getRequestConfig())
                .build();
    }

    private static PoolingHttpClientConnectionManager getConnectionManager() {
        //noinspection resource
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(
                RegistryBuilder.<ConnectionSocketFactory>create()
                        .register("http", PlainConnectionSocketFactory.getSocketFactory())
                        .register("https", SSLConnectionSocketFactory.getSocketFactory())
                        .build(),
                null, null, new IpVersionPriorityResolver(), 1, TimeUnit.MINUTES);
        connectionManager.setDefaultMaxPerRoute(500);
        connectionManager.setMaxTotal(1000);
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
                // при ошибках keep-alive не всегда правильно работает, на всякий случай закроем коннекшн
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
