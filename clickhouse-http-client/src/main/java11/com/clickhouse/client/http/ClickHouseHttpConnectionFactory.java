package com.clickhouse.client.http;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.http.config.HttpConnectionProvider;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

public final class ClickHouseHttpConnectionFactory {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseHttpConnectionFactory.class);

    public static ClickHouseHttpConnection createConnection(ClickHouseNode server, ClickHouseRequest<?> request,
            ExecutorService executor) throws IOException {
        HttpConnectionProvider provider = request.getConfig().getOption(ClickHouseHttpOption.CONNECTION_PROVIDER,
                HttpConnectionProvider.class);
        if (provider == HttpConnectionProvider.APACHE_HTTP_CLIENT) {
            try {
                return new ApacheHttpConnectionImpl(server, request, executor);
            } catch (ExceptionInInitializerError | NoClassDefFoundError t) {
                log.warn("Error when creating %s, fall back to HTTP_URL_CONNECTION", provider, t);
            }
        } else if (provider == HttpConnectionProvider.HTTP_CLIENT) {
            return new HttpClientConnectionImpl(server, request, executor);
        }

        return new HttpUrlConnectionImpl(server, request, executor);
    }

    private ClickHouseHttpConnectionFactory() {
    }
}
