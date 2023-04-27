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

        try {
            return provider == null || provider == HttpConnectionProvider.HTTP_URL_CONNECTION
                    ? new HttpUrlConnectionImpl(server, request, executor)
                    : provider == HttpConnectionProvider.HTTP_CLIENT
                        ? new HttpClientConnectionImpl(server, request, executor)
                        : new ApacheHttpConnectionImpl(server, request, executor);
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            log.warn("Error when creating http client %s, will use HTTP_URL_CONNECTION", provider.name(), t);
            return new HttpUrlConnectionImpl(server, request, executor);
        }
    }

    private ClickHouseHttpConnectionFactory() {
    }
}
