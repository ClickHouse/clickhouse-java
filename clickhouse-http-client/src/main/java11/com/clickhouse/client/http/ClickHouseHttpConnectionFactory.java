package com.clickhouse.client.http;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.http.config.HttpConnectionProvider;

public abstract class ClickHouseHttpConnectionFactory {
    public static ClickHouseHttpConnection createConnection(ClickHouseNode server, ClickHouseRequest<?> request,
            ExecutorService executor) throws IOException {
        HttpConnectionProvider provider = (HttpConnectionProvider) request.getConfig()
                .getOption(ClickHouseHttpOption.CONNECTION_PROVIDER);

        try {
            return provider == null || provider == HttpConnectionProvider.HTTP_URL_CONNECTION
                    ? new HttpUrlConnectionImpl(server, request, executor)
                    : new HttpClientConnectionImpl(server, request, executor);
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            return new HttpUrlConnectionImpl(server, request, executor);
        }
    }
}
