package com.clickhouse.client.http;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.gss.GssAuthorizationContext;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.http.config.HttpConnectionProvider;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

public final class ClickHouseHttpConnectionFactory {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseHttpConnectionFactory.class);

    public static ClickHouseHttpConnection createConnection(ClickHouseNode server, ClickHouseRequest<?> request,
            ExecutorService executor, GssAuthorizationContext gssAuthContext) throws IOException {
        HttpConnectionProvider provider = request.getConfig().getOption(ClickHouseHttpOption.CONNECTION_PROVIDER,
                HttpConnectionProvider.class);
        if (provider == HttpConnectionProvider.APACHE_HTTP_CLIENT) {
            try {
                return new ApacheHttpConnectionImpl(server, request, executor, gssAuthContext);
            } catch (ExceptionInInitializerError | NoClassDefFoundError t) {
                log.warn("Error when creating %s, fall back to HTTP_URL_CONNECTION", provider, t);
            }
        } else if (provider == HttpConnectionProvider.HTTP_CLIENT) {
            log.warn("HTTP_CLIENT is only supported in JDK 11 or above, fall back to HTTP_URL_CONNECTION");
        }

        return new HttpUrlConnectionImpl(server, request, executor, gssAuthContext);
    }

    private ClickHouseHttpConnectionFactory() {
    }
}
