package com.clickhouse.client.http;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import com.clickhouse.client.AbstractClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseTransaction;
import com.clickhouse.client.gss.GssAuthorizationContext;
import com.clickhouse.client.ClickHouseStreamResponse;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

public class ClickHouseHttpClient extends AbstractClient<ClickHouseHttpConnection> {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseHttpClient.class);

    static final List<ClickHouseProtocol> SUPPORTED = Collections.singletonList(ClickHouseProtocol.HTTP);

    @Override
    protected boolean checkConnection(ClickHouseHttpConnection connection, ClickHouseNode requestServer,
            ClickHouseNode currentServer, ClickHouseRequest<?> request) {
        // return false to suggest creating a new connection
        return connection != null && connection.isReusable() && requestServer.isSameEndpoint(currentServer);
    }

    @Override
    protected boolean checkHealth(ClickHouseNode server, int timeout) {
        return getConnection(read(server)).ping(timeout);
    }

    @Override
    protected Collection<ClickHouseProtocol> getSupportedProtocols() {
        return SUPPORTED;
    }

    @Override
    protected ClickHouseHttpConnection newConnection(ClickHouseHttpConnection connection, ClickHouseNode server,
            ClickHouseRequest<?> request) {
        if (connection != null && connection.isReusable()) {
            closeConnection(connection, false);
        }

        GssAuthorizationContext gssAuthContext = null;
        if (connection != null) {
            gssAuthContext = connection.getGssAuthorizationContext();
        } else {
            gssAuthContext = GssAuthorizationContext.initialize();
        }
        try {
            return ClickHouseHttpConnectionFactory.createConnection(server, request, getExecutor(), gssAuthContext);
        } catch (IOException e) {
            throw new CompletionException(e);
        }
    }

    @Override
    protected void closeConnection(ClickHouseHttpConnection connection, boolean force) {
        try {
            connection.close();
        } catch (Exception e) {
            log.warn("Failed to close http connection due to: %s", e.getMessage());
        }
    }

    protected String buildQueryParams(Map<String, String> params) {
        if (params == null || params.isEmpty()) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            builder.append(ClickHouseHttpConnection.urlEncode(entry.getKey(), StandardCharsets.UTF_8)).append('=')
                    .append(ClickHouseHttpConnection.urlEncode(entry.getValue(), StandardCharsets.UTF_8)).append('&');
        }

        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    @Override
    protected ClickHouseResponse send(ClickHouseRequest<?> sealedRequest) throws ClickHouseException, IOException {
        ClickHouseHttpConnection conn = getConnection(sealedRequest);

        List<String> stmts = sealedRequest.getStatements(false);
        int size = stmts.size();
        String sql;
        if (size == 0) {
            throw new IllegalArgumentException("At least one SQL statement is required for execution");
        } else if (size > 1) {
            throw new IllegalArgumentException("Expect one SQL statement to execute but we got " + size);
        } else {
            sql = stmts.get(0);
        }

        log.debug("Query: %s", sql);
        final ClickHouseConfig config = sealedRequest.getConfig();
        final ClickHouseHttpResponse httpResponse;
        final ClickHouseTransaction tx = sealedRequest.getTransaction();
        final Runnable postAction = tx != null && tx.isImplicit()
                ? () -> {
                    try {
                        tx.commit();
                    } catch (ClickHouseException e) {
                        throw new UncheckedIOException(new IOException(e.getMessage()));
                    }
                }
                : null;
        if (conn.isReusable()) {
            ClickHouseNode server = sealedRequest.getServer();
            httpResponse = conn.post(config, sql, sealedRequest.getInputStream().orElse(null),
                    sealedRequest.getExternalTables(), sealedRequest.getOutputStream().orElse(null),
                    ClickHouseHttpConnection.buildUrl(server.getBaseUri(), sealedRequest),
                    ClickHouseHttpConnection.createDefaultHeaders(config, server, conn.getUserAgent()),
                    postAction);
        } else {
            httpResponse = conn.post(config, sql, sealedRequest.getInputStream().orElse(null),
                    sealedRequest.getExternalTables(), sealedRequest.getOutputStream().orElse(null), null, null,
                    postAction);
        }
        return ClickHouseStreamResponse.of(httpResponse.getConfig(sealedRequest), httpResponse.getInputStream(),
                sealedRequest.getSettings(), null, httpResponse.summary);
    }

    @Override
    public final Class<? extends ClickHouseOption> getOptionClass() {
        return ClickHouseHttpOption.class;
    }
}
