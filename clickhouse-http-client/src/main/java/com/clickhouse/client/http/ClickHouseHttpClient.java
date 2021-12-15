package com.clickhouse.client.http;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.clickhouse.client.AbstractClient;
import com.clickhouse.client.ClickHouseCluster;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.config.ClickHouseOption;
import com.clickhouse.client.data.ClickHouseStreamResponse;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;

public class ClickHouseHttpClient extends AbstractClient<ClickHouseHttpConnection> {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseHttpClient.class);

    @Override
    protected boolean checkConnection(ClickHouseHttpConnection connection, ClickHouseNode requestServer,
            ClickHouseNode currentServer, ClickHouseRequest<?> request) {
        // return false to suggest creating a new connection
        return connection != null && connection.isReusable() && requestServer.equals(currentServer);
    }

    @Override
    protected ClickHouseHttpConnection newConnection(ClickHouseHttpConnection connection, ClickHouseNode server,
            ClickHouseRequest<?> request) {
        if (connection != null && connection.isReusable()) {
            closeConnection(connection, false);
        }

        try {
            return ClickHouseHttpConnectionFactory.createConnection(server, request, getExecutor());
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

    protected ClickHouseResponse postRequest(ClickHouseRequest<?> sealedRequest) throws IOException {
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
        ClickHouseHttpResponse httpResponse = conn.post(sql, sealedRequest.getInputStream().orElse(null),
                sealedRequest.getExternalTables(), null);
        return ClickHouseStreamResponse.of(httpResponse.getConfig(sealedRequest), httpResponse,
                sealedRequest.getSettings(), null,
                httpResponse.summary);
    }

    @Override
    public boolean accept(ClickHouseProtocol protocol) {
        return ClickHouseProtocol.HTTP == protocol || super.accept(protocol);
    }

    @Override
    public CompletableFuture<ClickHouseResponse> execute(ClickHouseRequest<?> request) {
        // sealedRequest is an immutable copy of the original request
        final ClickHouseRequest<?> sealedRequest = request.seal();

        if (sealedRequest.getConfig().isAsync()) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return postRequest(sealedRequest);
                } catch (IOException e) {
                    throw new CompletionException(ClickHouseException.of(e, sealedRequest.getServer()));
                }
            }, getExecutor());
        } else {
            try {
                return CompletableFuture.completedFuture(postRequest(sealedRequest));
            } catch (IOException e) {
                return failedResponse(ClickHouseException.of(e, sealedRequest.getServer()));
            }
        }
    }

    @Override
    public final Class<? extends ClickHouseOption> getOptionClass() {
        return ClickHouseHttpOption.class;
    }

    @Override
    public boolean ping(ClickHouseNode server, int timeout) {
        if (server != null) {
            server = ClickHouseCluster.probe(server, timeout);
            return getConnection(connect(server)).ping(timeout);
        }

        return false;
    }
}
