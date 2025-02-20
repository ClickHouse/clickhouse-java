package com.clickhouse.client.http;

import com.clickhouse.client.AbstractClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseStreamResponse;
import com.clickhouse.client.ClickHouseTransaction;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

@Deprecated
public class ClickHouseHttpClient extends AbstractClient<ClickHouseHttpConnection> {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseHttpClient.class);

    static final List<ClickHouseProtocol> SUPPORTED = Collections.singletonList(ClickHouseProtocol.HTTP);

    public static class HostNameAndAddress{
        public String hostName;
        public String address;
    }

    private static HostNameAndAddress getLocalHost() {
        // get local address but not localhost
        HostNameAndAddress hostNameAndAddress = new HostNameAndAddress();
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();

            outer:
            for (NetworkInterface ni : Collections.list(networkInterfaces)) {
                Enumeration<InetAddress> inetAddresses = ni.getInetAddresses();
                for (InetAddress ia : Collections.list(inetAddresses)) {
                    // We just use the first non-loopback address
                    if (!ia.isLoopbackAddress() && !ia.isLinkLocalAddress()) {
                        hostNameAndAddress.address = ia.getHostAddress();
                        hostNameAndAddress.hostName = ia.getCanonicalHostName();
                        break outer;
                    }
                }
            }
        } catch (SocketException e) {
            // ignore
        }
        return hostNameAndAddress;
    }

    public static String getReferer(ClickHouseConfig config) {
        String referer = null;
        if (!ClickHouseChecker.isNullOrEmpty(config.getStrOption(ClickHouseHttpOption.SEND_HTTP_CLIENT_ID)))
        {
            if (ClickHouseChecker.isNullOrEmpty(ClickHouseOption.toKeyValuePairs(config.getStrOption(ClickHouseHttpOption.CUSTOM_HEADERS)).get("referer"))) {
                if (config.getStrOption(ClickHouseHttpOption.SEND_HTTP_CLIENT_ID).equals("HOST_NAME"))
                    referer = LOCAL_HOST.hostName;
                else
                    referer = LOCAL_HOST.address;
            }
        }
        return referer;
    }

    public static HostNameAndAddress LOCAL_HOST = null;

    public ClickHouseHttpClient() {
        synchronized (this) {
            if (LOCAL_HOST == null)
                LOCAL_HOST = getLocalHost();
        }
    }

    protected ConcurrentSkipListSet<String> roles = new ConcurrentSkipListSet<>();

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

        try {

            return ClickHouseHttpConnectionFactory.createConnection(server, request, getExecutor(), buildAdditionalReqParams(request));
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

    private Map<String, Serializable> buildAdditionalReqParams(ClickHouseRequest<?> sealedRequest) {
        ClickHouseConfig config = sealedRequest.getConfig();
        if (config.getBoolOption(ClickHouseHttpOption.REMEMBER_LAST_SET_ROLES)) {
            if (!(sealedRequest.hasSetting("_set_roles_stmt") || roles.isEmpty())) {
                return Collections.singletonMap("_roles", roles);
            }
        }
        return Collections.emptyMap();
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

        ClickHouseNode server = sealedRequest.getServer();
        if (conn.isReusable()) {
            Map<String, Serializable> additionalParams = buildAdditionalReqParams(sealedRequest);

            httpResponse = conn.post(config, sql, sealedRequest.getInputStream().orElse(null),
                    sealedRequest.getExternalTables(), sealedRequest.getOutputStream().orElse(null),
                    ClickHouseHttpConnection.buildUrl(server.getBaseUri(), sealedRequest, additionalParams),
                    ClickHouseHttpConnection.createDefaultHeaders(config, server, conn.getUserAgent(), getReferer(config)),
                    postAction);
        } else {
            httpResponse = conn.post(config, sql, sealedRequest.getInputStream().orElse(null),
                    sealedRequest.getExternalTables(), sealedRequest.getOutputStream().orElse(null),
                    null, ClickHouseHttpConnection.createDefaultHeaders(config, server, conn.getUserAgent(), getReferer(config)),
                    postAction);
        }

        if (config.getBoolOption(ClickHouseHttpOption.REMEMBER_LAST_SET_ROLES)) {
            // At this point only successful responses are expected
            if (sealedRequest.hasSetting("_set_roles_stmt")) {
                rememberRoles((Set<String>) sealedRequest.getSettings().get("_set_roles_stmt"));
            }
        }


        return ClickHouseStreamResponse.of(httpResponse.getConfig(sealedRequest), httpResponse.getInputStream(),
                sealedRequest.getSettings(), null, httpResponse.summary, httpResponse.getTimeZone());
    }

    @Override
    public final Class<? extends ClickHouseOption> getOptionClass() {
        return ClickHouseHttpOption.class;
    }

    public void rememberRoles(Set<String> requestedRoles) {
        roles.clear();
        if (requestedRoles != null) {
            roles.addAll(requestedRoles.stream().filter(r -> !"NONE".equalsIgnoreCase(r)).collect(Collectors.toList()));
        }
    }

    public Set<String> getRoles() {
        return roles;
    }

    public void clearRoles() {
        roles.clear();
    }
}
