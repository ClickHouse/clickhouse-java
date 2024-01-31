package com.clickhouse.client.http;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ietf.jgss.GSSException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.gss.GssAuthorizationContext;
import com.clickhouse.data.ClickHouseExternalTable;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;

public class ClickHouseHttpConnectionTest {
    static class SimpleHttpConnection extends ClickHouseHttpConnection {

        protected SimpleHttpConnection(ClickHouseNode server, ClickHouseRequest<?> request, GssAuthorizationContext gssAuthContext) {
            super(server, request, gssAuthContext);
        }

        protected SimpleHttpConnection(ClickHouseNode server, ClickHouseRequest<?> request) {
            this(server, request, null);
        }

        @Override
        protected ClickHouseHttpResponse post(ClickHouseConfig config, String query, ClickHouseInputStream data,
                List<ClickHouseExternalTable> tables, ClickHouseOutputStream output, String url,
                Map<String, String> headers, Runnable postAction) throws IOException {
            return null;
        }

        @Override
        public boolean ping(int timeout) {
            return false;
        }

        @Override
        public void close() {
        }
    }

    @Test(groups = { "unit" })
    public void testBuildUrl() {
        ClickHouseNode server = ClickHouseNode.builder().port(ClickHouseProtocol.HTTP).build();
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(server);
        ClickHouseNode s = request.getServer();
        Assert.assertEquals(ClickHouseHttpConnection.buildUrl(server.getBaseUri(), request),
                "http://localhost:8123/?compress=1&extremes=0");
    }

    @Test(groups = { "unit" })
    public void testDefaultHeaders() {
        ClickHouseNode server = ClickHouseNode.builder().build();
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(server);
        SimpleHttpConnection sc = new SimpleHttpConnection(server, request);
        Assert.assertTrue(!sc.defaultHeaders.isEmpty());
        Assert.assertEquals(sc.defaultHeaders, sc.mergeHeaders(null));

        sc = new SimpleHttpConnection(server, request.format(ClickHouseFormat.ArrowStream));
        Assert.assertTrue(!sc.defaultHeaders.isEmpty());
        Assert.assertEquals(sc.defaultHeaders, sc.mergeHeaders(null));
    }

    @Test(groups = { "unit" })
    public void testDefaultHeadersWithGssAuth() throws GSSException {
        ClickHouseNode server = ClickHouseNode.builder()
                .credentials(ClickHouseCredentials.withGss("userA"))
                .addOption(ClickHouseClientOption.KERBEROS_SERVER_NAME.getKey(), "kerbServerName")
                .build();

        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(server);
        GssAuthorizationContext gssAuthMode = mock(GssAuthorizationContext.class);
        when(gssAuthMode.getAuthToken("userA", "kerbServerName", server.getHost())).thenReturn("AUTH_TOKEN_ABC");
        SimpleHttpConnection sc = new SimpleHttpConnection(server, request, gssAuthMode);
        Assert.assertFalse(sc.defaultHeaders.containsKey("authorization"));

        Map<String, String> headers = sc.mergeHeaders(null);

        assertAuthHeader(headers, "AUTH_TOKEN_ABC");
    }

    @Test(groups = { "unit" })
    public void testCustomHeadersWithGssAuth() throws GSSException {
        ClickHouseNode server = ClickHouseNode.builder()
                .credentials(ClickHouseCredentials.withGss("userB"))
                .addOption(ClickHouseClientOption.KERBEROS_SERVER_NAME.getKey(), "kerbServerNameB")
                .build();

        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(server);
        GssAuthorizationContext gssAuthMode = mock(GssAuthorizationContext.class);
        when(gssAuthMode.getAuthToken("userB", "kerbServerNameB", server.getHost())).thenReturn("AUTH_TOKEN_ABCD");
        SimpleHttpConnection sc = new SimpleHttpConnection(server, request, gssAuthMode);
        Assert.assertFalse(sc.defaultHeaders.containsKey("authorization"));

        Map<String, String> customHeaders = new HashMap<>();
        customHeaders.put("Content-type", "application/json");

        Map<String, String> headers = sc.mergeHeaders(customHeaders);

        assertAuthHeader(headers, "AUTH_TOKEN_ABCD");
    }

    private void assertAuthHeader(Map<String, String> headers, String token) {
        Assert.assertTrue(!headers.isEmpty());
        Assert.assertEquals(headers.get("authorization"), "Negotiate " + token);
        Assert.assertFalse(headers.containsKey("x-clickhouse-user"));
        Assert.assertFalse(headers.containsKey("x-clickhouse-key"));
    }

    @Test(groups = { "unit" })
    public void testGetBaseUrl() {
        ClickHouseNode server = ClickHouseNode.of("https://localhost/db1");
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(server);
        Assert.assertEquals(ClickHouseHttpConnection.buildUrl(server.getBaseUri(), request),
                "https://localhost:8443/?compress=1&extremes=0");
        try (SimpleHttpConnection c = new SimpleHttpConnection(server, request)) {
            Assert.assertEquals(c.getBaseUrl(), "https://localhost:8443/");
        }
    }
}
