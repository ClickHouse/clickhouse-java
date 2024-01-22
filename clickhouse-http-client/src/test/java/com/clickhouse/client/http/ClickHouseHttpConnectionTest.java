package com.clickhouse.client.http;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.gss.GssAuthorizator;
import com.clickhouse.data.ClickHouseExternalTable;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;

import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseHttpConnectionTest {
    static class SimpleHttpConnection extends ClickHouseHttpConnection {
        protected SimpleHttpConnection(ClickHouseNode server, ClickHouseRequest<?> request) {
            super(server, request);
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
        Map<String, String> defaultHeaders = sc.getDefaultHeaders();
        Assert.assertTrue(!defaultHeaders.isEmpty());
        Assert.assertEquals(defaultHeaders, sc.mergeHeaders(null));

        sc = new SimpleHttpConnection(server, request.format(ClickHouseFormat.ArrowStream));
        defaultHeaders = sc.getDefaultHeaders();
        Assert.assertTrue(!defaultHeaders.isEmpty());
        Assert.assertEquals(defaultHeaders, sc.mergeHeaders(null));
    }

    @Test(groups = { "unit" })
    public void testDefaultHeadersWithGssAuth() {
        ClickHouseNode server = ClickHouseNode.builder()
                .credentials(ClickHouseCredentials.withGss("userA"))
                .addOption(ClickHouseClientOption.KERBEROS_SERVER_NAME.getKey(), "kerbServerName")
                .build();

        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(server);
        SimpleHttpConnection sc = new SimpleHttpConnection(server, request);
        Map<String, String> defaultHeaders = null;
        try (MockedConstruction<GssAuthorizator> mockAuthorizer = Mockito.mockConstruction(
                GssAuthorizator.class, (mock, context) -> {
                    Assert.assertEquals(context.arguments().get(0), "userA");
                    Assert.assertEquals(context.arguments().get(1), "kerbServerName");
                    Assert.assertEquals(context.arguments().get(2), server.getHost());
                    when(mock.getAuthToken()).thenReturn("AUTH_TOKEN_ABC");
                })) {
            defaultHeaders = sc.getDefaultHeaders();
        }

        Assert.assertTrue(!defaultHeaders.isEmpty());
        Assert.assertEquals(defaultHeaders.get("authorization"), "Negotiate AUTH_TOKEN_ABC");
        Assert.assertFalse(defaultHeaders.containsKey("x-clickhouse-user"));
        Assert.assertFalse(defaultHeaders.containsKey("x-clickhouse-key"));
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
