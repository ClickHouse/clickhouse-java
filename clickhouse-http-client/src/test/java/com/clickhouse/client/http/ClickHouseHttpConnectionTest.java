package com.clickhouse.client.http;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.data.ClickHouseExternalTable;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import org.apache.hc.core5.net.URIBuilder;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ClickHouseHttpConnectionTest {
    static class SimpleHttpConnection extends ClickHouseHttpConnection {
        protected SimpleHttpConnection(ClickHouseNode server, ClickHouseRequest<?> request) {
            super(server, request, Collections.emptyMap());
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
        Assert.assertEquals(ClickHouseHttpConnection.buildUrl(server.getBaseUri(), request, Collections.emptyMap()),
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
    public void testGetBaseUrl() {
        ClickHouseNode server = ClickHouseNode.of("https://localhost/db1");
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(server);
        Assert.assertEquals(ClickHouseHttpConnection.buildUrl(server.getBaseUri(), request, Collections.emptyMap()),
                "https://localhost:8443/?compress=1&extremes=0");
        try (SimpleHttpConnection c = new SimpleHttpConnection(server, request)) {
            Assert.assertEquals(c.getBaseUrl(), "https://localhost:8443/");
        }
    }

    @Test(groups = { "unit" })
    public void testReferer() {

        ClickHouseNode server = ClickHouseNode.of("https://localhost/db1");
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(server);

        try (SimpleHttpConnection sc = new SimpleHttpConnection(server, request)) {
            Assert.assertFalse(sc.defaultHeaders.containsKey("referer"));
        }

        request.option(ClickHouseHttpOption.SEND_HTTP_CLIENT_ID, "IP_ADDRESS");
        try (SimpleHttpConnection sc = new SimpleHttpConnection(server, request)) {
            Assert.assertTrue(sc.defaultHeaders.containsKey("referer"));
            Assert.assertEquals(ClickHouseHttpClient.LOCAL_HOST.address, sc.defaultHeaders.get("referer"));
        }

        request.option(ClickHouseHttpOption.SEND_HTTP_CLIENT_ID, "HOST_NAME");
        try (SimpleHttpConnection sc = new SimpleHttpConnection(server, request)) {
            Assert.assertTrue(sc.defaultHeaders.containsKey("referer"));
            Assert.assertEquals(ClickHouseHttpClient.LOCAL_HOST.hostName, sc.defaultHeaders.get("referer"));
        }
    }

    @Test(groups = { "unit" }, dataProvider = "roles")
    public void testRolesQParam(String role) {
        ClickHouseNode server = ClickHouseNode.of("https://localhost/db1");
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(server);
        Map<String, Serializable> additionalParams = Collections.singletonMap("_roles", (Serializable) Sets.newHashSet(role));
        String url = ClickHouseHttpConnection.buildUrl(server.getBaseUri(), request, additionalParams);

        try {
            URIBuilder uriBuilder = new URIBuilder(new URI(url));
            String queryRole = uriBuilder.getQueryParams().stream()
                .filter(p -> "role".equals(p.getName()))
                .findFirst()
                .map(p -> p.getValue())
                .orElse(null);
            Assert.assertEquals(role, queryRole);
        } catch (Exception e) {
            Assert.fail("Failed to build URL with roles query parameter", e);
        }
    }

    @DataProvider(name = "roles")
    private static Object[][] getRolesQParamArguments() {
        return new Object[][] {
            { "ROLE1" },
            { "ROl2,," },
            { "role☺," },
            { "ROL3∕" },

        };
    }
}
