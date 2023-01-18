package com.clickhouse.client.http;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().connect(server);
        ClickHouseNode s = request.getServer();
        Assert.assertEquals(ClickHouseHttpConnection.buildUrl(server.getBaseUri(), request),
                "http://localhost:8123/?compress=1&extremes=0");

        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server.getBaseUri(),
                        request.option(ClickHouseHttpOption.WEB_CONTEXT, "")),
                "http://localhost:8123/?compress=1&extremes=0");
        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server.getBaseUri(),
                        request.option(ClickHouseHttpOption.WEB_CONTEXT, "/")),
                "http://localhost:8123/?compress=1&extremes=0");
        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server.getBaseUri(),
                        request.option(ClickHouseHttpOption.WEB_CONTEXT, "./")),
                "http://localhost:8123/./?compress=1&extremes=0");
        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server.getBaseUri(),
                        request.option(ClickHouseHttpOption.WEB_CONTEXT, ".")),
                "http://localhost:8123/.?compress=1&extremes=0");
        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server.getBaseUri(),
                        request.option(ClickHouseHttpOption.WEB_CONTEXT, "?")),
                "http://localhost:8123/?compress=1&extremes=0");
        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server.getBaseUri(),
                        request.option(ClickHouseHttpOption.WEB_CONTEXT, "#")),
                "http://localhost:8123/?compress=1&extremes=0");
        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server.getBaseUri(),
                        request.option(ClickHouseHttpOption.WEB_CONTEXT, "/a/b/c/d/")),
                "http://localhost:8123/a/b/c/d/?compress=1&extremes=0");
        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server.getBaseUri(),
                        request.option(ClickHouseHttpOption.WEB_CONTEXT, "/a?b=1#c")),
                "http://localhost:8123/a?compress=1&extremes=0");
        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server.getBaseUri(),
                        request.option(ClickHouseHttpOption.WEB_CONTEXT, "/a#c")),
                "http://localhost:8123/a?compress=1&extremes=0");
        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server.getBaseUri(),
                        request.option(ClickHouseHttpOption.WEB_CONTEXT, "/a%20c")),
                "http://localhost:8123/a%20c?compress=1&extremes=0");
        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server.getBaseUri(),
                        request.option(ClickHouseHttpOption.WEB_CONTEXT, "///.//")),
                "http://localhost:8123/./?compress=1&extremes=0");
    }

    @Test(groups = { "unit" })
    public void testDefaultHeaders() {
        ClickHouseNode server = ClickHouseNode.builder().build();
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().connect(server);
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
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().connect(server);
        Assert.assertEquals(ClickHouseHttpConnection.buildUrl(server.getBaseUri(), request),
                "https://localhost:8443/?compress=1&extremes=0");
        try (SimpleHttpConnection c = new SimpleHttpConnection(server, request)) {
            Assert.assertEquals(c.getBaseUrl(), "https://localhost:8443/");
        }
        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server.getBaseUri(),
                        request.option(ClickHouseHttpOption.WEB_CONTEXT, "/a/b/c")),
                "https://localhost:8443/a/b/c?compress=1&extremes=0");
        try (SimpleHttpConnection c = new SimpleHttpConnection(server, request)) {
            Assert.assertEquals(c.getBaseUrl(), "https://localhost:8443/a/b/c/");
        }
        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server.getBaseUri(),
                        request.option(ClickHouseHttpOption.WEB_CONTEXT, "a?b=1")),
                "https://localhost:8443/a?compress=1&extremes=0");
        try (SimpleHttpConnection c = new SimpleHttpConnection(server, request)) {
            Assert.assertEquals(c.getBaseUrl(), "https://localhost:8443/a/");
        }
    }
}
