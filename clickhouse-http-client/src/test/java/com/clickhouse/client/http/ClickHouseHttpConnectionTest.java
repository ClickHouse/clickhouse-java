package com.clickhouse.client.http;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.client.http.config.ClickHouseHttpOption;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseHttpConnectionTest {
    static class SimpleHttpConnection extends ClickHouseHttpConnection {
        protected SimpleHttpConnection(ClickHouseNode server, ClickHouseRequest<?> request) {
            super(server, request);
        }

        @Override
        protected ClickHouseHttpResponse post(String query, InputStream data, List<ClickHouseExternalTable> tables,
                Map<String, String> headers) throws IOException {
            return null;
        }

        @Override
        public boolean ping(int timeout) {
            return false;
        }

        @Override
        public void close() throws Exception {
        }
    }

    @Test(groups = { "unit" })
    public void testDefaultHeaders() throws Exception {
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
    public void testBuildUrl() throws Exception {
        ClickHouseNode server = ClickHouseNode.builder().build();
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().connect(server);
        Assert.assertEquals(ClickHouseHttpConnection.buildUrl(server, request),
                "http://localhost:8123/?compress=1&extremes=0");

        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server, request.option(ClickHouseHttpOption.WEB_CONTEXT, "")),
                "http://localhost:8123/?compress=1&extremes=0");
        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server, request.option(ClickHouseHttpOption.WEB_CONTEXT, "/")),
                "http://localhost:8123/?compress=1&extremes=0");
        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server, request.option(ClickHouseHttpOption.WEB_CONTEXT, ".")),
                "http://localhost:8123/.?compress=1&extremes=0");
        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server, request.option(ClickHouseHttpOption.WEB_CONTEXT, "./")),
                "http://localhost:8123/./?compress=1&extremes=0");
        Assert.assertEquals(
                ClickHouseHttpConnection.buildUrl(server, request.option(ClickHouseHttpOption.WEB_CONTEXT, "///.//")),
                "http://localhost:8123/./?compress=1&extremes=0");
    }
}
