package com.clickhouse.client.api;

import com.clickhouse.client.api.enums.CompressionAlgorithm;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.UnaryOperator;

public class CompressionRequestUnitTest {

    private WireMockServer server;

    @BeforeClass(groups = {"unit"})
    public void startServer() {
        server = new WireMockServer(WireMockConfiguration.options()
                .dynamicPort()
                .notifier(new ConsoleNotifier(false)));
        server.start();
        server.addStubMapping(WireMock.post(WireMock.anyUrl())
                .willReturn(WireMock.aResponse().withBody("")).build());
    }

    @AfterClass(groups = {"unit"})
    public void stopServer() {
        if (server != null) {
            server.stop();
        }
    }

    @DataProvider(name = "responseCompressionRequests")
    public static Object[][] responseCompressionRequests() {
        return new Object[][]{
                // algorithm -> content coding the response is requested with (null: no compression requested)
                {CompressionAlgorithm.LZ4, "lz4"},
                {CompressionAlgorithm.ZSTD, "zstd"},
        };
    }

    @Test(groups = {"unit"}, dataProvider = "responseCompressionRequests")
    public void testResponseCompressionRequestedWithContentCoding(CompressionAlgorithm algorithm, String coding) {
        LoggedRequest request = runQuery(builder -> builder.compressionAlgorithm(algorithm), null);

        Assert.assertEquals(header(request, "Accept-Encoding"), coding);
        // the codec of the compress=1 framing is the one of the server, so it is never requested
        Assert.assertFalse(request.queryParameter("compress").isPresent(),
                "compress=1 must not be requested");
        Assert.assertTrue(request.queryParameter("enable_http_compression").isPresent());
    }

    @Test(groups = {"unit"})
    public void testCompressionFlagsDisableCompression() {
        LoggedRequest request = runQuery(builder -> builder
                .compressionAlgorithm(CompressionAlgorithm.ZSTD)
                .compressServerResponse(false)
                .compressClientRequest(false), null);

        // the algorithm selects only how a body is compressed - the flags select whether it is
        Assert.assertNull(header(request, "Accept-Encoding"));
        Assert.assertNull(header(request, "Content-Encoding"));
        Assert.assertFalse(request.queryParameter("enable_http_compression").isPresent());
        Assert.assertFalse(request.queryParameter("compress").isPresent());
        Assert.assertFalse(request.queryParameter("decompress").isPresent());
    }

    @Test(groups = {"unit"})
    public void testAlgorithmDefaultsToLz4() {
        LoggedRequest request = runQuery(builder -> builder, null);

        Assert.assertEquals(header(request, "Accept-Encoding"), "lz4");
        Assert.assertFalse(request.queryParameter("compress").isPresent());
    }

    @Test(groups = {"unit"})
    public void testOperationOverridesClientAlgorithm() {
        LoggedRequest request = runQuery(builder -> builder.compressionAlgorithm(CompressionAlgorithm.LZ4),
                new QuerySettings().compressionAlgorithm(CompressionAlgorithm.ZSTD));

        Assert.assertEquals(header(request, "Accept-Encoding"), "zstd");
    }

    @Test(groups = {"unit"})
    public void testOperationAcceptsAlgorithmName() {
        LoggedRequest request = runQuery(builder -> builder,
                (QuerySettings) new QuerySettings()
                        .setOption(ClientConfigProperties.COMPRESSION_ALGORITHM.getKey(), "zstd"));

        Assert.assertEquals(header(request, "Accept-Encoding"), "zstd");
    }

    @Test(groups = {"unit"})
    public void testRequestCompressedWithContentCodingOfAlgorithm() {
        LoggedRequest request = runQuery(builder -> builder
                .compressionAlgorithm(CompressionAlgorithm.ZSTD)
                .compressClientRequest(true)
                .useHttpCompression(true), null);

        Assert.assertEquals(header(request, "Content-Encoding"), "zstd");
    }

    @Test(groups = {"unit"})
    public void testRequestFramingStaysLz4WithoutHttpCompression() {
        LoggedRequest request = runQuery(builder -> builder
                .compressionAlgorithm(CompressionAlgorithm.ZSTD)
                .compressClientRequest(true)
                .useHttpCompression(false), null);

        // the ClickHouse framing of a request is LZ4, so the algorithm applies to the response only
        Assert.assertNull(header(request, "Content-Encoding"));
        Assert.assertEquals(header(request, "Accept-Encoding"), "zstd");
        Assert.assertTrue(request.queryParameter("enable_http_compression").isPresent(),
                "the response is compressed with the requested content coding");
        Assert.assertFalse(request.queryParameter("compress").isPresent(),
                "compress=1 must not be requested");
        Assert.assertTrue(request.queryParameter("decompress").isPresent(),
                "a request compressed without http compression keeps the ClickHouse framing");
    }

    @Test(groups = {"unit"})
    public void testInsertOperationOverridesClientAlgorithm() {
        server.resetRequests();
        try (Client client = newBuilder()
                .compressionAlgorithm(CompressionAlgorithm.LZ4)
                .compressClientRequest(true)
                .useHttpCompression(true)
                .build()) {
            try {
                client.insert("some_table",
                        new ByteArrayInputStream("1\n".getBytes(StandardCharsets.UTF_8)),
                        ClickHouseFormat.TSV,
                        new InsertSettings().compressionAlgorithm(CompressionAlgorithm.ZSTD)).get();
            } catch (Exception e) {
                // the stub answers an empty body, so only the request itself is of interest here
            }
        }

        LoggedRequest request = lastRequest();
        Assert.assertEquals(header(request, "Content-Encoding"), "zstd");
        Assert.assertEquals(header(request, "Accept-Encoding"), "zstd");
    }

    private LoggedRequest runQuery(UnaryOperator<Client.Builder> configure,
                                   QuerySettings settings) {
        server.resetRequests();
        try (Client client = configure.apply(newBuilder()).build()) {
            try {
                if (settings == null) {
                    client.query("SELECT 1").get();
                } else {
                    client.query("SELECT 1", settings).get();
                }
            } catch (Exception e) {
                // the stub answers an empty body, so only the request itself is of interest here
            }
        }

        return lastRequest();
    }

    private Client.Builder newBuilder() {
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, "localhost", server.port(), false)
                .setUsername("default")
                .setPassword("")
                .retryOnFailures();
    }

    private LoggedRequest lastRequest() {
        List<LoggedRequest> requests = server.findAll(WireMock.postRequestedFor(WireMock.anyUrl()));
        Assert.assertEquals(requests.size(), 1, "expected exactly one request");
        return requests.get(0);
    }

    private static String header(Request request, String name) {
        return request.containsHeader(name) ? request.getHeader(name) : null;
    }
}
