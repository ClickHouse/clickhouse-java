package com.clickhouse.client.api;

import com.clickhouse.client.api.enums.CompressionAlgorithm;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.QuerySettings;
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

import java.util.List;

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
                {CompressionAlgorithm.GZIP, "gzip"},
                {CompressionAlgorithm.NONE, null},
        };
    }

    @Test(groups = {"unit"}, dataProvider = "responseCompressionRequests")
    public void testResponseCompressionRequestedWithContentCoding(CompressionAlgorithm algorithm, String coding) {
        LoggedRequest request = runQuery(builder -> builder.compressionAlgorithm(algorithm), null);

        Assert.assertEquals(header(request, "Accept-Encoding"), coding);
        // the codec of the compress=1 framing is the one of the server, so it is never requested
        Assert.assertFalse(request.queryParameter("compress").isPresent(),
                "compress=1 must not be requested");
        Assert.assertEquals(request.queryParameter("enable_http_compression").isPresent(), coding != null);
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
                new QuerySettings().compressionAlgorithm(CompressionAlgorithm.GZIP));

        Assert.assertEquals(header(request, "Accept-Encoding"), "gzip");
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
                .compressionAlgorithm(CompressionAlgorithm.GZIP)
                .compressClientRequest(true)
                .useHttpCompression(true), null);

        Assert.assertEquals(header(request, "Content-Encoding"), "gzip");
    }

    private LoggedRequest runQuery(java.util.function.UnaryOperator<Client.Builder> configure,
                                   QuerySettings settings) {
        server.resetRequests();
        Client.Builder builder = new Client.Builder()
                .addEndpoint(Protocol.HTTP, "localhost", server.port(), false)
                .setUsername("default")
                .setPassword("")
                .retryOnFailures();

        try (Client client = configure.apply(builder).build()) {
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

        List<LoggedRequest> requests = server.findAll(WireMock.postRequestedFor(WireMock.anyUrl()));
        Assert.assertEquals(requests.size(), 1, "expected exactly one request");
        return requests.get(0);
    }

    private static String header(Request request, String name) {
        return request.containsHeader(name) ? request.getHeader(name) : null;
    }
}
