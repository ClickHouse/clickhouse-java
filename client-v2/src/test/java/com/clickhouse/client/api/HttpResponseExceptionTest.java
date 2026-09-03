package com.clickhouse.client.api;

import com.clickhouse.client.api.http.ClickHouseHttpProto;
import com.clickhouse.client.api.query.QueryResponse;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.apache.hc.core5.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class HttpResponseExceptionTest {

    @Test
    public void shouldThrowServerExceptionWhileReadingSuccessfulResponse() throws Exception {
        String exceptionTag = "0123456789abcdef";
        String queryId = "mid-stream-timeout";
        byte[] resultPrefix = "result-data".getBytes(StandardCharsets.UTF_8);
        String errorMessage = "Code: 159. DB::Exception: Timeout exceeded. (TIMEOUT_EXCEEDED)";
        String exceptionFrame = "\r\n__exception__\r\n" + exceptionTag + "\r\n" + errorMessage
                + "\r\n" + errorMessage.getBytes(StandardCharsets.UTF_8).length + " " + exceptionTag
                + "\r\n__exception__\r\n";
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(resultPrefix);
        body.write(exceptionFrame.getBytes(StandardCharsets.UTF_8));

        WireMockServer mockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        mockServer.start();

        try {
            mockServer.stubFor(WireMock.post(WireMock.anyUrl())
                    .willReturn(WireMock.aResponse()
                            .withStatus(HttpStatus.SC_OK)
                            .withHeader("X-ClickHouse-Exception-Tag", exceptionTag)
                            .withHeader(ClickHouseHttpProto.HEADER_QUERY_ID, queryId)
                            .withBody(body.toByteArray())));

            try (Client client = new Client.Builder()
                    .addEndpoint("http://localhost:" + mockServer.port())
                    .setUsername("default")
                    .setPassword("")
                    .setDefaultDatabase("default")
                    .compressServerResponse(false)
                    .useHttpCompression(false)
                    .build();
                 QueryResponse response = client.query("SELECT 1").get(10, TimeUnit.SECONDS);
                 InputStream input = response.getInputStream()) {
                byte[] actualPrefix = new byte[resultPrefix.length];
                new DataInputStream(input).readFully(actualPrefix);
                Assert.assertEquals(actualPrefix, resultPrefix);

                ServerException exception = Assert.expectThrows(ServerException.class, input::read);
                Assert.assertEquals(exception.getCode(), 159);
                Assert.assertEquals(exception.getTransportProtocolCode(), HttpStatus.SC_OK);
                Assert.assertEquals(exception.getQueryId(), queryId);
                Assert.assertTrue(exception.getMessage().startsWith(errorMessage), exception.getMessage());
            }
        } finally {
            mockServer.stop();
        }
    }
}
