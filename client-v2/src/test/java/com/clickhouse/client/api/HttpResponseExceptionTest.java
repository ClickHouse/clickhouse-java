package com.clickhouse.client.api;

import com.clickhouse.client.api.http.ClickHouseHttpProto;
import com.clickhouse.client.api.internal.ClickHouseLZ4OutputStream;
import com.clickhouse.client.api.query.QueryResponse;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import net.jpountz.lz4.LZ4Factory;
import org.apache.hc.core5.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class HttpResponseExceptionTest {

    @DataProvider(name = "responseCompression")
    public Object[][] responseCompression() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "responseCompression")
    public void shouldThrowServerExceptionWhileReadingSuccessfulResponse(boolean compressedResponse) throws Exception {
        String exceptionTag = "0123456789abcdef";
        String queryId = "mid-stream-timeout";
        byte[] resultPrefix = "result-data".getBytes(StandardCharsets.UTF_8);
        String errorMessage = "Code: 159. DB::Exception: Timeout exceeded. (TIMEOUT_EXCEEDED)\n";
        String exceptionFrame = "\r\n__exception__\r\n" + exceptionTag + "\r\n" + errorMessage
                + errorMessage.getBytes(StandardCharsets.UTF_8).length + " " + exceptionTag
                + "\r\n__exception__\r\n";
        byte[] body = responseBody(resultPrefix, exceptionFrame.getBytes(StandardCharsets.UTF_8), compressedResponse);

        WireMockServer mockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        mockServer.start();

        try {
            mockServer.stubFor(WireMock.post(WireMock.anyUrl())
                    .willReturn(WireMock.aResponse()
                            .withStatus(HttpStatus.SC_OK)
                            .withHeader("X-ClickHouse-Exception-Tag", exceptionTag)
                            .withHeader(ClickHouseHttpProto.HEADER_QUERY_ID, queryId)
                            .withBody(body)));

            try (Client client = new Client.Builder()
                    .addEndpoint("http://localhost:" + mockServer.port())
                    .setUsername("default")
                    .setPassword("")
                    .setDefaultDatabase("default")
                    .compressServerResponse(compressedResponse)
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
                Assert.assertTrue(exception.getMessage().startsWith(errorMessage.trim()), exception.getMessage());
            }
        } finally {
            mockServer.stop();
        }
    }

    @DataProvider(name = "abortedResponses")
    public Object[][] abortedResponses() {
        return new Object[][] {{159, false}, {60, false}, {159, true}, {60, true}};
    }

    @Test(dataProvider = "abortedResponses")
    public void shouldRecognizeCompleteFrameWithoutFinalHttpChunk(int code, boolean compressed) throws Exception {
        String tag = "0123456789abcdef";
        String message = "Code: " + code + ". DB::Exception: Query failed — 错误.\n";
        String frame = "\r\n__exception__\r\n" + tag + "\r\n" + message
                + message.getBytes(StandardCharsets.UTF_8).length + " " + tag + "\r\n__exception__\r\n";
        byte[] prefix = "result-data".getBytes(StandardCharsets.UTF_8);
        byte[] body = responseBody(prefix, frame.getBytes(StandardCharsets.UTF_8), compressed);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
            server.setSoTimeout(10000);
            Future<?> sent = executor.submit(() -> {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(10000);
                    BufferedReader request = new BufferedReader(new InputStreamReader(
                            socket.getInputStream(), StandardCharsets.US_ASCII));
                    int contentLength = 0;
                    String line;
                    while ((line = request.readLine()) != null && !line.isEmpty()) {
                        if (line.regionMatches(true, 0, "Content-Length:", 0, 15)) {
                            contentLength = Integer.parseInt(line.substring(15).trim());
                        }
                    }
                    for (int i = 0; i < contentLength; i++) {
                        if (request.read() < 0) {
                            throw new IOException("Incomplete request");
                        }
                    }
                    OutputStream output = socket.getOutputStream();
                    output.write(("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
                            + "X-ClickHouse-Exception-Tag: " + tag + "\r\n\r\n"
                            + Integer.toHexString(body.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
                    output.write(body);
                    output.write("\r\n".getBytes(StandardCharsets.US_ASCII));
                    output.flush(); // Deliberately close without the terminating HTTP chunk.
                }
                return null;
            });
            try (Client client = new Client.Builder().addEndpoint("http://localhost:" + server.getLocalPort())
                    .compressServerResponse(compressed).useHttpCompression(false).build();
                 QueryResponse response = client.query("SELECT 1").get(10, TimeUnit.SECONDS);
                 InputStream input = response.getInputStream()) {
                byte[] actual = new byte[prefix.length];
                new DataInputStream(input).readFully(actual);
                Assert.assertEquals(actual, prefix);
                ServerException exception = Assert.expectThrows(ServerException.class, input::read);
                Assert.assertEquals(exception.getCode(), code);
                Assert.assertEquals(exception.getMessage(), message.trim());
            }
            sent.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static byte[] responseBody(byte[] resultPrefix, byte[] exceptionFrame, boolean compressed)
            throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        if (!compressed) {
            body.write(resultPrefix);
            body.write(exceptionFrame);
            return body.toByteArray();
        }

        try (ClickHouseLZ4OutputStream output = new ClickHouseLZ4OutputStream(body,
                LZ4Factory.fastestInstance().fastCompressor(), ClickHouseLZ4OutputStream.UNCOMPRESSED_BUFF_SIZE)) {
            output.write(resultPrefix);
            output.flush();
            output.write(exceptionFrame);
        }
        return body.toByteArray();
    }
}
