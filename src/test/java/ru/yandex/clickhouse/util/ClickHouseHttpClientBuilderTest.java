package ru.yandex.clickhouse.util;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;

import org.apache.http.NoHttpResponseException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.settings.ClickHouseProperties;

public class ClickHouseHttpClientBuilderTest {
    private static WireMockServer newServer() {
        int port = 8080;
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            port = serverSocket.getLocalPort();
        } catch (IOException e) {
        }

        WireMockServer server = new WireMockServer(port);
        server.start();
        server.stubFor(WireMock.post(WireMock.urlPathMatching("/*"))
                .willReturn(WireMock.aResponse().withStatus(200).withHeader("Connection", "Keep-Alive")
                        .withHeader("Content-Type", "text/plain; charset=UTF-8")
                        .withHeader("Transfer-Encoding", "chunked").withHeader("Keep-Alive", "timeout=3")
                        .withBody("OK.........................").withFixedDelay(2)));
        return server;
    }

    private static void shutDownServerWithDelay(final WireMockServer server, final long delayMs) {
        new Thread() {
            public void run() {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                server.shutdownServer();
                server.stop();
            }
        }.start();
    }

    // @Test(dependsOnMethods = { "testWithRetry" }, expectedExceptions = { NoHttpResponseException.class })
    public void testWithoutRetry() throws Exception {
        final WireMockServer server = newServer();

        ClickHouseProperties props = new ClickHouseProperties();
        props.setMaxRetries(0);
        ClickHouseHttpClientBuilder builder = new ClickHouseHttpClientBuilder(props);
        CloseableHttpClient client = builder.buildClient();
        HttpPost post = new HttpPost("http://localhost:" + server.port() + "/?db=system&query=select%201");

        shutDownServerWithDelay(server, 100);

        try {
            client.execute(post);
        } finally {
            client.close();
        }
    }

    // @Test(expectedExceptions = { HttpHostConnectException.class })
    public void testWithRetry() throws Exception {
        final WireMockServer server = newServer();

        ClickHouseProperties props = new ClickHouseProperties();
        // props.setMaxRetries(3);
        ClickHouseHttpClientBuilder builder = new ClickHouseHttpClientBuilder(props);
        CloseableHttpClient client = builder.buildClient();
        HttpPost post = new HttpPost("http://localhost:" + server.port() + "/?db=system&query=select%202");

        shutDownServerWithDelay(server, 100);

        try {
            client.execute(post);
        } finally {
            client.close();
        }
    }
}
