package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.SSLMode;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.hc.client5.http.io.HttpClientConnectionOperator;
import org.apache.hc.client5.http.io.ManagedHttpClientConnection;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.config.CharCodingConfig;
import org.apache.hc.core5.http.config.Http1Config;
import org.apache.hc.core5.http.impl.io.DefaultHttpResponseParserFactory;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.net.NamedEndpoint;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MeteredConnectionFactoryTest {
    private static final long DELAY_MILLIS = 250;

    private static HttpAPIClientHelper.MeteredManagedHttpClientConnectionFactory newFactory() {
        return new HttpAPIClientHelper.MeteredManagedHttpClientConnectionFactory(
                Http1Config.DEFAULT, CharCodingConfig.DEFAULT, DefaultHttpResponseParserFactory.INSTANCE);
    }

    @Test(groups = {"unit"})
    public void testConnectionTimesNotRecordedWithoutMetrics() throws Exception {
        HttpAPIClientHelper.MeteredManagedHttpClientConnectionFactory factory = newFactory();
        factory.recordConnectTime(1_000_000L);
        for (int i = 0; i < 10; i++) {
            factory.createConnection(new Socket());
        }
        Assert.assertEquals(factory.times.size(), 0, "Connection times must not accumulate when metrics are disabled");
    }

    @Test(groups = {"unit"})
    public void testConnectionTimesRecordedAndDrainedWithMetrics() throws Exception {
        HttpAPIClientHelper.MeteredManagedHttpClientConnectionFactory factory = newFactory();
        factory.enableMetrics();
        factory.recordConnectTime(500_000L);
        Assert.assertEquals(factory.times.size(), 1);
        Assert.assertEquals(factory.getAverageTimeInMillis(), 0.5, 0.0);
        Assert.assertEquals(factory.times.size(), 0);
    }

    @Test(groups = {"unit"})
    public void testConnectionTimesNotRecordedAfterMetricsDisabled() throws Exception {
        HttpAPIClientHelper.MeteredManagedHttpClientConnectionFactory factory = newFactory();
        factory.enableMetrics();
        factory.createConnection(new Socket());
        Assert.assertEquals(factory.times.size(), 0, "Connection object creation must not record a connect time");
        factory.disableMetrics();
        Assert.assertEquals(factory.times.size(), 0);
        factory.recordConnectTime(1_000_000L);
        Assert.assertEquals(factory.times.size(), 0, "Connection times must not accumulate once metrics are disabled");
    }

    @Test(groups = {"unit"})
    public void testDelayedTcpConnectIsIncludedInConnectionEstablishmentTime() throws Exception {
        HttpAPIClientHelper.MeteredManagedHttpClientConnectionFactory factory = newFactory();
        factory.enableMetrics();
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            TestConnectionOperator delegate = new TestConnectionOperator(() -> {
                try (Socket socket = new DelayedConnectSocket(DELAY_MILLIS)) {
                    socket.connect(server.getLocalSocketAddress(), 1_000);
                }
            });
            HttpAPIClientHelper.MeteredHttpClientConnectionOperator operator =
                    new HttpAPIClientHelper.MeteredHttpClientConnectionOperator(delegate, factory);

            operator.connect(null, null, null, null, null, Timeout.ofSeconds(1), null, null, null);

            Assert.assertTrue(delegate.connected, "Connection operator did not connect the socket");
            try (Socket accepted = server.accept()) {
                Assert.assertTrue(accepted.isConnected());
            }
            Assert.assertTrue(factory.getAverageTimeInMillis() >= DELAY_MILLIS - 50,
                    "Connection establishment time should include the delayed TCP connect");
        }
    }

    @Test(groups = {"unit"})
    public void testDelayedTlsHandshakeIsIncludedInConnectionEstablishmentTime() throws Exception {
        HttpAPIClientHelper.MeteredManagedHttpClientConnectionFactory factory = newFactory();
        factory.enableMetrics();
        SSLContext serverContext = createServerSslContext();
        SSLContext clientContext = createTrustAllSslContext();
        try (SSLServerSocket server = (SSLServerSocket) serverContext.getServerSocketFactory()
                .createServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<?> handshake = executor.submit(() -> {
                    try (SSLSocket socket = (SSLSocket) server.accept()) {
                        Thread.sleep(DELAY_MILLIS);
                        socket.startHandshake();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                TestConnectionOperator delegate = new TestConnectionOperator(() -> {
                    try (SSLSocket socket = (SSLSocket) clientContext.getSocketFactory().createSocket()) {
                        socket.connect(server.getLocalSocketAddress(), 1_000);
                        socket.startHandshake();
                    }
                });
                HttpAPIClientHelper.MeteredHttpClientConnectionOperator operator =
                        new HttpAPIClientHelper.MeteredHttpClientConnectionOperator(delegate, factory);

                operator.connect(null, null, null, null, null, Timeout.ofSeconds(1), null, null, null);
                handshake.get(5, TimeUnit.SECONDS);

                Assert.assertTrue(delegate.connected, "Connection operator did not complete the TLS handshake");
                Assert.assertTrue(factory.getAverageTimeInMillis() >= DELAY_MILLIS - 50,
                        "Connection establishment time should include the delayed TLS handshake");
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @DataProvider(name = "connectionMetricSchemes")
    public Object[][] connectionMetricSchemes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "connectionMetricSchemes", groups = {"unit"})
    public void testConnectionMetricThroughClient(boolean secure) {
        WireMockServer server = new WireMockServer(WireMockConfiguration.options()
                .dynamicPort()
                .dynamicHttpsPort()
                .notifier(new ConsoleNotifier(false)));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            server.start();
            server.stubFor(WireMock.post(WireMock.anyUrl()).willReturn(WireMock.ok("1")));
            String scheme = secure ? "https" : "http";
            int port = secure ? server.httpsPort() : server.port();

            Client.Builder builder = new Client.Builder()
                    .addEndpoint(scheme + "://localhost:" + port)
                    .enableConnectionPool(true)
                    .registerClientMetrics(registry, "connection-metric-test");
            if (secure) {
                builder.setSSLMode(SSLMode.TRUST);
            }

            try (Client client = builder.build()) {
                Assert.assertTrue(client.ping(), "The client request should succeed");
                Gauge times = registry.get("httpcomponents.httpclient.connect.time").gauge();
                Assert.assertTrue(times.value() > 0, "A new connection should record its establishment time");

                Assert.assertTrue(client.ping(), "The request on the pooled connection should succeed");
                Assert.assertEquals(times.value(), 0.0, 0.0,
                        "Reusing a pooled connection should not record another sample");
            }
            server.verify(2, WireMock.postRequestedFor(WireMock.anyUrl()));
        } finally {
            registry.close();
            server.stop();
        }
    }

    private static class TestConnectionOperator implements HttpClientConnectionOperator {
        private final ConnectAction action;
        private boolean connected;

        private TestConnectionOperator(ConnectAction action) {
            this.action = action;
        }

        @Override
        public void connect(ManagedHttpClientConnection connection, HttpHost host,
                            java.net.InetSocketAddress localAddress, TimeValue connectTimeout,
                            SocketConfig socketConfig, HttpContext context) {
            throw new AssertionError("Unexpected legacy connect overload");
        }

        @Override
        public void connect(ManagedHttpClientConnection connection, HttpHost host, NamedEndpoint namedEndpoint,
                            java.nio.file.Path path, java.net.InetSocketAddress localAddress, Timeout connectTimeout,
                            SocketConfig socketConfig, Object attachment, HttpContext context) throws IOException {
            action.run();
            connected = true;
        }

        @Override
        public void upgrade(ManagedHttpClientConnection connection, HttpHost host, HttpContext context) {
        }
    }

    private static class DelayedConnectSocket extends Socket {
        private final long delayMillis;

        private DelayedConnectSocket(long delayMillis) {
            this.delayMillis = delayMillis;
        }

        @Override
        public void connect(SocketAddress endpoint, int timeout) throws IOException {
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            super.connect(endpoint, timeout);
        }
    }

    private static SSLContext createServerSslContext() throws Exception {
        String privateKeyPem = readTestResource("containers/clickhouse-server/certs/localhost.key");
        byte[] privateKeyBytes = Base64.getMimeDecoder().decode(privateKeyPem
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", ""));
        PrivateKey privateKey = KeyFactory.getInstance("RSA")
                .generatePrivate(new PKCS8EncodedKeySpec(privateKeyBytes));
        Certificate certificate;
        try (InputStream input = MeteredConnectionFactoryTest.class.getClassLoader()
                .getResourceAsStream("containers/clickhouse-server/certs/localhost.crt")) {
            if (input == null) {
                throw new IOException("Missing test TLS certificate");
            }
            certificate = CertificateFactory.getInstance("X.509").generateCertificate(input);
        }

        char[] password = "test".toCharArray();
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        keyStore.setKeyEntry("server", privateKey, password, new Certificate[] { certificate });
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, password);

        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keyManagerFactory.getKeyManagers(), null, new SecureRandom());
        return context;
    }

    private static SSLContext createTrustAllSslContext() throws Exception {
        TrustManager[] trustManagers = new TrustManager[] {
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                }
        };
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, trustManagers, new SecureRandom());
        return context;
    }

    private static String readTestResource(String path) throws IOException {
        try (InputStream input = MeteredConnectionFactoryTest.class.getClassLoader().getResourceAsStream(path)) {
            if (input == null) {
                throw new IOException("Missing test resource: " + path);
            }
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[4_096];
            int read;
            while ((read = input.read(buffer)) != -1) {
                output.write(buffer, 0, read);
            }
            return new String(output.toByteArray(), StandardCharsets.US_ASCII);
        }
    }

    private interface ConnectAction {
        void run() throws IOException;
    }
}
