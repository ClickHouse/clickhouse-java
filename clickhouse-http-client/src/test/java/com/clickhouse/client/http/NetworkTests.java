package com.clickhouse.client.http;

import com.clickhouse.client.AbstractSocketClient;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseSocketFactory;
import com.clickhouse.client.ClickHouseSslContextProvider;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.config.ClickHouseSslMode;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseUtils;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.DefaultHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import wiremock.Run;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.Socket;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Map;

/**
 * This tests is only for development and manual testing
 */
@Test(groups = {"integration"})
public class NetworkTests {
    private static final String NGINX_IMAGE = "nginx:alpine";
    private static final int NGINX_SSL_PORT = 8443;
    private static final String NODE1_HOST = "node1.test";
    private static final String NODE2_HOST = "node2.test";

    private GenericContainer<?> nginxContainer;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
    }

    @BeforeClass
    public void setUp() throws Exception {

        // Create Nginx container with custom configuration
        nginxContainer = new GenericContainer<>(NGINX_IMAGE)
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("nginx.conf"),
                        "/etc/nginx/nginx.conf"
                )
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("certs"),
                        "/etc/nginx/certs/"
                )
                .withExposedPorts(NGINX_SSL_PORT)
                .waitingFor(Wait.forListeningPort());

        nginxContainer.start();
    }

    @AfterClass
    public void tearDown() {
        if (nginxContainer != null) {
            nginxContainer.stop();
        }
    }

    private String getNginxHost() {
        return nginxContainer.getHost();
    }

    private int getNginxPort() {
        return nginxContainer.getMappedPort(NGINX_SSL_PORT);
    }

    @Test
    public void testSNI() {
        // Test will be implemented here
        String host = getNginxHost();
        int port = getNginxPort();
        System.out.println("Nginx container running at: " + host + ":" + port);
    }

    @Test(dataProvider = "testSNINodesDP")
    void testSNINodes(String host) throws Exception {
        int port = nginxContainer.getMappedPort(NGINX_SSL_PORT);
        SSLSocket socket = createSniSocket(host, "localhost", port);
        socket.startHandshake();

        SSLSession session = socket.getSession();
        X509Certificate cert = (X509Certificate) session.getPeerCertificates()[0];
        Assert.assertTrue(cert.getSubjectX500Principal().getName().contains("CN=" + host));
    }

    @DataProvider
    static Object[][] testSNINodesDP() {
        return new Object[][]{
                {NODE1_HOST},
                {NODE2_HOST}
        };
    }

    private SSLSocket createSniSocket(String sniHost, String serverHost, int port) throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, new TrustManager[]{new X509TrustManager() {
            public void checkClientTrusted(X509Certificate[] chain, String authType) {
            }

            public void checkServerTrusted(X509Certificate[] chain, String authType) {
            }

            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        }}, null);

        SSLSocketFactory factory = context.getSocketFactory();
        SSLSocket socket = (SSLSocket) factory.createSocket(serverHost, port);

        SSLParameters sslParams = socket.getSSLParameters();
        sslParams.setServerNames(Collections.singletonList(new SNIHostName(sniHost)));
        socket.setSSLParameters(sslParams);

        return socket;
    }

    @Test(groups = {"integration"})
    void testClientConfiguration() throws Exception {
        ClickHouseNode gateway = ClickHouseNode.of("https://" + getNginxHost() + ":" + getNginxPort() + "/?sslmode=none");
        try (ClickHouseClient client = ClickHouseClient.builder()
                .option(ClickHouseDefaults.USER, "default")
                .option(ClickHouseDefaults.PASSWORD, "")
                .option(ClickHouseClientOption.SSL_MODE, ClickHouseSslMode.NONE)
                .option(ClickHouseClientOption.COMPRESS, false) ///  we emulate servers so need plain text
                .option(ClickHouseClientOption.SSL_SNI_MAPPING, "127.0.1.1=nodeX.test,localhost=node2.test")
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build()) {



            try (ClickHouseResponse response = client.read(gateway)
                    .format(ClickHouseFormat.TabSeparated)
                    .query("SELECT hostname()").executeAndWait()) {
//                ClickHouseRecord record = response.firstRecord();
                String serverHostname = response.firstRecord().getValue(0).asString();
                Assert.assertEquals(serverHostname, "node2.test");
            };
        }
    }


    @Test(groups = {"integration"})
    void testCustomSSLConnectionFactory() throws Exception {
        ClickHouseNode gateway = ClickHouseNode.of("https://" + getNginxHost() + ":" + getNginxPort() + "/?sslmode=none");
        try (ClickHouseClient client = ClickHouseClient.builder()
                .option(ClickHouseDefaults.USER, "default")
                .option(ClickHouseDefaults.PASSWORD, "")
                .option(ClickHouseClientOption.CUSTOM_SOCKET_FACTORY_OPTIONS, "default_sni=node1.sni")
                .option(ClickHouseClientOption.CUSTOM_SOCKET_FACTORY, SNIAwareSSLSocketFactory.class.getName())
                .option(ClickHouseClientOption.SSL_MODE, ClickHouseSslMode.NONE)
                .option(ClickHouseClientOption.COMPRESS, false) ///  we emulate servers so need plain text

                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build()) {



            try (ClickHouseResponse response = client.read(gateway)
                    .format(ClickHouseFormat.TabSeparated)
                    .query("SELECT hostname()").executeAndWait()) {
//                ClickHouseRecord record = response.firstRecord();
                String serverHostname = response.firstRecord().getValue(0).asString();
                Assert.assertEquals(serverHostname, "node2.test");
            };
        }
    }

    public static class SNIAwareSSLSocketFactory implements ClickHouseSocketFactory {

        @Override
        public <T> T create(ClickHouseConfig config, Class<T> clazz) throws IOException, UnsupportedOperationException {
            if (config == null || clazz == null) {
                throw new IllegalArgumentException("Non-null configuration and class are required");
            } else if (SSLConnectionSocketFactory.class.equals(clazz)) {
                return clazz.cast(new CustomSSLSocketFactory(config));
            } else if (PlainConnectionSocketFactory.class.equals(clazz)) {
                return clazz.cast(new CustomPlainSocketFactory(config));
            }

            throw new UnsupportedOperationException(ClickHouseUtils.format("Class %s is not supported", clazz));
        }

        @Override
        public boolean supports(Class<?> clazz) {
            return PlainConnectionSocketFactory.class.equals(clazz) || SSLConnectionSocketFactory.class.equals(clazz);
        }
    }

    static class CustomPlainSocketFactory extends PlainConnectionSocketFactory {
        private final ClickHouseConfig config;

        public CustomPlainSocketFactory(ClickHouseConfig config) {
            this.config = config;
        }

        @Override
        public Socket createSocket(final HttpContext context) throws IOException {
            // Use AbstractSockerClient.setSockerOptions to propagate socket configuration
            return AbstractSocketClient.setSocketOptions(config, new Socket());
        }
    }

    static class CustomSSLSocketFactory extends SSLConnectionSocketFactory {
        private static final Logger LOG = LoggerFactory.getLogger(CustomSSLSocketFactory.class);
        private final ClickHouseConfig config;

        private final SNIHostName defaultSNIHostName;

        public CustomSSLSocketFactory(ClickHouseConfig config) throws SSLException {
            super(ClickHouseSslContextProvider.getProvider().getSslContext(SSLContext.class, config)
                            .orElse(SSLContexts.createDefault()),
                    config.getSslMode() == ClickHouseSslMode.STRICT
                            ? new DefaultHostnameVerifier()
                            : (hostname, session) -> true); // NOSONAR
            this.config = config;
            String configStr = config.getStrOption(ClickHouseClientOption.CUSTOM_SOCKET_FACTORY_OPTIONS);
            if (configStr != null) {
                Map<String, String> configMap = ClickHouseOption.toKeyValuePairs(configStr);
                defaultSNIHostName = new SNIHostName(configMap.get("default_sni"));
            } else {
                throw new RuntimeException("Missing configuration for the factory");
            }
        }

        @Override
        protected void prepareSocket(SSLSocket socket, HttpContext context) throws IOException {
            LOG.debug("Preparing socket: {}", socket);
            LOG.debug("Remote address: {}", socket.getInetAddress());
            SSLParameters sslParams = socket.getSSLParameters();
            sslParams.setServerNames(Collections.singletonList(defaultSNIHostName));
            socket.setSSLParameters(sslParams);
        }
    }
}
