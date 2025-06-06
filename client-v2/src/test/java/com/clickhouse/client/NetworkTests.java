package com.clickhouse.client;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;
import java.util.Collections;

@Test(groups = {"integration"})
public class NetworkTests {
    private static final String NGINX_IMAGE = "nginx:alpine";
    private static final int NGINX_SSL_PORT = 8443;
    private static final String NODE1_HOST = "node1.test";
    private static final String NODE2_HOST = "node2.test";

    private GenericContainer<?> nginxContainer;

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
        return new Object[][] {
            {NODE1_HOST},
            {NODE2_HOST}
        };
    }

    private SSLSocket createSniSocket(String sniHost, String serverHost, int port) throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, new TrustManager[] { new X509TrustManager() {
            public void checkClientTrusted(X509Certificate[] chain, String authType) {}
            public void checkServerTrusted(X509Certificate[] chain, String authType) {}
            public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        } }, null);

        SSLSocketFactory factory = context.getSocketFactory();
        SSLSocket socket = (SSLSocket) factory.createSocket(serverHost, port);

        SSLParameters sslParams = socket.getSSLParameters();
        sslParams.setServerNames(Collections.singletonList(new SNIHostName(sniHost)));
        socket.setSSLParameters(sslParams);

        return socket;
    }
}
