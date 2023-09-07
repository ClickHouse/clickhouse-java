package com.clickhouse.client;

import static java.time.temporal.ChronoUnit.SECONDS;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseVersion;

/**
 * Adaptive ClickHouse server environment for integration test. Two modes are
 * supported: 1) existing server(when system property {@code clickhouseServer}
 * is defined); and 2) test container.
 *
 * <p>
 * Below system properties can be used for customization:
 * <ul>
 * <li>clickhouseServer - host of clickhouse server</li>
 * <li>clickhouse&lt;Protocol&gt;Port - port of specific protocol, for example:
 * {@code clickhouseGRPCPort}</li>
 * <li>clickhouseImage - custom docker image, with or without tag and/or image
 * digest</li>
 * <li>clickhouseVersion - version of clickhouse, could be replaced by the one
 * used in {@code clickhouseImage}</li>
 * <li>clickhouseTimezone - server timezone</li>
 * <li>additionalPackages - additional system packages should be installed in
 * container mode</li>
 * </ul>
 */
@SuppressWarnings("squid:S2187")
public class ClickHouseServerForTest {

    private static final Network network = Network.newNetwork();
    private static final Properties properties;

    private static final String clickhouseServer;
    private static final String clickhouseVersion;
    private static final GenericContainer<?> clickhouseContainer;

    private static final String proxyHost;
    private static final int proxyPort;
    private static final String proxyImage;

    static {
        properties = new Properties();
        try (InputStream in = ClickHouseUtils.getFileInputStream("test.properties")) {
            properties.load(in);
        } catch (Exception e) {
            // ignore
        }

        String proxy = ClickHouseUtils.getProperty("proxyAddress", properties);
        if (!ClickHouseChecker.isNullOrEmpty(proxy)) { // use external proxy
            int index = proxy.indexOf(':');
            if (index > 0) {
                proxyHost = proxy.substring(0, index);
                proxyPort = Integer.parseInt(proxy.substring(index + 1));
            } else {
                proxyHost = proxy;
                proxyPort = 8666;
            }
            proxyImage = "";
        } else {
            proxyHost = "";
            proxyPort = -1;
            String image = ClickHouseUtils.getProperty("proxyImage", properties);
            proxyImage = ClickHouseChecker.isNullOrEmpty(image) ? "ghcr.io/shopify/toxiproxy:2.5.0" : image;
        }

        final String containerName = System.getenv("CHC_TEST_CONTAINER_ID");

        String host = ClickHouseUtils.getProperty("clickhouseServer", properties);
        clickhouseServer = ClickHouseChecker.isNullOrEmpty(host) ? null : host;


        String imageTag = ClickHouseUtils.getProperty("clickhouseVersion", properties);

        if (clickhouseServer != null) { // use external server
            clickhouseVersion = ClickHouseChecker.isNullOrEmpty(imageTag)
                    || ClickHouseVersion.of(imageTag).getYear() == 0 ? "" : imageTag;
            clickhouseContainer = null;
        } else { // use test container
            String timezone = ClickHouseUtils.getProperty("clickhouseTimezone", properties);
            if (ClickHouseChecker.isNullOrEmpty(timezone)) {
                timezone = "UTC";
            }

            String imageName = ClickHouseUtils.getProperty("clickhouseImage", properties);
            if (ClickHouseChecker.isNullOrEmpty(imageName)) {
                imageName = "clickhouse/clickhouse-server";
            }

            int tagIndex = imageName.indexOf(':');
            int digestIndex = imageName.indexOf('@');
            if (tagIndex > 0) {
                imageTag = "";
                clickhouseVersion = digestIndex > 0 ? imageName.substring(tagIndex + 1, digestIndex)
                        : imageName.substring(tagIndex + 1);
            } else if (digestIndex > 0 || ClickHouseChecker.isNullOrEmpty(imageTag)) {
                clickhouseVersion = imageTag = "";
            } else {
                if (ClickHouseVersion.of(imageTag).getYear() == 0) {
                    clickhouseVersion = "";
                } else {
                    clickhouseVersion = imageTag;
                }
                imageTag = ":" + imageTag;
            }

            String imageNameWithTag = imageName + imageTag;
            String customPackages = ClickHouseUtils.getProperty("additionalPackages", properties);
            if (!ClickHouseChecker.isNullOrEmpty(clickhouseVersion)
                    && ClickHouseVersion.check(clickhouseVersion, "(,21.3]")) {
                if (ClickHouseChecker.isNullOrEmpty(customPackages)) {
                    customPackages = "tzdata";
                } else if (!customPackages.contains("tzdata")) {
                    customPackages += " tzdata";
                }
            }

            final String additionalPackages = customPackages;
            final String customDirectory = "/custom";
            clickhouseContainer = (ClickHouseChecker.isNullOrEmpty(additionalPackages)
                    ? new GenericContainer<>(imageNameWithTag)
                    : new GenericContainer<>(new ImageFromDockerfile().withDockerfileFromBuilder(builder -> builder
                    .from(imageNameWithTag).run("apt-get update && apt-get install -y " + additionalPackages))))
                    .withCreateContainerCmdModifier(
                            it -> {
                                it.withEntrypoint("/bin/sh");
                                if (!ClickHouseChecker.isNullOrBlank(containerName)) {
                                    it.withName(containerName);
                                }
                            })
                    .withCommand("-c", String.format("chmod +x %1$s/patch && %1$s/patch", customDirectory))
                    .withEnv("TZ", timezone)
                    .withExposedPorts(ClickHouseProtocol.GRPC.getDefaultPort(),
                            ClickHouseProtocol.HTTP.getDefaultPort(),
                            ClickHouseProtocol.HTTP.getDefaultSecurePort(),
                            ClickHouseProtocol.MYSQL.getDefaultPort(),
                            ClickHouseProtocol.TCP.getDefaultPort(),
                            ClickHouseProtocol.TCP.getDefaultSecurePort(),
                            ClickHouseProtocol.POSTGRESQL.getDefaultPort())
                    .withClasspathResourceMapping("containers/clickhouse-server", customDirectory, BindMode.READ_ONLY)
                    .withFileSystemBind(System.getProperty("java.io.tmpdir"), getClickHouseContainerTmpDir(),
                            BindMode.READ_WRITE)
                    .withNetwork(network)
                    .withNetworkAliases("clickhouse")
                    .waitingFor(Wait.forHttp("/ping").forPort(ClickHouseProtocol.HTTP.getDefaultPort())
                            .forStatusCode(200).withStartupTimeout(Duration.of(600, SECONDS)));
        }

    }

    public static String getClickHouseVersion() {
        return clickhouseVersion;
    }

    public static boolean hasClickHouseContainer() {
        return clickhouseContainer != null;
    }

    public static GenericContainer<?> getClickHouseContainer() {
        return clickhouseContainer;
    }

    public static String getClickHouseContainerTmpDir() {
        return "/tmp";
    }

    public static String getClickHouseAddress() {
        return getClickHouseAddress(ClickHouseProtocol.ANY, false);
    }

    public static String getClickHouseAddress(ClickHouseProtocol protocol, boolean useIPaddress) {
        StringBuilder builder = new StringBuilder();

        if (clickhouseContainer != null) {
            builder.append(useIPaddress ? clickhouseContainer.getHost() : clickhouseContainer.getHost())
                    .append(':').append(clickhouseContainer.getMappedPort(protocol.getDefaultPort()));
        } else {
            String port = ClickHouseUtils
                    .getProperty(ClickHouseUtils.format("clickhouse%SPort", protocol.name(), properties));
            if (ClickHouseChecker.isNullOrEmpty(port)) {
                port = String.valueOf(protocol.getDefaultPort());
            }
            builder.append(clickhouseServer).append(':').append(port);
        }

        return builder.toString();
    }

    public static ClickHouseNode getClickHouseNode(ClickHouseProtocol protocol, boolean useSecurePort,
                                                   ClickHouseNode template) {
        String host = clickhouseServer;
        int port = useSecurePort ? protocol.getDefaultSecurePort() : protocol.getDefaultPort();
        GenericContainer<?> container = clickhouseContainer;
        if (container != null) {
            host = container.getHost();
            port = container.getMappedPort(port);
        } else {
            String config = ClickHouseUtils
                    .getProperty(ClickHouseUtils.format("clickhouse%SPort", protocol.name(), properties));
            if (config != null && !config.isEmpty()) {
                port = Integer.parseInt(config);
            }
        }

        return ClickHouseNode.builder(template).address(protocol, new InetSocketAddress(host, port)).build();
    }

    public static ClickHouseNode getClickHouseNode(ClickHouseProtocol protocol, int port) {
        String host = clickhouseServer;

        if (clickhouseContainer != null) {
            host = clickhouseContainer.getHost();
            port = clickhouseContainer.getMappedPort(port);
        }

        return ClickHouseNode.builder().address(protocol, new InetSocketAddress(host, port)).build();
    }

    public static ClickHouseNode getClickHouseNode(ClickHouseProtocol protocol, Map<String, String> options) {
        String host = clickhouseServer;
        int port = protocol.getDefaultPort();

        if (clickhouseContainer != null) {
            host = clickhouseContainer.getHost();
            port = clickhouseContainer.getMappedPort(port);
        }

        String url = String.format("http://%s:%d/default", host, port);
        return ClickHouseNode.of(url, options);
    }

    public static boolean hasProxyAddress() {
        return !ClickHouseChecker.isNullOrEmpty(proxyHost);
    }

    public static String getProxyImage() {
        return proxyImage;
    }

    public static String getProxyHost() {
        return proxyHost;
    }

    public static int getProxyPort() {
        return proxyPort;
    }

    public static Network getNetwork() {
        return network;
    }

    @BeforeSuite(groups = {"integration"})
    public static void beforeSuite() {
        if (clickhouseContainer != null) {
            if (clickhouseContainer.isRunning()) {
                return;
            }

            try {
                clickhouseContainer.start();
            } catch (RuntimeException e) {
                throw new IllegalStateException(new StringBuilder()
                        .append("Failed to start docker container for integration test.\r\n")
                        .append("If you prefer to run tests without docker, ")
                        .append("please follow instructions at https://github.com/ClickHouse/clickhouse-java#testing")
                        .toString(), e);
            }
        }
    }

    @AfterSuite(groups = {"integration"})
    public static void afterSuite() {
        if (clickhouseContainer != null) {
            clickhouseContainer.stop();
        }
    }
}
