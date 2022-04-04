package com.clickhouse.client;

import static java.time.temporal.ChronoUnit.SECONDS;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Properties;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

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
public class ClickHouseServerForTest {
    private static final Properties properties;

    private static final String clickhouseServer;
    private static final String clickhouseVersion;
    private static final GenericContainer<?> clickhouseContainer;

    static {
        properties = new Properties();
        try (InputStream in = ClickHouseUtils.getFileInputStream("test.properties")) {
            properties.load(in);
        } catch (Exception e) {
            // ignore
        }

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
                    .withCreateContainerCmdModifier(it -> it.withEntrypoint("/bin/sh"))
                    .withCommand("-c", String.format("chmod +x %1$s/patch && %1$s/patch", customDirectory))
                    .withEnv("TZ", timezone)
                    .withExposedPorts(ClickHouseProtocol.GRPC.getDefaultPort(),
                            ClickHouseProtocol.HTTP.getDefaultPort(),
                            ClickHouseProtocol.MYSQL.getDefaultPort(),
                            ClickHouseProtocol.TCP.getDefaultPort(),
                            ClickHouseProtocol.POSTGRESQL.getDefaultPort())
                    .withClasspathResourceMapping("containers/clickhouse-server", customDirectory, BindMode.READ_ONLY)
                    .waitingFor(Wait.forHttp("/ping").forPort(ClickHouseProtocol.HTTP.getDefaultPort())
                            .forStatusCode(200).withStartupTimeout(Duration.of(60, SECONDS)));
        }
    }

    public static String getClickHouseVersion() {
        return clickhouseVersion;
    }

    public static GenericContainer<?> getClickHouseContainer() {
        return clickhouseContainer;
    }

    public static String getClickHouseAddress() {
        return getClickHouseAddress(ClickHouseProtocol.ANY, false);
    }

    public static String getClickHouseAddress(ClickHouseProtocol protocol, boolean useIPaddress) {
        StringBuilder builder = new StringBuilder();

        if (clickhouseContainer != null) {
            builder.append(useIPaddress ? clickhouseContainer.getContainerIpAddress() : clickhouseContainer.getHost())
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

    public static ClickHouseNode getClickHouseNode(ClickHouseProtocol protocol, ClickHouseNode template) {
        String host = clickhouseServer;
        int port = protocol.getDefaultPort();

        if (clickhouseContainer != null) {
            host = clickhouseContainer.getContainerIpAddress();
            port = clickhouseContainer.getMappedPort(port);
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
            host = clickhouseContainer.getContainerIpAddress();
            port = clickhouseContainer.getMappedPort(port);
        }

        return ClickHouseNode.builder().address(protocol, new InetSocketAddress(host, port)).build();
    }

    @BeforeSuite(groups = { "integration" })
    public static void beforeSuite() {
        if (clickhouseContainer != null) {
            if (clickhouseContainer.isRunning()) {
                return;
            }

            clickhouseContainer.start();
        }
    }

    @AfterSuite(groups = { "integration" })
    public static void afterSuite() {
        if (clickhouseContainer != null) {
            clickhouseContainer.stop();
        }
    }
}
