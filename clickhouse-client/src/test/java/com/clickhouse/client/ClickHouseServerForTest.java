package com.clickhouse.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static java.time.temporal.ChronoUnit.SECONDS;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseServerForTest.class);

    private static final Network network = Network.newNetwork();
    private static final Properties properties;

    private static final String clickhouseServer;
    private static final String clickhouseVersion;
    private static final GenericContainer<?> clickhouseContainer;

    private static final String proxyHost;
    private static final int proxyPort;
    private static final String proxyImage;
    private static boolean isCloud = false;

    private static final String database;

    static {
        properties = new Properties(System.getProperties());
        database = "clickhouse_java_" + UUID.randomUUID().toString().substring(0, 8) + "_test_" + System.currentTimeMillis();

        String proxy = properties.getProperty("proxyAddress");
        if (proxy != null && !proxy.isEmpty()) { // use external proxy
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
            String image = properties.getProperty("proxyImage");
            proxyImage = image == null ? "ghcr.io/shopify/toxiproxy:2.5.0" : image;
        }

        final String containerName = System.getenv("CHC_TEST_CONTAINER_ID");

        clickhouseServer = properties.getProperty("clickhouseServer");
        String imageTag = properties.getProperty("clickhouseVersion");
        if (imageTag != null && imageTag.equalsIgnoreCase("cloud")) {
            isCloud = true;
            imageTag = "";
        }
        if (clickhouseServer != null) { // use external server
            clickhouseVersion = imageTag == null
                    || ClickHouseVersionUtils.of(imageTag).getYear() == 0 ? "" : imageTag;
            clickhouseContainer = null;
        } else { // use test container
            String timezone = properties.getProperty("clickhouseTimezone");
            if (timezone == null) {
                timezone = "UTC";
            }

            String imageName = properties.getProperty("clickhouseImage");
            if (imageName == null) {
                imageName = "clickhouse/clickhouse-server";
            }

            int tagIndex = imageName.indexOf(':');
            int digestIndex = imageName.indexOf('@');
            if (tagIndex > 0) {
                imageTag = "";
                clickhouseVersion = digestIndex > 0 ? imageName.substring(tagIndex + 1, digestIndex)
                        : imageName.substring(tagIndex + 1);
            } else if (digestIndex > 0 || imageTag == null) {
                clickhouseVersion = imageTag = "";
            } else {
                if (ClickHouseVersionUtils.of(imageTag).getYear() == 0) {
                    clickhouseVersion = "";
                } else {
                    clickhouseVersion = imageTag;
                }
                imageTag = ":" + imageTag;
            }

            String imageNameWithTag = imageName + imageTag;
            String customPackages = properties.getProperty("additionalPackages");
            if (ClickHouseVersionUtils.check(clickhouseVersion, "(,21.3]")) {
                if (customPackages == null) {
                    customPackages = "tzdata";
                } else if (!customPackages.contains("tzdata")) {
                    customPackages += " tzdata";
                }
            }

            final String additionalPackages = customPackages;
            final String customDirectory = "/custom";
            clickhouseContainer = ((additionalPackages == null)
                    ? new GenericContainer<>(imageNameWithTag)
                    : new GenericContainer<>(new ImageFromDockerfile().withDockerfileFromBuilder(builder -> builder
                    .from(imageNameWithTag).run("apt-get update && apt-get install -y " + additionalPackages))))
                    .withCreateContainerCmdModifier(
                            it -> {
                                it.withEntrypoint("/bin/sh");
                                if (containerName != null) {
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
                    .withClasspathResourceMapping("empty.csv", "/var/lib/clickhouse/user_files/empty.csv", BindMode.READ_ONLY)
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
        if (isCloud) {
            String host = System.getenv("CLICKHOUSE_CLOUD_HOST");
            int port = 8443;
            builder.append("https://").append(host).append(':').append(port);
            return builder.toString();
        } else if (clickhouseContainer != null) {
            builder.append(clickhouseContainer.getHost())
                    .append(':').append(clickhouseContainer.getMappedPort(protocol.getDefaultPort()));
        } else {
            String port = properties
                    .getProperty(String.format("clickhouse%SPort", protocol.name()), String.valueOf(protocol.getDefaultPort()));
            builder.append(clickhouseServer).append(':').append(port);
        }

        return builder.toString();
    }

    public static ClickHouseNode getClickHouseNode(ClickHouseProtocol protocol,
                                                   boolean useSecurePort,
                                                   ClickHouseNode template) {
        String host = clickhouseServer;
        int port = useSecurePort ? protocol.getDefaultSecurePort() : protocol.getDefaultPort();
        String database = template != null ? template.getDatabase().orElse("default") : "default";
        GenericContainer<?> container = clickhouseContainer;
        if (isCloud()) {
            port = 8443;
            host = System.getenv("CLICKHOUSE_CLOUD_HOST");
            return ClickHouseNode.builder(template)
                    .address(ClickHouseProtocol.HTTP, new InetSocketAddress(host, port))
                    .credentials(new ClickHouseCredentials("default", getPassword()))
                    .options(Collections.singletonMap("ssl", "true"))
                    .database(database)
                    .build();
        } else if (container != null) {
            host = container.getHost();
            port = container.getMappedPort(port);
        } else {
            String config = properties
                    .getProperty(String.format("clickhouse%SPort", protocol.name()));
            if (config != null && !config.isEmpty()) {
                port = Integer.parseInt(config);
            }
        }

        return ClickHouseNode.builder(template).address(protocol, new InetSocketAddress(host, port))
                .credentials(new ClickHouseCredentials("default", getPassword()))
                .build();
    }

    public static ClickHouseNode getClickHouseNode(ClickHouseProtocol protocol, int port) {
        String host = clickhouseServer;

        if (isCloud) {
            host = System.getenv("CLICKHOUSE_CLOUD_HOST");
            port = 8443;
            return ClickHouseNode.builder().
                    address(protocol, new InetSocketAddress(host, port))
                    .credentials(new ClickHouseCredentials("default", getPassword()))
                    .database(database)
                    .build();
        }
        if (clickhouseContainer != null) {
            host = clickhouseContainer.getHost();
            port = clickhouseContainer.getMappedPort(port);
        }
        return ClickHouseNode.builder().address(protocol, new InetSocketAddress(host, port)).build();
    }

    public static ClickHouseNode getClickHouseNode(ClickHouseProtocol protocol, Map<String, String> options) {
        String host = clickhouseServer;
        String url = null;
        int port = protocol.getDefaultPort();
        if (isCloud) {
            host = System.getenv("CLICKHOUSE_CLOUD_HOST");
            port = 8443;
            options.put("password", getPassword());
            url = String.format("https://%s:%d/%s", host, port, database);
        } else if (clickhouseContainer != null) {
            host = clickhouseContainer.getHost();
            port = clickhouseContainer.getMappedPort(port);
            url = String.format("http://%s:%d/default", host, port);//TODO: Should this always be http?
        }
        return ClickHouseNode.of(url, options);
    }

    public static boolean hasProxyAddress() {
        return proxyHost != null;
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

    public static String getUsername() {
        // For cloud, the username is set in environment variable
        if (isCloud) {
            return System.getenv("CLICKHOUSE_CLOUD_USERNAME") == null ? "default" : System.getenv("CLICKHOUSE_CLOUD_USERNAME");
        } else {
            return "default";
        }
    }

    public static String getPassword() {
        // For cloud, the password is set in environment variable
        if (isCloud) {
            return System.getenv("CLICKHOUSE_CLOUD_PASSWORD");
        } else {
            return "test_default_password";
        }
    }

    public static boolean isCloud() {
        return isCloud;
    }

    @BeforeSuite(groups = {"integration"})
    public static void beforeSuite() {
        if (isCloud) {
            if (!runQuery("CREATE DATABASE IF NOT EXISTS " + database)) {
                throw new IllegalStateException("Failed to create database for testing.");
            }

            return;
        }

        if (clickhouseContainer != null) {
            if (clickhouseContainer.isRunning()) {
                return;
            }

            try {
                clickhouseContainer.start();

                if (clickhouseContainer.isRunning()) {
                    if (!runQuery("CREATE DATABASE IF NOT EXISTS " + getDatabase())) {
                        throw new RuntimeException("Failed to create database");
                    }
                }
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
//            clickhouseContainer.copyFileFromContainer("/var/log/clickhouse-server/clickhouse-server.log", "server-container.log");
            clickhouseContainer.stop();
        }

        if (isCloud) {
            if (!runQuery("DROP DATABASE IF EXISTS " + database)) {
                LOGGER.warn("Failed to drop database for testing.");
            }
        }
    }

    public static String getDatabase() {
        return database;
    }

    public static boolean runQuery(String sql) {
        LOGGER.info("Run a query for testing...");

        if (clickhouseContainer != null) {
            try {
                Container.ExecResult res =  clickhouseContainer.execInContainer("clickhouse-client",
                        "-u", "default", "--password", getPassword(), sql);
                return res.getExitCode() == 0;
            } catch (Exception e) {
                throw new RuntimeException("runQuery('" + sql + "') failed", e);
            }
        } else {
            //Create database for testing
            ClickHouseNode server = getClickHouseNode(ClickHouseProtocol.HTTP, isCloud(), ClickHouseNode.builder().build());

            LOGGER.info("SQL: " + sql);
            LOGGER.info("Server: " + server);

            int retries = 0;
            do {
                try (ClickHouseClient client = ClickHouseClient.builder().nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP)).build();
                     ClickHouseResponse response = client.read(server).query(sql).executeAndWait()) {
                    if (response.getSummary() != null && response.getSummary().getWrittenRows() > -1) {
                        return true;
                    }
                } catch (Exception e) {
                    LOGGER.warn("Failed to run query for testing...", e);
                }

                try {
                    Thread.sleep(15000);
                } catch (InterruptedException e) {
                    LOGGER.error("Failed to sleep", e);
                    throw new RuntimeException(e);
                }
            } while(retries++ < 10);

            return false;
        }
    }
}
