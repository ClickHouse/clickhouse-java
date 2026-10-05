package com.clickhouse.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

/**
 * ClickHouse endpoint for integration tests.
 * <p>
 * What to start is {@code TEST_CLICKHOUSE_CONTAINER}: {@code single} (default) starts one
 * server, {@code cluster} starts three servers behind nginx, and {@code none} starts nothing.
 * Where tests connect when no container is started comes from {@code TEST_CLICKHOUSE_HOST}
 * (default {@code localhost}), {@code TEST_CLICKHOUSE_USER}, and {@code TEST_CLICKHOUSE_PASSWORD}.
 * {@code TEST_CLICKHOUSE_IMAGE_VERSION} is the image tag used to start a container.
 * The removed {@code clickhouseVersion} system property throws {@link IllegalArgumentException}.
 * {@code TEST_CLICKHOUSE_SECURE=true} selects the HTTPS endpoint on port 8443 (ClickHouse Cloud).
 */
@SuppressWarnings("squid:S2187")
public class ClickHouseServerForTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseServerForTest.class);

    private static final Network network;
    private static final ClickHouseTestEnvironment environment;
    private static final ClickHouseContainerStarter starter;

    private static final String proxyHost;
    private static final int proxyPort;
    private static final String proxyImage;

    static {
        ClickHouseTestEnvironment.rejectRemovedOptions(System.getProperties());
        network = Network.newNetwork();
        environment = ClickHouseTestEnvironment.from(System.getenv());
        LOGGER.info("Local database: {}", environment.isLocalDatabase());

        proxyHost = environment.getProxyHost();
        proxyPort = environment.getProxyPort();
        proxyImage = environment.getProxyImage();

        starter = ClickHouseContainerStarter.create(environment, network);

        LOGGER.info(
                "ClickHouse test target: container={}, image={}, host={}, user={}, secure={}, database={}",
                environment.getContainerMode(), environment.getImageRef(), environment.getHost(),
                environment.getUser(), environment.isSecure(), environment.getDatabase());
        System.out.println("TEST_CLICKHOUSE_CONTAINER="
                + environment.getContainerMode().name().toLowerCase(Locale.ROOT));
        System.out.println("TEST_CLICKHOUSE_IMAGE_VERSION=" + environment.getImageRef());
    }

    public static String getClickHouseVersion() {
        return environment.getClickHouseVersion();
    }

    public static boolean hasClickHouseContainer() {
        return starter != null;
    }

    public static GenericContainer<?> getClickHouseContainer() {
        return starter == null ? null : starter.getEndpointContainer();
    }

    public static String getClickHouseContainerTmpDir() {
        return ClickHouseContainerStarter.CONTAINER_TMP_DIR;
    }

    public static String getClickHouseAddress() {
        return getClickHouseAddress(ClickHouseProtocol.ANY, false);
    }

    public static String getClickHouseAddress(ClickHouseProtocol protocol, boolean useIPaddress) {
        if (isCloud()) {
            Endpoint endpoint = cloudHttpEndpoint();
            return "https://" + endpoint.host + ":" + endpoint.port;
        }
        Endpoint endpoint = resolve(protocol, protocol.getDefaultPort(), true);
        return endpoint.host + ":" + endpoint.port;
    }

    public static ClickHouseNode getClickHouseNode(ClickHouseProtocol protocol,
                                                   boolean useSecurePort,
                                                   ClickHouseNode template) {
        String database = template != null ? template.getDatabase().orElse("default") : "default";
        if (isCloud()) {
            Endpoint endpoint = cloudHttpEndpoint();
            return ClickHouseNode.builder(template)
                    .address(ClickHouseProtocol.HTTP, new InetSocketAddress(endpoint.host, endpoint.port))
                    .credentials(new ClickHouseCredentials(getUsername(), getPassword()))
                    .options(Collections.singletonMap("ssl", "true"))
                    .database(database)
                    .build();
        }

        int port = useSecurePort ? protocol.getDefaultSecurePort() : protocol.getDefaultPort();
        Endpoint endpoint = resolve(protocol, port, true);
        return ClickHouseNode.builder(template).address(protocol, new InetSocketAddress(endpoint.host, endpoint.port))
                .credentials(new ClickHouseCredentials(getUsername(), getPassword()))
                .build();
    }

    public static ClickHouseNode getClickHouseNode(ClickHouseProtocol protocol, int port) {
        if (isCloud()) {
            Endpoint endpoint = cloudHttpEndpoint();
            return ClickHouseNode.builder()
                    .address(protocol, new InetSocketAddress(endpoint.host, endpoint.port))
                    .credentials(new ClickHouseCredentials(getUsername(), getPassword()))
                    .database(getDatabase())
                    .build();
        }
        Endpoint endpoint = resolve(protocol, port, false);
        return ClickHouseNode.builder().address(protocol, new InetSocketAddress(endpoint.host, endpoint.port)).build();
    }

    public static ClickHouseNode getClickHouseNode(ClickHouseProtocol protocol, Map<String, String> options) {
        String url;
        if (isCloud()) {
            Endpoint endpoint = cloudHttpEndpoint();
            options.put("password", getPassword());
            url = String.format("https://%s:%d/%s", endpoint.host, endpoint.port, getDatabase());
        } else {
            Endpoint endpoint = resolve(protocol, protocol.getDefaultPort(), true);
            url = String.format("http://%s:%d/default", endpoint.host, endpoint.port);
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
        return environment.getUser();
    }

    public static String getPassword() {
        return environment.getPassword();
    }

    public static boolean isCloud() {
        return environment.isSecure();
    }

    @BeforeSuite(groups = {"integration"})
    public static void beforeSuite() {
        if (starter != null) {
            if (!starter.isRunning()) {
                try {
                    starter.start();
                } catch (RuntimeException e) {
                    throw new IllegalStateException(new StringBuilder()
                            .append("Failed to start docker container for integration test.\r\n")
                            .append("To use an existing server, set TEST_CLICKHOUSE_CONTAINER=none ")
                            .append("and TEST_CLICKHOUSE_HOST. See ")
                            .append("https://github.com/ClickHouse/clickhouse-java#testing")
                            .toString(), e);
                }
            }
            if (starter.isRunning() && !createDatabaseOnServers()) {
                throw new RuntimeException("Failed to create database");
            }
            return;
        }

        if (isCloud() && environment.isLocalDatabase()) {
            if (!runQuery("CREATE DATABASE IF NOT EXISTS `" + getDatabase() + "`")) {
                throw new RuntimeException("Failed to create database for testing.");
            }
        }
    }

    @AfterSuite(groups = {"integration"})
    public static void afterSuite() {
        if (starter != null) {
            starter.stop();
        }

        if (isCloud() && environment.isLocalDatabase() && starter == null) {
            if (!runQuery("DROP DATABASE IF EXISTS `" + getDatabase() + "`")) {
                LOGGER.warn("Failed to drop database for testing.");
            }
        }
    }

    public static String getDatabase() {
        return environment.getDatabase();
    }

    private static boolean createDatabaseOnServers() {
        String database = getDatabase();
        String cluster = starter.replicatedClusterName();
        try {
            if (cluster == null) {
                return starter.execQueryOnServers("CREATE DATABASE IF NOT EXISTS `" + database + "`",
                        getUsername(), getPassword());
            }
            String sql = "CREATE DATABASE IF NOT EXISTS `" + database + "` ON CLUSTER " + cluster
                    + " ENGINE = Replicated('/clickhouse/databases/" + database + "', '{shard}', '{replica}')";
            Container.ExecResult result = starter.execInContainer("clickhouse-client",
                    "-u", getUsername(), "--password", getPassword(), "--query", sql);
            if (result.getExitCode() != 0) {
                LOGGER.error("query failed: stderr={}, stdout={}", result.getStderr(), result.getStdout());
                return false;
            }
            return true;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create database", e);
        }
    }

    public static boolean runQuery(String sql) {
        LOGGER.info("runQuery: (\"{}\")", sql);
        ClickHouseNode server = getClickHouseNode(ClickHouseProtocol.HTTP, isCloud(), ClickHouseNode.builder().build());
        String uri = server.getBaseUri();

        try {
            URL serverURL = new URL(uri);
            LOGGER.info("sending request to {} (uri={})", serverURL, uri);
            byte[] postData = sql.getBytes(StandardCharsets.UTF_8);
            String authorization = Base64.getEncoder()
                    .encodeToString((getUsername() + ":" + getPassword()).getBytes(StandardCharsets.UTF_8));
            for (int attempts = 0; attempts < 10; attempts++) {
                HttpURLConnection httpConn = (HttpURLConnection) serverURL.openConnection();
                try {
                    httpConn.setRequestMethod("POST");
                    httpConn.setDoOutput(true);
                    httpConn.setRequestProperty("Authorization", "Basic " + authorization);
                    httpConn.setFixedLengthStreamingMode(postData.length);

                    try (OutputStream out = httpConn.getOutputStream()) {
                        out.write(postData, 0, postData.length);
                        out.flush();
                    }

                    if (httpConn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                        return true;
                    }
                } finally {
                    httpConn.disconnect();
                }
            }
        } catch (Exception e) {
            LOGGER.error("failed to run query", e);
        }

        return false;
    }

    private static Endpoint resolve(ClickHouseProtocol protocol, int port, boolean applyOverride) {
        if (starter != null) {
            return new Endpoint(starter.getHost(), starter.getMappedPort(port));
        }
        if (applyOverride) {
            String override = environment.portOverride(protocol);
            if (override != null) {
                port = Integer.parseInt(override);
            }
        }
        return new Endpoint(environment.getHost(), port);
    }

    private static Endpoint cloudHttpEndpoint() {
        if (starter != null) {
            int port = ClickHouseProtocol.HTTP.getDefaultSecurePort();
            return new Endpoint(starter.getHost(), starter.getMappedPort(port));
        }
        String override = environment.portOverride(ClickHouseProtocol.HTTP);
        int port = override != null ? Integer.parseInt(override) : ClickHouseProtocol.HTTP.getDefaultSecurePort();
        return new Endpoint(environment.getHost(), port);
    }

    private static final class Endpoint {
        private final String host;
        private final int port;

        private Endpoint(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }
}
