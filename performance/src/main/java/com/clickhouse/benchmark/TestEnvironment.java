package com.clickhouse.benchmark;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseClientOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;

import static com.clickhouse.benchmark.clients.BenchmarkBase.runQuery;
import static java.time.temporal.ChronoUnit.SECONDS;

public class TestEnvironment {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestEnvironment.class);
    public static final String DB_NAME = "jmh_benchmarks_" + System.currentTimeMillis();
    private static final String CLICKHOUSE_DOCKER_IMAGE = "clickhouse/clickhouse-server:latest";
    private static ClickHouseNode serverNode;
    private static ClickHouseContainer container;


    //Environment Variables
    // Set CLICKHOUSE_URL to point the benchmarks at an existing remote ClickHouse
    // server (ClickHouse Cloud or any self-hosted instance), e.g.:
    //   https://default:my-password@abc123.clickhouse.cloud:8443?cluster=true
    //   http://default@localhost:8123
    // The scheme selects HTTP vs HTTPS/SSL, and credentials come from the URL's
    // user-info (username[:password]). When unset, a local Docker container is
    // started automatically instead.
    //
    // The optional "cluster" query parameter (default false) tells the benchmarks
    // whether the remote server is part of a replicated cluster (e.g. ClickHouse
    // Cloud) and therefore needs a SYSTEM SYNC REPLICA after writes. Leave it
    // unset/false for a plain standalone remote server, whose tables aren't
    // replicated and would reject that statement.
    private static URI getRemoteUrl() {
        String url = System.getenv("CLICKHOUSE_URL");
        return url == null ? null : URI.create(url);
    }
    public static boolean isRemote() {
        return getRemoteUrl() != null;
    }
    public static boolean isSsl() {
        URI url = getRemoteUrl();
        return url != null && "https".equalsIgnoreCase(url.getScheme());
    }
    public static boolean isCluster() {
        URI url = getRemoteUrl();
        String query = url == null ? null : url.getQuery();
        if (query == null) {
            return false;
        }
        for (String param : query.split("&")) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2 && kv[0].equalsIgnoreCase("cluster")) {
                return Boolean.parseBoolean(kv[1]);
            }
        }
        return false;
    }
    public static String getHost() {
        URI url = getRemoteUrl();
        return url != null ? url.getHost() : container.getHost();
    }
    public static int getPort() {
        URI url = getRemoteUrl();
        if (url != null) {
            return url.getPort() != -1 ? url.getPort() : (isSsl() ? 8443 : 8123);
        }
        return container.getMappedPort(8123);
    }
    public static String getUsername() {
        URI url = getRemoteUrl();
        if (url == null) {
            return container.getUsername();
        }
        String userInfo = url.getUserInfo();
        if (userInfo == null) {
            return "default";
        }
        int sep = userInfo.indexOf(':');
        return sep == -1 ? userInfo : userInfo.substring(0, sep);
    }
    public static String getPassword() {
        URI url = getRemoteUrl();
        if (url == null) {
            return container.getPassword();
        }
        String userInfo = url.getUserInfo();
        int sep = userInfo == null ? -1 : userInfo.indexOf(':');
        return sep == -1 ? null : userInfo.substring(sep + 1);
    }
    public static ClickHouseNode getServer() {
        return serverNode;
    }


    //Initialization and Teardown methods
    public static void setupEnvironment() {
        LOGGER.info("Initializing ClickHouse test environment...");

        if (isRemote()) {
            LOGGER.info("Using remote ClickHouse server at {}:{}", getHost(), getPort());
            container = null;
        } else {
            LOGGER.info("Using ClickHouse Docker container");
            container = new ClickHouseContainer(CLICKHOUSE_DOCKER_IMAGE)
                    .withPassword("testing_password")
                    .withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
                    .withExposedPorts(8123, 8443)
                    .waitingFor(Wait.forHttp("/ping").forPort(8123).forStatusCode(200).withStartupTimeout(Duration.of(600, SECONDS)));
            container.start();
        }

        serverNode = ClickHouseNode.builder(ClickHouseNode.builder().build())
                .address(ClickHouseProtocol.HTTP, new InetSocketAddress(getHost(), getPort()))
                .credentials(ClickHouseCredentials.fromUserAndPassword(getUsername(), getPassword()))
                .options(Collections.singletonMap(ClickHouseClientOption.SSL.getKey(), isSsl() ? "true" : "false"))
                .database(DB_NAME)
                .build();
        createDatabase();
    }

    public static void cleanupEnvironment() {
        LOGGER.info("Cleaning up ClickHouse test environment...");
        if (isRemote()) {
            dropDatabase();
        }

        if (container != null && container.isRunning()) {
            container.stop();
            container = null;
        }
    }

    public static void createDatabase() {
        LOGGER.info("Creating database: {}", DB_NAME);
        runQuery(String.format("CREATE DATABASE IF NOT EXISTS %s", DB_NAME), false);
    }

    public static void dropDatabase() {
        LOGGER.info("Dropping database: {}", DB_NAME);
        runQuery(String.format("DROP DATABASE IF EXISTS %s", DB_NAME));
    }
}
