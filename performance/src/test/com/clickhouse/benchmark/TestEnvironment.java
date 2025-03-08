package com.clickhouse.benchmark;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.config.ClickHouseClientOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;

import java.net.InetSocketAddress;
import java.util.Collections;

import static com.clickhouse.benchmark.clients.BenchmarkBase.runQuery;

public class TestEnvironment {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestEnvironment.class);
    public static final String DB_NAME = "jmh_benchmarks";
    private static final String CLICKHOUSE_DOCKER_IMAGE = "clickhouse/clickhouse-server:latest";
    private static ClickHouseNode serverNode;
    private static ClickHouseContainer container;


    //Environment Variables
    public static boolean isCloud() {
        return System.getenv("CLICKHOUSE_HOST") != null;
    }
    public static String getHost() {
        String host = System.getenv("CLICKHOUSE_HOST");
        if (host == null) {
            host = container.getHost();
        }

        return host;
    }
    public static int getPort() {
        String port = System.getenv("CLICKHOUSE_PORT");
        if (port == null) {
            if (isCloud()) {//Default handling for ClickHouse Cloud
                port = "8443";
            } else {
                port = String.valueOf(container.getFirstMappedPort());
            }
        }

        return Integer.parseInt(port);
    }
    public static String getPassword() {
        String password = System.getenv("CLICKHOUSE_PASSWORD");
        if (password == null) {
            if (isCloud()) {
                password = System.getenv("CLICKHOUSE_PASSWORD");
            } else {
                password = container.getPassword();
            }
        }
        return password;
    }
    public static String getUsername() {
        String username = System.getenv("CLICKHOUSE_USERNAME");
        if (username == null) {
            if (isCloud()) {
                username = "default";
            } else {
                username = container.getUsername();
            }
        }
        return username;
    }
    public static ClickHouseNode getServer() {
        return serverNode;
    }


    //Initialization and Teardown methods
    public static void setupEnvironment() {
        LOGGER.info("Initializing ClickHouse test environment...");

        if (isCloud()) {
            LOGGER.info("Using ClickHouse Cloud");
            container = null;
            serverNode = ClickHouseNode.builder(ClickHouseNode.builder().build())
                    .address(ClickHouseProtocol.HTTP, new InetSocketAddress(getHost(), getPort()))
                    .credentials(ClickHouseCredentials.fromUserAndPassword(getUsername(), getPassword()))
                    .options(Collections.singletonMap(ClickHouseClientOption.SSL.getKey(), "true"))
                    .database(DB_NAME)
                    .build();
        } else {
            LOGGER.info("Using ClickHouse Docker container");
            container = new ClickHouseContainer(CLICKHOUSE_DOCKER_IMAGE).withPassword(getPassword()).withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1");
            container.start();

            serverNode = ClickHouseServerForTest.getClickHouseNode(ClickHouseProtocol.HTTP, isCloud(), ClickHouseNode.builder().build());
        }

        createDatabase();
    }

    public static void cleanupEnvironment() {
        if (isCloud()) {
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
