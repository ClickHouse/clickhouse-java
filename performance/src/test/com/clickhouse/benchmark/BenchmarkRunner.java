package com.clickhouse.benchmark;

import com.clickhouse.benchmark.clients.ClientV1;
import com.clickhouse.benchmark.clients.ClientV2;
import com.clickhouse.benchmark.clients.JdbcV1;
import com.clickhouse.benchmark.clients.JdbcV2;
import com.clickhouse.benchmark.data.DataSet;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseFormat;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.MemPoolProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class BenchmarkRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkRunner.class);
    public static final String DB_NAME = "benchmarks";
    public static final long SMALL_SIZE = 500;
    public static final long MEDIUM_SIZE = 5000;
    public static final long LARGE_SIZE = 5000000;
    public static final long EXTRA_LARGE_SIZE = 5000000000L;

    public static DataSet dataSet;

    public static void main(String[] args) throws Exception {
        createDatabase(DB_NAME);

        LOGGER.info("Starting Benchmarks");
        Options opt = new OptionsBuilder()
                .include(ClientV1.class.getSimpleName())
                .include(ClientV2.class.getSimpleName())
                .include(JdbcV1.class.getSimpleName())
                .include(JdbcV2.class.getSimpleName())
                .forks(1)
                .mode(Mode.SampleTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .threads(1)
                .addProfiler(GCProfiler.class)
                .addProfiler(MemPoolProfiler.class)
                .warmupIterations(1)
                .warmupTime(TimeValue.seconds(3))
                .measurementIterations(1)
                .measurementTime(TimeValue.seconds(5))
                .build();

        new Runner(opt).run();
    }

    public static String getEnvOrDefault(String key, String _default) {
        String k = System.getenv(key);
        return (k != null) ? k : _default;
    }

    public static boolean getBooleanEnv(String key) {
        String value = getEnvOrDefault(key, "false").trim().toLowerCase();
        return value.equals("true") || value.equals("1");
    }


    //Connection parameters
    public static String getEndpointUrl() {
        String host = getEnvOrDefault("CLICKHOUSE_HOST", "localhost");
        String port = getEnvOrDefault("CLICKHOUSE_HTTP_PORT", "8123");
        boolean secure = getBooleanEnv("CLICKHOUSE_SECURE");
        String protocol = secure ? "https" : "http";
        return String.format("%s://%s:%s", protocol, host, port);
    }
    public static String getPassword() {
        return getEnvOrDefault("CLICKHOUSE_PASSWORD", "");
    }
    public static String getUsername() {
        return getEnvOrDefault("CLICKHOUSE_USERNAME", "default");
    }


    //Benchmark Helpers
    public static void notNull(Object obj) {
        if (obj == null) {
            throw new IllegalStateException("Null value");
        }
    }


    public static void createDatabase(String databaseName) {
        runQuery("CREATE DATABASE IF NOT EXISTS " + databaseName, false);
    }

    public static void dropDatabase(String databaseName) {
        runQuery("DROP DATABASE IF EXISTS " + databaseName, false);
    }

    public static void createTable(String createTableQuery) {
        runQuery(createTableQuery, true);
    }

    public static void dropTable(String tableName) {
        runQuery("DROP TABLE IF EXISTS " + tableName, true);
    }

    public static void runQuery(String query, boolean useDatabase) {
        try (Client client = new Client.Builder()
                .addEndpoint(BenchmarkRunner.getEndpointUrl())
                .setUsername(BenchmarkRunner.getUsername())
                .setPassword(BenchmarkRunner.getPassword())
                .compressClientRequest(true)
                .useHttpCompression(true)
                .setDefaultDatabase(useDatabase ? DB_NAME : "default")
                .build()) {
            client.queryAll(query);
        }
    }

    public static TableSchema describeTable(String tableName) {
        try (Client client = new Client.Builder()
                .addEndpoint(BenchmarkRunner.getEndpointUrl())
                .setUsername(BenchmarkRunner.getUsername())
                .setPassword(BenchmarkRunner.getPassword())
                .compressClientRequest(true)
                .useHttpCompression(true)
                .setDefaultDatabase(DB_NAME)
                .build()) {
            return client.getTableSchema(tableName);
        }
    }

    public static void insertData(String tableName, InputStream dataStream, ClickHouseFormat format) {
        try (Client client = new Client.Builder()
                .addEndpoint(BenchmarkRunner.getEndpointUrl())
                .setUsername(BenchmarkRunner.getUsername())
                .setPassword(BenchmarkRunner.getPassword())
                .compressClientRequest(true)
                .useHttpCompression(true)
                .setDefaultDatabase(DB_NAME)
                .build()) {
            client.insert(tableName, dataStream, format);
        }
    }

}
