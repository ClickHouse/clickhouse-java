package com.clickhouse.benchmark.clients;

import com.clickhouse.benchmark.BenchmarkRunner;
import com.clickhouse.benchmark.data.DataSet;
import com.clickhouse.benchmark.data.DataSets;
import com.clickhouse.benchmark.data.SimpleDataSet;
import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseFormat;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.InputStream;

import static com.clickhouse.client.ClickHouseServerForTest.isCloud;

@State(Scope.Benchmark)
public class BenchmarkBase {
    public static final String DB_NAME = "benchmarks";
    public final DataSet dataSet = new SimpleDataSet();

    @Setup(Level.Trial)
    public void setUpTrial() throws Exception {
        BaseIntegrationTest.setupClickHouseContainer();
        runQuery("CREATE DATABASE IF NOT EXISTS " + DB_NAME, false);
        DataSets.initializeTables(dataSet);
    }
    @TearDown(Level.Trial)
    public void tearDownTrial() {
        runQuery("DROP DATABASE IF EXISTS " + DB_NAME, false);
        BaseIntegrationTest.teardownClickHouseContainer();
    }

    //Connection parameters
    public static String getPassword() {
        return ClickHouseServerForTest.getPassword();
    }
    public static String getUsername() {
        return "default";
    }
    public static ClickHouseNode getServer() {
        return ClickHouseServerForTest.getClickHouseNode(ClickHouseProtocol.HTTP, isCloud(), ClickHouseNode.builder().build());
    }
    public static void notNull(Object obj) {
        if (obj == null) {
            throw new IllegalStateException("Null value");
        }
    }

    public static void runQuery(String query, boolean useDatabase) {
        ClickHouseNode node = getServer();
        try (Client client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername(getUsername())
                .setPassword(getPassword())
                .compressClientRequest(true)
                .useHttpCompression(true)
                .setDefaultDatabase(useDatabase ? DB_NAME : "default")
                .useAsyncRequests(false)
                .build()) {
            client.queryAll(query);
        }
    }

    public static void insertData(String tableName, InputStream dataStream, ClickHouseFormat format) {
        ClickHouseNode node = getServer();
        try (Client client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername(getUsername())
                .setPassword(getPassword())
                .compressClientRequest(true)
                .useHttpCompression(true)
                .setDefaultDatabase(DB_NAME)
                .useAsyncRequests(false)
                .build()) {
            client.insert(tableName, dataStream, format);
        }
    }
}
