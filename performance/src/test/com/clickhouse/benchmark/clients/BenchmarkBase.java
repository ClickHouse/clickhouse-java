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
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.data.ClickHouseFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

import static com.clickhouse.client.ClickHouseServerForTest.isCloud;

public class BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkBase.class);
    public static final String DB_NAME = "benchmarks";
    public final DataSet dataSet = new SimpleDataSet();

    public void setup() throws Exception {
        LOGGER.debug("BenchmarkBase setup()");
        BaseIntegrationTest.setupClickHouseContainer();
        runQuery("CREATE DATABASE IF NOT EXISTS " + DB_NAME, false);
        DataSets.initializeTables(dataSet);
    }

    public void tearDown() {
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
                .setDefaultDatabase(useDatabase ? DB_NAME : "default")
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
                .setDefaultDatabase(DB_NAME)
                .build();
             InsertResponse response = client.insert(tableName, dataStream, format).get()) {
            LOGGER.info("Rows inserted: {}", response.getWrittenRows());
        } catch (Exception e) {
            LOGGER.error("Error inserting data: ", e);
            throw new RuntimeException("Error inserting data", e);
        }
    }
}
