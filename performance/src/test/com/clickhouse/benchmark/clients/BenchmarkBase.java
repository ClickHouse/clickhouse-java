package com.clickhouse.benchmark.clients;

import com.clickhouse.benchmark.data.DataSet;
import com.clickhouse.benchmark.data.DataSets;
import com.clickhouse.benchmark.data.FileDataSet;
import com.clickhouse.benchmark.data.SimpleDataSet;
import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.data.ClickHouseFormat;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

import static com.clickhouse.client.ClickHouseServerForTest.isCloud;

public class BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkBase.class);
    public static final String DB_NAME = "benchmarks";

    @State(Scope.Benchmark)
    public static class DataState {
        @Param({"simple"})
        String datasetSourceName;
        ClickHouseFormat format = ClickHouseFormat.JSONEachRow;

        DataSet dataSet;
    }

    public void setup(DataState dataState, boolean insertData) throws Exception {
        LOGGER.info("Setup BenchmarkBase using " + dataState.datasetSourceName + " dataset.");
        if ("simple".equals(dataState.datasetSourceName) && dataState.dataSet == null) {
            dataState.datasetSourceName = "simple";
            dataState.dataSet = new SimpleDataSet();
        } else if (dataState.datasetSourceName.startsWith("file://")) {

            dataState.dataSet = new FileDataSet(dataState.datasetSourceName.substring("file://".length()));
            dataState.datasetSourceName = dataState.dataSet.getName();
        }

        LOGGER.debug("BenchmarkBase setup(). Data source " + dataState.datasetSourceName);
        BaseIntegrationTest.setupClickHouseContainer();
        runQuery("CREATE DATABASE IF NOT EXISTS " + DB_NAME, false);
        DataSets.initializeTables(dataState.dataSet, insertData);
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
    public static void isNotNull(Object obj, boolean doWeCare) {
        if (obj == null && doWeCare) {
            throw new RuntimeException("Object is null");
        }
    }

    public static void runQuery(String query, boolean useDatabase) {
        try (Client client = getClientV2(useDatabase)) {
            client.queryAll(query);
        }
    }

    public static void insertData(String tableName, InputStream dataStream, ClickHouseFormat format) {
        try (Client client = getClientV2();
             InsertResponse response = client.insert(tableName, dataStream, format).get()) {
            LOGGER.info("Rows inserted: {}", response.getWrittenRows());
        } catch (Exception e) {
            LOGGER.error("Error inserting data: ", e);
            throw new RuntimeException("Error inserting data", e);
        }
    }

    protected static ClickHouseClient getClientV1() {
        //We get a new client so that closing won't affect other subsequent calls
        return ClickHouseClient.newInstance(ClickHouseCredentials.fromUserAndPassword(getUsername(), getPassword()), ClickHouseProtocol.HTTP);
    }

    protected static Client getClientV2() {
        return getClientV2(true);
    }
    protected static Client getClientV2(boolean useDatabase) {
        ClickHouseNode node = getServer();
        //We get a new client so that closing won't affect other subsequent calls
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername(getUsername())
                .setPassword(getPassword())
                .compressClientRequest(true)
                .setMaxRetries(0)
                .setDefaultDatabase(useDatabase ? DB_NAME : "default")
                .build();
    }
}
