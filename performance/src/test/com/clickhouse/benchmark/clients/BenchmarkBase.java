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
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.format.ClickHouseRowBinaryProcessor;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;

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

    public static void loadClickHouseRecords(String tableName, DataSet dataSet) {
        ClickHouseNode node = getServer();

        try (ClickHouseClient clientV1 = ClickHouseClient
                .newInstance(ClickHouseCredentials.fromUserAndPassword(getUsername(), getPassword()), ClickHouseProtocol.HTTP);
             ClickHouseResponse response = clientV1.read(node).query("SELECT * FROM " + DB_NAME + "." + tableName)
                     .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                     .executeAndWait()) {

            // Create a data processor to serialize data in ClientV1 tests
            ClickHouseDataProcessor dataProcessor= new ClickHouseRowBinaryProcessor(clientV1.getConfig(), null,
                    ClickHouseOutputStream.of(new ByteArrayOutputStream()), response.getColumns(), Collections.emptyMap());
            assert dataProcessor.getColumns() != null;
            dataSet.setClickHouseDataProcessor(dataProcessor);
            ArrayList<ClickHouseRecord> records = new ArrayList<>();
            for (ClickHouseRecord record : response.records()) {
                records.add(record);
            }

            dataSet.setClickHouseRecords(records);
        } catch (Exception e) {
            LOGGER.error("Error inserting data: ", e);
            throw new RuntimeException("Error inserting data", e);
        }
    }
}
