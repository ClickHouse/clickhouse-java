package com.clickhouse.benchmark.clients;

import com.clickhouse.benchmark.BenchmarkRunner;
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
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.format.ClickHouseRowBinaryProcessor;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.math.BigInteger;
import java.util.List;

import static com.clickhouse.benchmark.BenchmarkRunner.getSelectCountQuery;
import static com.clickhouse.benchmark.BenchmarkRunner.getSyncQuery;

@State(Scope.Benchmark)
public class BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkBase.class);
    public static final String DB_NAME = "benchmarks";

    protected ClickHouseClient clientV1;
    protected Client clientV2;
    @Setup(Level.Iteration)
    public void setUpIteration() {
        clientV1 = getClientV1();
        clientV2 = getClientV2();

    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
        if (clientV1 != null) {
            clientV1.close();
            clientV1 = null;
        }
        if (clientV2 != null) {
            clientV2.close();
            clientV2 = null;
        }
    }

    @State(Scope.Benchmark)
    public static class DataState {
        @Param({"simple"})
        String datasetSourceName;
        @Param({"300000", "220000", "100000", "10000"})
        int limit;

        DataSet dataSet;
    }

    public void setup(DataState dataState, boolean insertData) throws Exception {
        LOGGER.info("Setup BenchmarkBase using " + dataState.datasetSourceName + " dataset.");
        if ("simple".equals(dataState.datasetSourceName) && dataState.dataSet == null) {
            dataState.datasetSourceName = "simple";
            dataState.dataSet = new SimpleDataSet();
        } else if (dataState.datasetSourceName.startsWith("file://")) {

            dataState.dataSet = new FileDataSet(dataState.datasetSourceName.substring("file://".length()), dataState.limit);
            dataState.datasetSourceName = dataState.dataSet.getName();
        }

        LOGGER.debug("BenchmarkBase setup(). Data source " + dataState.datasetSourceName);
        //BaseIntegrationTest.setupClickHouseContainer();
        runQuery("CREATE DATABASE IF NOT EXISTS " + DB_NAME, false);
        DataSets.initializeTables(dataState.dataSet, insertData);
        syncQuery(dataState.dataSet);
    }

    public void tearDown() {
        runQuery("DROP DATABASE IF EXISTS " + DB_NAME, false);
        //BaseIntegrationTest.teardownClickHouseContainer();
    }


    //Connection parameters
    public static boolean isCloud() {
        return true;
    }
    public static String getPassword() {
        return "";
    }
    public static String getUsername() {
        return "default";
    }
    public static ClickHouseNode getServer() {
        return ClickHouseNode.builder(ClickHouseNode.builder().build())
                .address(ClickHouseProtocol.HTTP, new InetSocketAddress("", 8443))
                .credentials(ClickHouseCredentials.fromUserAndPassword(getUsername(), getPassword()))
                .options(Collections.singletonMap(ClickHouseClientOption.SSL.getKey(), "true"))
                .database(DB_NAME)
                .build();
//        return ClickHouseServerForTest.getClickHouseNode(ClickHouseProtocol.HTTP, isCloud(), ClickHouseNode.builder().build());
    }
    public static void isNotNull(Object obj, boolean doWeCare) {
        if (obj == null && doWeCare) {
            throw new RuntimeException("Object is null");
        }
    }

    public static List<GenericRecord> runQuery(String query, boolean useDatabase) {
        try (Client client = getClientV2(useDatabase)) {
            return client.queryAll(query);
        }
    }

    public static void syncQuery(DataSet dataSet) {
        if (isCloud()) {
            LOGGER.debug("{}", getSyncQuery(dataSet.getTableName()));
            runQuery(getSyncQuery(dataSet.getTableName()), true);
        }
    }

    public static void insertData(String tableName, InputStream dataStream, ClickHouseFormat format) {
        try (Client client = getClientV2();
             InsertResponse ignored = client.insert(tableName, dataStream, format).get()) {
            if (isCloud()) {
                runQuery(getSyncQuery(tableName), true);
            }
            List<GenericRecord> count = runQuery("SELECT COUNT(*) FROM `" + DB_NAME + "`.`" + tableName + "`", true);
            LOGGER.info("Rows written: {}", count.get(0).getBigInteger(1));
        } catch (Exception e) {
            LOGGER.error("Error inserting data: ", e);
            throw new RuntimeException("Error inserting data", e);
        }
    }

    public static void verifyRowsInsertedAndCleanup(DataSet dataSet) {
        try {
            syncQuery(dataSet);
            List<GenericRecord> records = runQuery(BenchmarkRunner.getSelectCountQuery(dataSet), true);
            BigInteger count = records.get(0).getBigInteger(1);
            if (count.longValue() != dataSet.getSize()) {
                throw new IllegalStateException("Rows written: " + count + " Expected " + dataSet.getSize() + " rows");
            }
            runQuery("TRUNCATE TABLE IF EXISTS `" + dataSet.getTableName() + "`", true);
            syncQuery(dataSet);
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
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

    public static void loadClickHouseRecords(DataSet dataSet) {
        syncQuery(dataSet);

        try (ClickHouseClient clientV1 = getClientV1();
             ClickHouseResponse response = clientV1.read(getServer())
                     .query(BenchmarkRunner.getSelectQuery(dataSet))
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
            LOGGER.info("Rows read size: {}", records.size());

            dataSet.setClickHouseRecords(records);
        } catch (Exception e) {
            LOGGER.error("Error inserting data: ", e);
            throw new RuntimeException("Error inserting data", e);
        }
    }
}
