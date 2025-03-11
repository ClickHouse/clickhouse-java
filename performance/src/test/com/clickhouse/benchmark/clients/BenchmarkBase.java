package com.clickhouse.benchmark.clients;

import com.clickhouse.benchmark.BenchmarkRunner;
import com.clickhouse.benchmark.data.DataSet;
import com.clickhouse.benchmark.data.FileDataSet;
import com.clickhouse.benchmark.data.SimpleDataSet;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
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
import java.util.ArrayList;
import java.util.Collections;
import java.math.BigInteger;
import java.util.List;

import static com.clickhouse.benchmark.BenchmarkRunner.getSelectCountQuery;
import static com.clickhouse.benchmark.BenchmarkRunner.getSyncQuery;
import static com.clickhouse.benchmark.TestEnvironment.DB_NAME;
import static com.clickhouse.benchmark.TestEnvironment.cleanupEnvironment;
import static com.clickhouse.benchmark.TestEnvironment.getPassword;
import static com.clickhouse.benchmark.TestEnvironment.getServer;
import static com.clickhouse.benchmark.TestEnvironment.getUsername;
import static com.clickhouse.benchmark.TestEnvironment.isCloud;
import static com.clickhouse.benchmark.TestEnvironment.setupEnvironment;

@State(Scope.Benchmark)
public class BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkBase.class);

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
        @Param({"file://dataset_500k.csv"})
        String datasetSourceName;
        @Param({"300000", "220000", "100000", "10000"})
        int limit;
        @Param({"data_filled"})
        String tableNameFilled;
        @Param({"data_empty"})
        String tableNameEmpty;

        @Param({"true"})
        boolean useClientCompression = true;
        DataSet dataSet;

        public void setDataSet(DataSet dataSet) {
            this.dataSet = dataSet;
        }

        public void setDatasetSourceName(String datasetSourceName) {
            this.datasetSourceName = datasetSourceName;
        }

        public void setLimit(int limit) {
            this.limit = limit;
        }

        public void setTableNameFilled(String tableNameFilled) {
            this.tableNameFilled = tableNameFilled;
        }

        public void setTableNameEmpty(String tableNameEmpty) {
            this.tableNameEmpty = tableNameEmpty;
        }

    }

    @Setup(Level.Trial)
    public void setup(DataState dataState) {
        setupEnvironment();
        LOGGER.info("Setup benchmarks using dataset: {}", dataState.datasetSourceName);
        if (dataState.dataSet == null && "simple".equals(dataState.datasetSourceName)) {
            dataState.dataSet = new SimpleDataSet();
        } else if (dataState.dataSet == null && dataState.datasetSourceName.startsWith("file://")) {
            LOGGER.info("Loading data from file " + dataState.datasetSourceName + " with limit " + dataState.limit);
            dataState.dataSet = new FileDataSet(dataState.datasetSourceName.substring("file://".length()), dataState.limit);
        }
        initializeTables(dataState);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        cleanupEnvironment();
    }


    public static void initializeTables(DataState dataState) {
        LOGGER.info("Initializing tables: {}, {}", dataState.tableNameFilled, dataState.tableNameEmpty);
        LOGGER.debug("Create {}: {}", dataState.tableNameFilled, dataState.dataSet.getCreateTableString(dataState.tableNameFilled));
        LOGGER.debug("Create {}: {}", dataState.tableNameEmpty, dataState.dataSet.getCreateTableString(dataState.tableNameEmpty));
        runAndSyncQuery(dataState.dataSet.getCreateTableString(dataState.tableNameEmpty), dataState.tableNameEmpty);
        runAndSyncQuery(dataState.dataSet.getCreateTableString(dataState.tableNameFilled), dataState.tableNameFilled);
        //Truncate tables if they existed
        truncateTable(dataState.tableNameEmpty);
        truncateTable(dataState.tableNameFilled);

        ClickHouseFormat format = dataState.dataSet.getFormat();
        LOGGER.debug("Inserting data into table: {}, format: {}", dataState.tableNameFilled, format);
        insertData(dataState.tableNameFilled, dataState.dataSet.getInputStream(format), format);//For query testing
        loadClickHouseRecords(dataState);//For insert testing
    }



    public static void isNotNull(Object obj, boolean doWeCare) {
        if (obj == null && doWeCare) {
            throw new RuntimeException("Object is null");
        }
    }

    public static List<GenericRecord> runQuery(String query) {
        return runQuery(query, true);
    }
    public static List<GenericRecord> runQuery(String query, boolean useDatabase) {
        try (Client client = getClientV2(useDatabase)) {
            return client.queryAll(query);
        }
    }
    public static void runAndSyncQuery(String query, String tableName) {
        runQuery(query);
        syncQuery(tableName);
    }


    public static void syncQuery(String tableName) {
        if (isCloud()) {
            LOGGER.debug("Syncing: {}", tableName);
            runQuery(getSyncQuery(tableName));
        }
    }


    public static void truncateTable(String tableName) {
        LOGGER.info("Truncating table: {}", tableName);
        runAndSyncQuery(String.format("TRUNCATE TABLE IF EXISTS `%s`.`%s`", DB_NAME, tableName), tableName);
    }


    public static void insertData(String tableName, InputStream dataStream, ClickHouseFormat format) {
        try (Client client = getClientV2();
             InsertResponse ignored = client.insert(tableName, dataStream, format).get()) {
            syncQuery(tableName);
            List<GenericRecord> count = runQuery(getSelectCountQuery(tableName));
            LOGGER.info("Rows written: {}", count.get(0).getBigInteger(1));
        } catch (Exception e) {
            LOGGER.error("Error inserting data: ", e);
            throw new RuntimeException("Error inserting data", e);
        }
    }


    public static void verifyCount(String tableName, long expectedCount) {
        syncQuery(tableName);
        List<GenericRecord> records = runQuery(BenchmarkRunner.getSelectCountQuery(tableName));
        BigInteger count = records.get(0).getBigInteger(1);
        if (count.longValue() != expectedCount) {
            throw new IllegalStateException(String.format("Expected %d rows but got %d", expectedCount, count));
        }
    }

    protected static ClickHouseClient getClientV1() {
        //We get a new client so that closing won't affect other subsequent calls
        return ClickHouseClient.newInstance(ClickHouseCredentials.fromUserAndPassword(getUsername(), getPassword()), ClickHouseProtocol.HTTP);
    }
    protected static Client getClientV2() {
        return getClientV2(true);
    }
    protected static Client getClientV2(boolean includeDb) {
        ClickHouseNode node = getServer();
        //We get a new client so that closing won't affect other subsequent calls
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername(getUsername())
                .setPassword(getPassword())
                .compressClientRequest(true)
                .setMaxRetries(0)
                .setDefaultDatabase(includeDb ? DB_NAME : "default")
                .build();
    }

    public static void loadClickHouseRecords(DataState dataState) {
        syncQuery(dataState.tableNameFilled);

        try (ClickHouseClient clientV1 = getClientV1();
             ClickHouseResponse response = clientV1.read(getServer())
                     .query(BenchmarkRunner.getSelectQuery(dataState.tableNameFilled))
                     .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                     .executeAndWait()) {

            // Create a data processor to serialize data in ClientV1 tests
            ClickHouseDataProcessor dataProcessor= new ClickHouseRowBinaryProcessor(clientV1.getConfig(), null,
                    ClickHouseOutputStream.of(new ByteArrayOutputStream()), response.getColumns(), Collections.emptyMap());
            assert dataProcessor.getColumns() != null;
            dataState.dataSet.setClickHouseDataProcessor(dataProcessor);
            ArrayList<ClickHouseRecord> records = new ArrayList<>();
            for (ClickHouseRecord record : response.records()) {
                records.add(record);
            }
            LOGGER.info("Rows read size: {}", records.size());

            dataState.dataSet.setClickHouseRecords(records);
        } catch (Exception e) {
            LOGGER.error("Error inserting data: ", e);
            throw new RuntimeException("Error inserting data", e);
        }
    }
}
