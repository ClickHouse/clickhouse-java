package com.clickhouse.benchmark.clients;

import com.clickhouse.benchmark.BenchmarkRunner;
import com.clickhouse.benchmark.data.DataSet;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseSerializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.clickhouse.benchmark.TestEnvironment.getServer;
@Threads(3)
@State(Scope.Thread)
public class ConcurrentInsertClient extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentInsertClient.class);
    private static final AtomicInteger GLOBAL_ID = new AtomicInteger(0);
    private final ThreadLocal<Integer> invocationId = new ThreadLocal<>();
    @State(Scope.Benchmark)
    public static class GlobalState {
        ClickHouseClient clientV1Global = BenchmarkBase.getClientV1();
        Client clientV2Global = BenchmarkBase.getClientV2();
        ClickHouseClient getClientV1() {
            return clientV1Global;
        }
        Client getClientV2() {
            return clientV2Global;
        }
    }

    private String createTableName(int id) {
        return String.format("%s_%d", "concurrent_data_empty", id);
    }

    @Setup(Level.Invocation)
    public void setup() throws InterruptedException {
        int id = GLOBAL_ID.incrementAndGet();
        invocationId.set(id);
        DataSet dataSet = DataState.getDataSet();
        String tableName = createTableName(id);
        LOGGER.warn("setup create table name: " + tableName);
        // create table
        String createTableString = dataSet.getCreateTableString(tableName);
        runAndSyncQuery(createTableString, tableName);
    }
    @TearDown(Level.Invocation)
    public void verifyRowsInsertedAndCleanup(DataState dataState) throws InterruptedException {
        boolean success;
        String tableName = createTableName(invocationId.get());
        LOGGER.warn("TearDown: " + tableName);
        int count = 0;
        do {
            success = verifyCount(tableName, dataState.dataSet.getSize());
            if (!success) {
                LOGGER.warn("Retrying to verify rows inserted");
                try {
                    Thread.sleep(2500);
                } catch (InterruptedException e) {
                    LOGGER.error("Error: ", e);
                }
            }
        } while (!success && count++ < 10);
        if (!success) {
            LOGGER.error("Failed to verify rows inserted");
            throw new RuntimeException("Failed to verify rows inserted");
        }
        truncateTable(tableName);
    }
    @Benchmark
    public void insertV1(DataState dataState, GlobalState globalState) {
        int id = invocationId.get();
        String tableName = createTableName(id);
//        System.out.println(Thread.currentThread().getName() + " is executing insertV1:[" + id + "] " + globalState.getClientV1().hashCode());
        try {
            ClickHouseFormat format = dataState.dataSet.getFormat();
            try (ClickHouseResponse response = globalState.getClientV1().read(getServer())
                    .write()
                    .option(ClickHouseClientOption.ASYNC, false)
                    .format(format)
                    .query(BenchmarkRunner.getInsertQuery(tableName))
                    .data(out -> {
                        for (byte[] bytes: dataState.dataSet.getBytesList(format)) {
                            out.write(bytes);
                        }
                    }).executeAndWait()) {
                response.getSummary();
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    @Benchmark
    public void insertV2(DataState dataState, GlobalState globalState) {
        int id = invocationId.get();
//        System.out.println(Thread.currentThread().getName() + " is executing insertV2:[" + id + "] " + globalState.getClientV2().hashCode());
        String tableName = createTableName(id);
        LOGGER.warn("insertV2: " + tableName);
        try {
            ClickHouseFormat format = dataState.dataSet.getFormat();
            try (InsertResponse response = globalState.getClientV2().insert(tableName, out -> {
                for (byte[] bytes: dataState.dataSet.getBytesList(format)) {
                    out.write(bytes);
                }
                out.close();
            }, format, new InsertSettings().setDeduplicationToken("insert_v2")).get()) {
                response.getWrittenRows();
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    @Benchmark
    public void insertV1Compressed(DataState dataState, GlobalState globalState) {
        try {
            ClickHouseFormat format = dataState.dataSet.getFormat();
            try (ClickHouseResponse response = globalState.getClientV1().read(getServer())
                    .write()
                    .option(ClickHouseClientOption.ASYNC, false)
                    .option(ClickHouseClientOption.DECOMPRESS, true)
                    .format(format)
                    .query(BenchmarkRunner.getInsertQuery(dataState.tableNameEmpty))
                    .data(out -> {
                        for (byte[] bytes: dataState.dataSet.getBytesList(format)) {
                            out.write(bytes);
                        }
                    }).executeAndWait()) {
                response.getSummary();
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }
    @Benchmark
    public void insertV2Compressed(DataState dataState, GlobalState globalState) {
        try {
            ClickHouseFormat format = dataState.dataSet.getFormat();
            try (InsertResponse response = globalState.getClientV2().insert(dataState.tableNameEmpty, out -> {
                for (byte[] bytes: dataState.dataSet.getBytesList(format)) {
                    out.write(bytes);
                }
                out.close();
            }, format, new InsertSettings()
                    .compressClientRequest(true)).get()) {
                response.getWrittenRows();
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    @Benchmark
    public void insertV1RowBinary(DataState dataState, GlobalState globalState) {
        try {
            ClickHouseFormat format = ClickHouseFormat.RowBinary;
            try (ClickHouseResponse response = globalState.getClientV1().read(getServer())
                    .write()
                    .option(ClickHouseClientOption.ASYNC, false)
                    .format(format)
                    .query(BenchmarkRunner.getInsertQuery(dataState.tableNameEmpty))
                    .data(out -> {
                        ClickHouseDataProcessor p = dataState.dataSet.getClickHouseDataProcessor();
                        ClickHouseSerializer[] serializers = p.getSerializers(clientV1.getConfig(), p.getColumns());
                        for (ClickHouseRecord record : dataState.dataSet.getClickHouseRecords()) {
                            for (int i = 0; i < serializers.length; i++) {
                                serializers[i].serialize(record.getValue(i), out);
                            }
                        }
                    })
                    .executeAndWait()) {
                response.getSummary();
            }
        } catch ( Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    @Benchmark
    public void insertV2RowBinary(DataState dataState, GlobalState globalState) {
        try {
            try (InsertResponse response = globalState.getClientV2().insert(dataState.tableNameEmpty, out -> {
                RowBinaryFormatWriter w = new RowBinaryFormatWriter(out, dataState.dataSet.getSchema(), ClickHouseFormat.RowBinary);
                for (List<Object> row : dataState.dataSet.getRowsOrdered()) {
                    int index = 1;
                    for (Object value : row) {
                        w.setValue(index, value);
                        index++;
                    }
                    w.commitRow();
                }
                out.flush();

            }, ClickHouseFormat.RowBinaryWithDefaults, new InsertSettings()).get()) {
                response.getWrittenRows();
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }
}
