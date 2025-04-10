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

import static com.clickhouse.benchmark.TestEnvironment.getServer;
@Threads(3)
@State(Scope.Benchmark)
public class ConcurrentInsertClient extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentInsertClient.class);
    private static ClickHouseClient clientV1Shared;
    private static Client clientV2Shared;
    @Setup(Level.Trial)
    public void setUpIteration() {
        clientV1Shared = getClientV1();
        clientV2Shared = getClientV2();
    }
    @TearDown(Level.Trial)
    public void tearDownIteration() {
        if (clientV1Shared != null) {
            clientV1Shared.close();
            clientV1Shared = null;
        }
        if (clientV2Shared != null) {
            clientV2Shared.close();
            clientV2Shared = null;
        }
    }
    @State(Scope.Thread)
    public static class ThreadLocalState {
        public String createTableName() {
            String name = Thread.currentThread().getName();
            int index = name.lastIndexOf("-");
            String id = name.substring(index + 1);
            return String.format("%s_%s", "concurrent_data_empty", id);
        }
        @Setup(Level.Trial)
        public void setup() {
            DataSet dataSet = DataState.getDataSet();
            String tableName = createTableName();
            LOGGER.warn("setup create table name: " + tableName);
            // create table
            String createTableString = dataSet.getCreateTableString(tableName);
            runAndSyncQuery(createTableString, tableName);
        }

        @TearDown(Level.Invocation)
        public void verifyRowsInsertedAndCleanup(DataState dataState) {
            String tableName = createTableName();
            boolean success;
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
    }


    @Benchmark
    public void insertV1(DataState dataState, ThreadLocalState threadLocalState) {
        String tableName = threadLocalState.createTableName();
        try {
            ClickHouseFormat format = dataState.dataSet.getFormat();
            try (ClickHouseResponse response = clientV1Shared.read(getServer())
                    .write()
                    .option(ClickHouseClientOption.ASYNC, false)
                    .format(format)
                    .query(BenchmarkBase.getInsertQuery(tableName))
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
    public void insertV2(DataState dataState, ThreadLocalState threadLocalState) {
        String tableName = threadLocalState.createTableName();
        LOGGER.warn("insertV2: " + tableName);
        try {
            ClickHouseFormat format = dataState.dataSet.getFormat();
            try (InsertResponse response = clientV2Shared.insert(tableName, out -> {
                for (byte[] bytes: dataState.dataSet.getBytesList(format)) {
                    out.write(bytes);
                }
                out.close();
            }, format, new InsertSettings()).get()) {
                response.getWrittenRows();
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    @Benchmark
    public void insertV1Compressed(DataState dataState, ThreadLocalState threadLocalState) {
        try {
            ClickHouseFormat format = dataState.dataSet.getFormat();
            try (ClickHouseResponse response = clientV1Shared.read(getServer())
                    .write()
                    .option(ClickHouseClientOption.ASYNC, false)
                    .option(ClickHouseClientOption.DECOMPRESS, true)
                    .format(format)
                    .query(BenchmarkBase.getInsertQuery(threadLocalState.createTableName()))
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
    public void insertV2Compressed(DataState dataState, ThreadLocalState threadLocalState) {
        try {
            ClickHouseFormat format = dataState.dataSet.getFormat();
            try (InsertResponse response = clientV2Shared.insert(threadLocalState.createTableName(), out -> {
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
    public void insertV1RowBinary(DataState dataState, ThreadLocalState threadLocalState) {
        try {
            ClickHouseFormat format = ClickHouseFormat.RowBinary;
            try (ClickHouseResponse response = clientV1Shared.read(getServer())
                    .write()
                    .option(ClickHouseClientOption.ASYNC, false)
                    .format(format)
                    .query(BenchmarkBase.getInsertQuery(threadLocalState.createTableName()))
                    .data(out -> {
                        ClickHouseDataProcessor p = dataState.dataSet.getClickHouseDataProcessor();
                        ClickHouseSerializer[] serializers = p.getSerializers(clientV1Shared.getConfig(), p.getColumns());
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
    public void insertV2RowBinary(DataState dataState, ThreadLocalState threadLocalState) {
        try {
            try (InsertResponse response = clientV2Shared.insert(threadLocalState.createTableName(), out -> {
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
