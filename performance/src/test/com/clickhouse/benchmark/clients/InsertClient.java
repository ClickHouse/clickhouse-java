package com.clickhouse.benchmark.clients;

import com.clickhouse.benchmark.BenchmarkRunner;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseSerializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.clickhouse.benchmark.TestEnvironment.getServer;

@State(Scope.Benchmark)
public class InsertClient extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(InsertClient.class);

    @TearDown(Level.Invocation)
    public void verifyRowsInsertedAndCleanup(DataState dataState) {
        verifyCount(dataState.tableNameEmpty, dataState.dataSet.getSize());
        truncateTable(dataState.tableNameEmpty);
        syncQuery(dataState.tableNameEmpty);
    }

    @Benchmark
    public void insertV1(DataState dataState) {
        try {
            ClickHouseFormat format = dataState.dataSet.getFormat();
            try (ClickHouseResponse response = clientV1.read(getServer())
                    .write()
                    .option(ClickHouseClientOption.ASYNC, false)
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
    public void insertV2(DataState dataState) {
        try {
            ClickHouseFormat format = dataState.dataSet.getFormat();
            try (InsertResponse response = clientV2.insert(dataState.tableNameEmpty, out -> {
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
    public void insertV1RowBinary(DataState dataState) {
        try {
            ClickHouseFormat format = ClickHouseFormat.RowBinary;
            try (ClickHouseResponse response = clientV1.read(getServer())
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
    public void insertV2RowBinary(DataState dataState) {
        try {
            try (InsertResponse response = clientV2.insert(dataState.tableNameEmpty, out -> {
                RowBinaryFormatWriter w = new RowBinaryFormatWriter(out, dataState.dataSet.getSchema(), ClickHouseFormat.RowBinary);
                List<ClickHouseColumn> columns = dataState.dataSet.getSchema().getColumns();
                for (Map<String, Object> row : dataState.dataSet.getRows()) {
                    for (ClickHouseColumn column : columns) {
                        w.setValue(column.getColumnName(),row.get(column.getColumnName()));
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
