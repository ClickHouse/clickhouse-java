package com.clickhouse.benchmark.clients;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseSerializer;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.clickhouse.benchmark.TestEnvironment.getServer;

@Threads(3)
@State(Scope.Benchmark)
public class MixedWorkload extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(MixedWorkload.class);
    private static final int LIMIT = 10000;

    private ClickHouseClient clientV1Shared;
    private Client clientV2Shared;
    @Setup(Level.Trial)
    public void setUpTrial() {
        clientV1Shared = getClientV1();
        clientV2Shared = getClientV2();
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        if (clientV1Shared != null) {
            clientV1Shared.close();
            clientV1Shared = null;
        }
        if (clientV2Shared != null) {
            clientV2Shared.close();
            clientV2Shared = null;
        }
    }


    @TearDown(Level.Iteration)
    public void teardownIteration(DataState dataState) {
        truncateTable(dataState.tableNameEmpty);
    }



    @Benchmark
    @Group("mixed_v1")
    public void insertV1(DataState dataState) {
        try {
            ClickHouseFormat format = dataState.dataSet.getFormat();
            try (ClickHouseResponse response = clientV1Shared.read(getServer())
                    .write()
                    .option(ClickHouseClientOption.ASYNC, false)
                    .format(format)
                    .query(BenchmarkBase.getInsertQuery(dataState.tableNameEmpty))
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
    @Group("mixed_v1")
    public void insertV1RowBinary(DataState dataState) {
        try {
            ClickHouseFormat format = ClickHouseFormat.RowBinary;
            try (ClickHouseResponse response = clientV1Shared.read(getServer())
                    .write()
                    .option(ClickHouseClientOption.ASYNC, false)
                    .format(format)
                    .query(BenchmarkBase.getInsertQuery(dataState.tableNameEmpty))
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
    @Group("mixed_v1")
    public void queryV1(DataState dataState, Blackhole blackhole) {
        try {
            try (ClickHouseResponse response = clientV1Shared.read(getServer())
                    .query(BenchmarkBase.getSelectQueryWithLimit(dataState.tableNameFilled, LIMIT))
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .option(ClickHouseClientOption.ASYNC, false)
                    .executeAndWait()) {
                for (ClickHouseRecord record: response.records()) {//Compiler optimization avoidance
                    for (int i = 0; i < dataState.dataSet.getSchema().getColumns().size(); i++) {
                        blackhole.consume(record.getValue(i).asObject());
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }






    @Benchmark
    @Group("mixed_v2")
    public void insertV2(DataState dataState) {
        try {
            ClickHouseFormat format = dataState.dataSet.getFormat();
            try (InsertResponse response = clientV2Shared.insert(dataState.tableNameEmpty, out -> {
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
    @Group("mixed_v2")
    public void insertV2RowBinary(DataState dataState) {
        try {
            try (InsertResponse response = clientV2Shared.insert(dataState.tableNameEmpty, out -> {
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

    @Benchmark
    @Group("mixed_v2")
    public void queryV2(DataState dataState, Blackhole blackhole) {
        try {
            try(QueryResponse response = clientV2Shared.query(BenchmarkBase.getSelectQueryWithLimit(dataState.tableNameFilled, LIMIT)).get()) {
                ClickHouseBinaryFormatReader reader = clientV2Shared.newBinaryFormatReader(response);
                while (reader.next() != null) {//Compiler optimization avoidance
                    for (int i = 1; i <= dataState.dataSet.getSchema().getColumns().size(); i++) {
                        blackhole.consume(reader.readValue(1));
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }
}
