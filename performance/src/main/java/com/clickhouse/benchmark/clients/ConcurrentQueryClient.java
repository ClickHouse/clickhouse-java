package com.clickhouse.benchmark.clients;

import com.clickhouse.benchmark.BenchmarkRunner;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.clickhouse.benchmark.TestEnvironment.*;

@Threads(3)
@State(Scope.Benchmark)
public class ConcurrentQueryClient extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentQueryClient.class);
    private static int LIMIT = 10000;
    private ClickHouseClient clientV1Shared;
    private Client clientV2Shared;
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

    @TearDown(Level.Iteration)
    public void teardownIteration(DataState dataState) {
        truncateTable(dataState.tableNameEmpty);
    }
    @Benchmark
    public void queryV1(DataState dataState, Blackhole blackhole) {
        try {
            try (ClickHouseResponse response = clientV1Shared.read(getServer())
                    .query(BenchmarkBase.getSelectQueryWithLimit(dataState.tableNameFilled, LIMIT))
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .option(ClickHouseClientOption.ASYNC, false)
                    .executeAndWait()) {
                for (ClickHouseRecord record: response.records()) {//Compiler optimization avoidance
                    for (int i = 0; i < dataState.dataSet.getSchema().getColumns().size(); i++) {
                        blackhole.consume(record.getValue(i));
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    @Benchmark
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
