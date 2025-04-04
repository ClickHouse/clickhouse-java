package com.clickhouse.benchmark.clients;

import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.clickhouse.benchmark.TestEnvironment.getServer;

@State(Scope.Benchmark)
public class QueryClient extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryClient.class);

    @Benchmark
    public void queryV1(DataState dataState, Blackhole blackhole) {
        try {
            try (ClickHouseResponse response = clientV1.read(getServer())
                    .query(BenchmarkBase.getSelectQuery(dataState.tableNameFilled))
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
    public void queryV2(DataState dataState, Blackhole blackhole) {
        try {
            try(QueryResponse response = clientV2.query(BenchmarkBase.getSelectQuery(dataState.tableNameFilled)).get()) {
                ClickHouseBinaryFormatReader reader = clientV2.newBinaryFormatReader(response);
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
