package com.clickhouse.benchmark.clients;

import com.clickhouse.client.ClickHouseResponse;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@State(Scope.Benchmark)
public class QueryClient extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryClient.class);

    @Setup(Level.Trial)
    public void setup(DataState dataState) throws Exception {
        super.setup(dataState, true);
    }


    @Benchmark
    public void queryV1(DataState dataState) {
        try {
            try (ClickHouseResponse response = clientV1.read(getServer())
                    .query("SELECT * FROM `" + DB_NAME + "`.`" + dataState.dataSet.getTableName() + "` LIMIT " + dataState.limit)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .option(ClickHouseClientOption.ASYNC, false)
                    .executeAndWait()) {
                for (ClickHouseRecord record: response.records()) {//Compiler optimization avoidance
                    for (int i = 0; i < dataState.dataSet.getSchema().getColumns().size(); i++) {
                        isNotNull(record.getValue(i), false);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    @Benchmark
    public void queryV2(DataState dataState) {
        try {
            try(QueryResponse response = clientV2.query("SELECT * FROM `" + dataState.dataSet.getTableName() + "` LIMIT " + dataState.limit).get()) {
                ClickHouseBinaryFormatReader reader = clientV2.newBinaryFormatReader(response);
                while (reader.next() != null) {//Compiler optimization avoidance
                    for (int i = 1; i <= dataState.dataSet.getSchema().getColumns().size(); i++) {
                        isNotNull(reader.readValue(1), false);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }
}
