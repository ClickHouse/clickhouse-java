package com.clickhouse.benchmark.clients;

import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@State(Scope.Benchmark)
public class InsertClient extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(InsertClient.class);

    @Setup(Level.Trial)
    public void setup(DataState dataState) throws Exception {
        super.setup(dataState, false);
    }


    @TearDown(Level.Invocation)
    public void tearDownIteration(DataState dataState) throws InterruptedException {
        verifyRowsInsertedAndCleanup(dataState.dataSet);
    }


    @Benchmark
    public void insertV1(DataState dataState) {
        try {
            ClickHouseFormat format = dataState.dataSet.getFormat();
            try (ClickHouseResponse response = clientV1.read(getServer())
                    .write()
                    .option(ClickHouseClientOption.ASYNC, false)
                    .format(format)
                    .query("INSERT INTO `" + DB_NAME + "`.`" + dataState.dataSet.getTableName() + "`")
                    .data(out -> {
                        for (byte[] bytes: dataState.dataSet.getBytesList(format)) {
                            out.write(bytes);
                        }
                    })
                    .executeAndWait()) {
                ClickHouseResponseSummary summary = response.getSummary();
                if (summary.getWrittenRows() <= 0) {
                    throw new IllegalStateException("Rows written: " + summary.getWrittenRows());
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    @Benchmark
    public void insertV2(DataState dataState) {
        try {
            ClickHouseFormat format = dataState.dataSet.getFormat();
            try (InsertResponse response = clientV2.insert(dataState.dataSet.getTableName(), out -> {
                for (byte[] bytes: dataState.dataSet.getBytesList(format)) {
                    out.write(bytes);

                }
                out.close();
            }, format, new InsertSettings()).get()) {
                if (response.getWrittenRows() <= 0) {
                    throw new IllegalStateException("Rows written: " + response.getWrittenRows());
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }
}
