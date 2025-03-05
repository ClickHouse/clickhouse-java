package com.clickhouse.benchmark.clients;


import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@State(Scope.Benchmark)
public class ClientV1 extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientV1.class);
    ClickHouseClient client;

    @Setup(Level.Trial)
    public void setup(DataState dataState) throws Exception {
        super.setup(dataState, true);
    }

    @Setup(Level.Iteration)
    public void setUpIteration() {
        LOGGER.info("Setup Each Invocation");
        client = ClickHouseClient
                .newInstance(ClickHouseCredentials.fromUserAndPassword(getUsername(), getPassword()), ClickHouseProtocol.HTTP);
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @State(Scope.Thread)
    public static class V1State {
        @Param({"simple"})
        String dataSetName;
        ClickHouseFormat format = ClickHouseFormat.JSONEachRow;
    }


    @Benchmark
    public void query(DataState dataState, V1State state) {
        try {
            try (ClickHouseResponse response = client.read(getServer())
                    .query("SELECT * FROM `" + DB_NAME + "`.`" + dataState.dataSet.getTableName() + "`")
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .option(ClickHouseClientOption.ASYNC, false)
                    .executeAndWait()) {
                for (ClickHouseRecord record: response.records()) {//Compiler optimization avoidance
                    notNull(record.getValue(0));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

//    @Benchmark
//    public void insert(DataState dataState, V1State state) {
//        try {
//            ClickHouseFormat format = dataState.dataSet.getFormat();
//            try (ClickHouseResponse response = client.read(getServer())
//                    .write()
//                    .option(ClickHouseClientOption.ASYNC, false)
//                    .format(format)
//                    .query("INSERT INTO `" + DB_NAME + "`.`" + dataState.dataSet.getTableName() + "`")
//                    .data(out -> {
//                        for (byte[] bytes: dataState.dataSet.getBytesList(format)) {
//                            out.write(bytes);
//                        }
//                    })
//                    .executeAndWait()) {
//                ClickHouseResponseSummary summary = response.getSummary();
//                if (summary.getWrittenRows() <= 0) {
//                    throw new IllegalStateException("Rows written: " + summary.getWrittenRows());
//                }
//            }
//        } catch (Exception e) {
//            LOGGER.error("Error: ", e);
//        }
//    }

//    @Benchmark
//    public void insert() {
//        try {
//            try (ClickHouseResponse response = client.read(getEndpointUrl())
//                    .write()
//                    .format(dataSet.getFormat())
//                    .query("INSERT INTO `" + BenchmarkRunner.DB_NAME + "`.`" + dataSet.tableName + "`")
//                    .data(dataSet.getInputStream())
//                    .executeAndWait()) {
//                ClickHouseResponseSummary summary = response.getSummary();
//                long rowsWritten = summary.getWrittenRows();
//                if (rowsWritten != dataSet.size) {
//                    throw new IllegalStateException("Rows written: " + rowsWritten);
//                }
//            }
//        } catch (Exception e) {
//            LOGGER.error("Error: ", e);
//        }
//    }

}
