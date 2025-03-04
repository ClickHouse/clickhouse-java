package com.clickhouse.benchmark.clients;


import com.clickhouse.benchmark.BenchmarkRunner;
import com.clickhouse.benchmark.data.DataSet;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.clickhouse.benchmark.BenchmarkRunner.LARGE_SIZE;
import static com.clickhouse.benchmark.BenchmarkRunner.MEDIUM_SIZE;
import static com.clickhouse.benchmark.BenchmarkRunner.SMALL_SIZE;
import static com.clickhouse.benchmark.BenchmarkRunner.getEndpointUrl;
import static com.clickhouse.benchmark.BenchmarkRunner.getPassword;
import static com.clickhouse.benchmark.BenchmarkRunner.getUsername;
import static com.clickhouse.benchmark.BenchmarkRunner.notNull;

@State(Scope.Benchmark)
public class ClientV1 {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientV1.class);
    ClickHouseClient client;

    @Setup(Level.Iteration)
    public void setUp() {
        LOGGER.info("Setup Each Invocation");
        client = ClickHouseClient.newInstance(ClickHouseCredentials.fromUserAndPassword(getUsername(), getPassword()), ClickHouseProtocol.HTTP);
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        if (client != null) {
            client.close();
            client = null;
        }
    }


    @Benchmark
    public void query(DataSet dataSet) {
        try {
            try (ClickHouseResponse response = client.read(getEndpointUrl())
                    .query("SELECT * FROM `" + BenchmarkRunner.DB_NAME + "`.`" + dataSet.tableName + "`")
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .executeAndWait()) {
                for (ClickHouseRecord record: response.records()) {//Compiler optimization avoidance
                    notNull(record.getValue(0));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    @Benchmark
    public void insert(DataSet dataSet) {
        try {
            try (ClickHouseResponse response = client.read(getEndpointUrl())
                    .write()
                    .format(dataSet.getFormat())
                    .query("INSERT INTO `" + BenchmarkRunner.DB_NAME + "`.`" + dataSet.tableName + "`")
                    .data(dataSet.getInputStream())
                    .executeAndWait()) {
                ClickHouseResponseSummary summary = response.getSummary();
                long rowsWritten = summary.getWrittenRows();
                if (rowsWritten != dataSet.size) {
                    throw new IllegalStateException("Rows written: " + rowsWritten);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }




}
