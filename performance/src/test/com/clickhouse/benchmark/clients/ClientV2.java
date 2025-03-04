package com.clickhouse.benchmark.clients;

import com.clickhouse.benchmark.BenchmarkRunner;
import com.clickhouse.benchmark.data.DataSet;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.api.query.QueryResponse;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.clickhouse.benchmark.BenchmarkRunner.notNull;

@State(Scope.Thread)
public class ClientV2 {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientV2.class);
    Client client;

    @Setup(Level.Iteration)
    public void setUp() {
        LOGGER.info("Setup Each Invocation");
        client = new Client.Builder()
                .addEndpoint(BenchmarkRunner.getEndpointUrl())
                .setUsername(BenchmarkRunner.getUsername())
                .setPassword(BenchmarkRunner.getPassword())
                .compressClientRequest(true)
                .setMaxRetries(0)
                .useHttpCompression(true)
                .build();
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
            try(QueryResponse response = client.query("SELECT * FROM `" + BenchmarkRunner.DB_NAME + "`.`" + dataSet.tableName + "`").get()) {
                ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
                while (reader.next() != null) {//Compiler optimization avoidance
                    notNull(reader.readValue(1));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    @Benchmark
    public void insert(DataSet dataSet) {
        try {
            try(InsertResponse response = client.insert(dataSet.tableName, dataSet.getInputStream(), dataSet.getFormat()).get()) {
                long rowsWritten = response.getMetrics().getMetric(ServerMetrics.NUM_ROWS_WRITTEN).getLong();
                if (rowsWritten != dataSet.size) {
                    throw new IllegalStateException("Rows written: " + rowsWritten);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }
}
