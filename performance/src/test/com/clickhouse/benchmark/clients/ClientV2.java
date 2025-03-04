package com.clickhouse.benchmark.clients;

import com.clickhouse.benchmark.BenchmarkRunner;
import com.clickhouse.benchmark.data.DataSets;
import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.data.ClickHouseFormat;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.clickhouse.client.ClickHouseServerForTest.isCloud;

@State(Scope.Thread)
public class ClientV2 extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientV2.class);
    Client client;

    @Setup(Level.Iteration)
    public void setUpIteration() {
        LOGGER.info("Setup Each Invocation");

        ClickHouseNode node = getServer();
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername(getUsername())
                .setPassword(getPassword())
                .compressClientRequest(true)
                .setMaxRetries(0)
                .useHttpCompression(true)
                .setDefaultDatabase(DB_NAME)
                .build();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @State(Scope.Thread)
    public static class V2State {
        @Param({"simple"})
        String dataSetName;
        ClickHouseFormat format = ClickHouseFormat.JSONEachRow;
    }


    @Benchmark
    public void query(V2State state) {
        try {
            try(QueryResponse response = client.query("SELECT * FROM `" + dataSet.getTableName() + "`").get()) {
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
    public void insert(V2State state) {
        try {
            try (InsertResponse response = client.insert(dataSet.getTableName(), out -> {
                for (byte[] bytes: dataSet.getBytesList(state.format)) {
                    out.write(bytes);
                }
            }, state.format, new InsertSettings()).get()) {
                if (response.getWrittenRows() <= 0) {
                    throw new IllegalStateException("Rows written: " + response.getWrittenRows());
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }
}
