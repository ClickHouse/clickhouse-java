package com.clickhouse.benchmark.clients;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.stream.NonBlockingPipedOutputStream;
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

    @Setup(Level.Trial)
    public void setup(DataState dataState) throws Exception {
        super.setup(dataState, true);
    }

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
                .setLZ4UncompressedBufferSize(2 * 1024 * 1024)
                .setClientNetworkBufferSize(20 * 1024 * 1024)
                .setSocketRcvbuf(2 * 1024 * 1024)
                .setSocketSndbuf(2 * 1024 * 1024)
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
    public void query(DataState dataState, V2State state) {
        try {
            try(QueryResponse response = client.query("SELECT * FROM `" + dataState.dataSet.getTableName() + "`").get()) {
                ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
                while (reader.next() != null) {//Compiler optimization avoidance
                    notNull(reader.readValue(1));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

//    @Benchmark
//    public void insert(DataState dataState, V2State state) {
//        try {
//            ClickHouseFormat format = dataState.dataSet.getFormat();
//            try (InsertResponse response = client.insert(dataState.dataSet.getTableName(), out -> {
//                for (byte[] bytes: dataState.dataSet.getBytesList(format)) {
//                    out.write(bytes);
//
//                }
//                out.close();
//            }, format, new InsertSettings()).get()) {
//                if (response.getWrittenRows() <= 0) {
//                    throw new IllegalStateException("Rows written: " + response.getWrittenRows());
//                }
//            }
//        } catch (Exception e) {
//            LOGGER.error("Error: ", e);
//        }
//    }
}
