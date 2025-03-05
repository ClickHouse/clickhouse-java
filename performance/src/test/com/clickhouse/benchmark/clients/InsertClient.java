package com.clickhouse.benchmark.clients;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
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

import java.math.BigInteger;

import static com.clickhouse.client.ClickHouseServerForTest.isCloud;

@State(Scope.Benchmark)
public class InsertClient extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(InsertClient.class);
    ClickHouseClient clientV1;
    Client clientV2;

    @Setup(Level.Trial)
    public void setup(DataState dataState) throws Exception {
        super.setup(dataState, false);
    }

    @Setup(Level.Iteration)
    public void setUpIteration() {
        LOGGER.info("Setup Each Invocation");
        clientV1 = ClickHouseClient
                .newInstance(ClickHouseCredentials.fromUserAndPassword(getUsername(), getPassword()), ClickHouseProtocol.HTTP);
        ClickHouseNode node = getServer();
        clientV2 = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername(getUsername())
                .setPassword(getPassword())
                .compressClientRequest(true)
                .setMaxRetries(0)
                .setDefaultDatabase(DB_NAME)
                .build();

    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
        if (clientV1 != null) {
            clientV1.close();
            clientV1 = null;
        }
        if (clientV2 != null) {
            clientV2.close();
            clientV2 = null;
        }
    }

    @TearDown(Level.Invocation)
    public void tearDownIteration(DataState dataState) throws InterruptedException {
        try {
            try(QueryResponse response = clientV2.query("SELECT count(*) FROM `" + dataState.dataSet.getTableName() + "`").get()) {
                ClickHouseBinaryFormatReader reader = clientV2.newBinaryFormatReader(response);
                while (reader.next() != null) {//Compiler optimization avoidance
                    BigInteger count = reader.readValue(1);
                    if (count.longValue() != dataState.dataSet.getSize()) {
                        throw new IllegalStateException("Rows written: " + count + " Expected " +
                                dataState.dataSet.getSize() + " rows");
                    }
                }
            }
            try(QueryResponse response = clientV2.query("TRUNCATE TABLE IF EXISTS `" + dataState.dataSet.getTableName() + "`").get()) {
                ClickHouseBinaryFormatReader reader = clientV2.newBinaryFormatReader(response);
                while (reader.next() != null) {//Compiler optimization avoidance
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }
    @State(Scope.Thread)
    public static class InsertState {
        @Param({"simple"})
        String dataSetName;
        ClickHouseFormat format = ClickHouseFormat.JSONEachRow;
    }

    @Benchmark
    public void insertV1(DataState dataState, InsertState state) {
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
    public void insertV2(DataState dataState, InsertState state) {
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
