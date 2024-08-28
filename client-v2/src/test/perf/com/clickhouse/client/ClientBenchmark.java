package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.insert.InsertTests;
import com.clickhouse.client.query.QueryTests;
import com.clickhouse.data.ClickHouseFormat;
import org.openjdk.jmh.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import static com.clickhouse.client.ClickHouseServerForTest.isCloud;
import static org.testng.Assert.assertEquals;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
public class ClientBenchmark {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientBenchmark.class);
    private static final int SMALL_SIZE = 1000;
    private static final int MEDIUM_SIZE = 1000000;
    private static final int LARGE_SIZE = 10000000;

    private Client newClient;
    private Client oldClient;

    private InsertTests insertTests;
    private QueryTests queryTests;

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @Setup
    public void setup() throws IOException {
        BaseIntegrationTest.setupClickHouseContainer();

        ClickHouseNode node = ClickHouseServerForTest.getClickHouseNode(ClickHouseProtocol.HTTP, isCloud(), ClickHouseNode.builder().build());
        oldClient = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), false)
                .setUsername("default")
                .setPassword("")
                .compressClientRequest(false)
                .compressServerResponse(false)
                .useHttpCompression(false)
                .useNewImplementation(false)
                .build();
        newClient = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), false)
                .setUsername("default")
                .setPassword("")
                .compressClientRequest(false)
                .compressServerResponse(false)
                .useHttpCompression(false)
                .useNewImplementation(true)
                .build();
    }

    @Setup(Level.Invocation)
    public void setupEachInvocation() throws IOException {
        queryTests = new QueryTests(false, false);
        queryTests.setUp();

        insertTests = new InsertTests(false, false);
        insertTests.setUp();
    }

    @TearDown
    public void tearDown() {
        oldClient.close();
        newClient.close();
        insertTests.tearDown();
        queryTests.tearDown();
        BaseIntegrationTest.teardownClickHouseContainer();
    }



    // Add benchmark methods here
    @Benchmark
    public void pingBenchmark() {
        LOGGER.info("(V2) Ping benchmark");
        ClientTests clientTests = new ClientTests();
        clientTests.testPing();
    }

    @Benchmark
    public void queryBenchmarkSmall() throws Exception {
        LOGGER.info("(V2) Query benchmark");
        queryTests.testQueryAllSimple(SMALL_SIZE);
    }

    @Benchmark
    public void queryBenchmarkMedium() throws Exception {
        LOGGER.info("(V2) Query benchmark");
        queryTests.testQueryAllSimple(MEDIUM_SIZE);
    }

    @Benchmark
    public void queryBenchmarkLarge() throws Exception {
        LOGGER.info("(V2) Query benchmark");
        queryTests.testQueryAllSimple(LARGE_SIZE);
    }

    @Threads(4)
    @Benchmark
    public void queryBenchmarkSmallParallelOldImpl() throws Exception {
        LOGGER.info("(V2) Query parallel benchmark");
        GenericRecord record = oldClient.queryAll("SELECT number FROM system.numbers LIMIT " + SMALL_SIZE).stream().findFirst().get();
        Assert.assertNotNull(record);
    }

    @Threads(4)
    @Benchmark
    public void queryBenchmarkMediumParallelOldImpl() throws Exception {
        LOGGER.info("(V2) Query parallel benchmark");
        GenericRecord record = oldClient.queryAll("SELECT number FROM system.numbers LIMIT " + MEDIUM_SIZE).stream().findFirst().get();
        Assert.assertNotNull(record);
    }

    @Threads(4)
    @Benchmark
    public void queryBenchmarkLargeParallelOldImpl() throws Exception {
        LOGGER.info("(V2) Query parallel benchmark");
        GenericRecord record = oldClient.queryAll("SELECT number FROM system.numbers LIMIT " + LARGE_SIZE).stream().findFirst().get();
        Assert.assertNotNull(record);
    }

    @Threads(4)
    @Benchmark
    public void queryBenchmarkSmallParallelNewImpl() throws Exception {
        LOGGER.info("(V2) Query parallel benchmark");
        GenericRecord record = newClient.queryAll("SELECT number FROM system.numbers LIMIT " + SMALL_SIZE).stream().findFirst().get();
        Assert.assertNotNull(record);
    }

    @Threads(4)
    @Benchmark
    public void queryBenchmarkMediumParallelNewImpl() throws Exception {
        LOGGER.info("(V2) Query parallel benchmark");
        GenericRecord record = newClient.queryAll("SELECT number FROM system.numbers LIMIT " + MEDIUM_SIZE).stream().findFirst().get();
        Assert.assertNotNull(record);
    }

    @Threads(4)
    @Benchmark
    public void queryBenchmarkLargeParallelNewImpl() throws Exception {
        LOGGER.info("(V2) Query parallel benchmark");
        GenericRecord record = newClient.queryAll("SELECT number FROM system.numbers LIMIT " + LARGE_SIZE).stream().findFirst().get();
        Assert.assertNotNull(record);
    }

    @Benchmark
    public void insertRawDataBenchmarkSmall() throws Exception {
        LOGGER.info("(V2) Insert raw data benchmark");
        insertTests.insertRawDataSimple(SMALL_SIZE);
    }

    @Benchmark
    public void insertRawDataBenchmarkMedium() throws Exception {
        LOGGER.info("(V2) Insert raw data benchmark");
        insertTests.insertRawDataSimple(MEDIUM_SIZE);
    }

    @Benchmark
    public void insertRawDataBenchmarkLarge() throws Exception {
        LOGGER.info("(V2) Insert raw data benchmark");
        insertTests.insertRawDataSimple(LARGE_SIZE);
    }

    @Threads(4)
    @Benchmark
    public void insertRawDataBenchmarkSmallParallel() throws Exception {
        LOGGER.info("(V2) Insert raw data parallel benchmark");
        insertTests.insertRawDataSimple(SMALL_SIZE);
    }

    @Threads(4)
    @Benchmark
    public void insertRawDataBenchmarkMediumParallel() throws Exception {
        LOGGER.info("(V2) Insert raw data parallel benchmark");
        insertTests.insertRawDataSimple(MEDIUM_SIZE);
    }

    @Threads(4)
    @Benchmark
    public void insertRawDataBenchmarkLargeParallel() throws Exception {
        LOGGER.info("(V2) Insert raw data parallel benchmark");
        insertTests.insertRawDataSimple(LARGE_SIZE);
    }

    @Benchmark
    public void insertSimplePojoBenchmark() throws Exception {
        LOGGER.info("(V2) Insert simple pojo benchmark");
        insertTests.insertSimplePOJOs();
    }
}
