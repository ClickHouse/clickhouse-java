package com.clickhouse.client;

import com.clickhouse.client.insert.InsertTests;
import com.clickhouse.client.query.QueryTests;
import org.openjdk.jmh.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
public class ClientBenchmark {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientBenchmark.class);

    @Setup
    public void setup() {
        BaseIntegrationTest.setupClickHouseContainer();
    }

    @TearDown
    public void tearDown() {
        BaseIntegrationTest.teardownClickHouseContainer();
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    // Add benchmark methods here
    @Benchmark
    public void pingBenchmark() {
        LOGGER.info("Ping benchmark");
        ClientTests clientTests = new ClientTests();
        clientTests.testPing();
    }

    @Benchmark
    public void queryBenchmarkSmall() throws Exception {
        LOGGER.info("Query benchmark");
        QueryTests queryTests = new QueryTests(false, false);
        queryTests.setUp();
        queryTests.testQueryAll(1000);
        queryTests.tearDown();
    }

    @Benchmark
    public void queryBenchmarkMedium() throws Exception {
        LOGGER.info("Query benchmark");
        QueryTests queryTests = new QueryTests(false, false);
        queryTests.setUp();
        queryTests.testQueryAll(1000000);
        queryTests.tearDown();
    }

    @Benchmark
    public void queryBenchmarkLarge() throws Exception {
        LOGGER.info("Query benchmark");
        QueryTests queryTests = new QueryTests(false, false);
        queryTests.setUp();
        queryTests.testQueryAll(1000000000);
        queryTests.tearDown();
    }

    @Benchmark
    public void insertRawDataBenchmark() throws Exception {
        LOGGER.info("Insert raw data benchmark");
        InsertTests insertTests = new InsertTests(false, false);
        insertTests.setUp();
        insertTests.insertRawData();
        insertTests.insertSimplePOJOs();
        insertTests.tearDown();
    }

    @Benchmark
    public void insertSimplePojoBenchmark() throws Exception {
        LOGGER.info("Insert simple pojo benchmark");
        InsertTests insertTests = new InsertTests(false, false);
        insertTests.setUp();
        insertTests.insertSimplePOJOs();
        insertTests.tearDown();
    }
}
