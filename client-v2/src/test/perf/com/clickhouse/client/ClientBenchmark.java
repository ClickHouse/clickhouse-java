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
    private static final int SMALL_SIZE = 1000;
    private static final int MEDIUM_SIZE = 1000000;
    private static final int LARGE_SIZE = 10000000;

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
        LOGGER.info("(V2) Ping benchmark");
        ClientTests clientTests = new ClientTests();
        clientTests.testPing();
    }

    @Benchmark
    public void queryBenchmarkSmall() throws Exception {
        LOGGER.info("(V2) Query benchmark");
        QueryTests queryTests = new QueryTests(false, false);
        queryTests.setUp();
        queryTests.testQueryAllSimple(SMALL_SIZE);
        queryTests.tearDown();
    }

    @Benchmark
    public void queryBenchmarkMedium() throws Exception {
        LOGGER.info("(V2) Query benchmark");
        QueryTests queryTests = new QueryTests(false, false);
        queryTests.setUp();
        queryTests.testQueryAllSimple(MEDIUM_SIZE);
        queryTests.tearDown();
    }

    @Benchmark
    public void queryBenchmarkLarge() throws Exception {
        LOGGER.info("(V2) Query benchmark");
        QueryTests queryTests = new QueryTests(false, false);
        queryTests.setUp();
        queryTests.testQueryAllSimple(LARGE_SIZE);
        queryTests.tearDown();
    }

    @Benchmark
    public void insertRawDataBenchmarkSmall() throws Exception {
        LOGGER.info("(V2) Insert raw data benchmark");
        InsertTests insertTests = new InsertTests(false, false);
        insertTests.setUp();
        insertTests.insertRawDataSimple(SMALL_SIZE);
        insertTests.tearDown();
    }

    @Benchmark
    public void insertRawDataBenchmarkMedium() throws Exception {
        LOGGER.info("(V2) Insert raw data benchmark");
        InsertTests insertTests = new InsertTests(false, false);
        insertTests.setUp();
        insertTests.insertRawDataSimple(MEDIUM_SIZE);
        insertTests.tearDown();
    }

    @Benchmark
    public void insertRawDataBenchmarkLarge() throws Exception {
        LOGGER.info("(V2) Insert raw data benchmark");
        InsertTests insertTests = new InsertTests(false, false);
        insertTests.setUp();
        insertTests.insertRawDataSimple(LARGE_SIZE);
        insertTests.tearDown();
    }

    @Benchmark
    public void insertSimplePojoBenchmark() throws Exception {
        LOGGER.info("(V2) Insert simple pojo benchmark");
        InsertTests insertTests = new InsertTests(false, false);
        insertTests.setUp();
        insertTests.insertSimplePOJOs();
        insertTests.tearDown();
    }
}
