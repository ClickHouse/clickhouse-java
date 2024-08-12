package com.clickhouse.client;

import com.clickhouse.client.http.ApacheHttpConnectionImplTest;
import com.clickhouse.client.http.ClickHouseHttpClientTest;
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
        LOGGER.info("(V1) Ping benchmark");
        ApacheHttpConnectionImplTest apacheHttpConnectionImplTest = new ApacheHttpConnectionImplTest();
        apacheHttpConnectionImplTest.testPing();
    }

    @Benchmark
    public void queryBenchmarkSmall() throws Exception {
        LOGGER.info("(V1) Query benchmark");
        ApacheHttpConnectionImplTest apacheHttpConnectionImplTest = new ApacheHttpConnectionImplTest();
        apacheHttpConnectionImplTest.testQuery(1000);
    }

    @Benchmark
    public void queryBenchmarkMedium() throws Exception {
        LOGGER.info("(V1) Query benchmark");
        ApacheHttpConnectionImplTest apacheHttpConnectionImplTest = new ApacheHttpConnectionImplTest();
        apacheHttpConnectionImplTest.testQuery(1000000);
    }

    @Benchmark
    public void queryBenchmarkLarge() throws Exception {
        LOGGER.info("(V1) Query benchmark");
        ApacheHttpConnectionImplTest apacheHttpConnectionImplTest = new ApacheHttpConnectionImplTest();
        apacheHttpConnectionImplTest.testQuery(1000000000);
    }

    @Benchmark
    public void insertRawDataBenchmark() throws Exception {
        LOGGER.info("(V1) Insert raw data benchmark");
        ApacheHttpConnectionImplTest apacheHttpConnectionImplTest = new ApacheHttpConnectionImplTest();
        apacheHttpConnectionImplTest.testInsertWithCustomFormat();
    }
}
