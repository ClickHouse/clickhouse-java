package com.clickhouse.benchmark;

import com.clickhouse.benchmark.clients.ClientV1;
import com.clickhouse.benchmark.clients.ClientV2;
import com.clickhouse.benchmark.clients.JdbcV1;
import com.clickhouse.benchmark.clients.JdbcV2;
import com.clickhouse.benchmark.data.DataSet;
import com.clickhouse.benchmark.data.DataSets;
import com.clickhouse.benchmark.data.SimpleDataSet;
import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseFormat;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.MemPoolProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;



public class BenchmarkRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkRunner.class);
    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting Benchmarks");
        Options opt = new OptionsBuilder()
                .include(ClientV1.class.getSimpleName())
                .include(ClientV2.class.getSimpleName())
                .include(JdbcV1.class.getSimpleName())
                .include(JdbcV2.class.getSimpleName())
                .forks(1)
                .mode(Mode.SampleTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .threads(1)
                .addProfiler(GCProfiler.class)
                .addProfiler(MemPoolProfiler.class)
                .warmupIterations(1)
                .warmupTime(TimeValue.seconds(3))
                .measurementIterations(1)
                .measurementTime(TimeValue.seconds(5))
                .resultFormat(ResultFormatType.JSON)
                .result("jmh-simple-results.json")
                .build();

        new Runner(opt).run();
    }

}
