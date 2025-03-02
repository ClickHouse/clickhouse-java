package com.clickhouse.jdbc.perf;

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

public class JDBCBenchmarkRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCBenchmarkRunner.class);

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting Benchmarks");
        Options opt = new OptionsBuilder()
                .include(JDBCSelectBenchmark.class.getSimpleName())
                .include(JDBCInsertBenchmark.class.getSimpleName())
                .forks(1)
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .threads(1)
                .addProfiler(GCProfiler.class)
                .addProfiler(MemPoolProfiler.class)
                .warmupIterations(1)
                .warmupTime(TimeValue.seconds(5))
                .measurementIterations(1)
                .measurementTime(TimeValue.seconds(5))
                .resultFormat(ResultFormatType.JSON)
                .result("jmh-jdbc-results.json")
                .build();
        new Runner(opt).run();
    }
}
