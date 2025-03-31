package com.clickhouse.benchmark;

import com.clickhouse.benchmark.clients.*;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.MemPoolProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.clickhouse.benchmark.TestEnvironment.isCloud;


public class BenchmarkRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkRunner.class);

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting Benchmarks");
        int measurementIterations = Integer.getInteger("m", 10);
        int measurementTime = Integer.getInteger("t", isCloud() ? 30 : 10);

        ChainedOptionsBuilder optBuilder = new OptionsBuilder()
                .forks(1) // must be a fork. No fork only for debugging
                .mode(Mode.SampleTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .addProfiler(GCProfiler.class)
                .addProfiler(MemPoolProfiler.class)
                .warmupIterations(1)
                .warmupTime(TimeValue.seconds(5))
                .measurementIterations(measurementIterations)
                .jvmArgs("-Xms8g", "-Xmx8g")
                .measurementTime(TimeValue.seconds(measurementTime))
                .resultFormat(ResultFormatType.JSON)
//                .output(String.format("jmh-results-%s-%s.out", isCloud() ? "cloud" : "local", System.currentTimeMillis()))
                .result(String.format("jmh-results-%s-%s.json", isCloud() ? "cloud" : "local", System.currentTimeMillis()));

        String testMask = System.getProperty("b", "q,i");
        String[] testMaskParts = testMask.split(",");

//                .include(QueryClient.class.getSimpleName())
//                .include(InsertClient.class.getSimpleName())
//                .include(ConcurrentInsertClient.class.getSimpleName())
//                .include(ConcurrentQueryClient.class.getSimpleName())
//                .include(Compression.class.getSimpleName())
//                .include(Serializers.class.getSimpleName())
//                .include(Deserializers.class.getSimpleName())
//                .include(MixedWorkload.class.getSimpleName())


        new Runner(optBuilder.build()).run();
    }

    private enum Benchmarks {

        QUERY_CLIENT(QueryClient.class, "q");

        private Class<QueryClient> queryClientClass;
        private String mask;

        Benchmarks(Class<QueryClient> queryClientClass, String mask) {
            this.queryClientClass = queryClientClass;
            this.mask = mask;
        }

        String benchmarkName() {
            return queryClientClass.getName();
        }

        String mask() {
            return mask;
        }
    }
}
