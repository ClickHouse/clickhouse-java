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

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.clickhouse.benchmark.TestEnvironment.isCloud;


public class BenchmarkRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkRunner.class);

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting Benchmarks");
        Map<String, String> options = parseArgs(args);
        System.out.println("Start Benchmarks with options: " + options);
        final String env = isCloud() ? "cloud" : "local";
        final long time = System.currentTimeMillis();

        final int measurementIterations = Integer.parseInt(options.getOrDefault("-m", "10"));
        final int measurementTime = Integer.parseInt(options.getOrDefault("-t", "" + (isCloud() ? 30 : 10)));
        final String resultFile = String.format("jmh-results-%s-%s.json", env, time);
        final String outputFile = String.format("jmh-results-%s-%s.out", env, time);

        System.out.println("Measurement iterations: " + measurementIterations);
        System.out.println("Measurement time: " + measurementTime + "s");
        System.out.println("Env: " + env);
        System.out.println("Epoch Time: " + time);

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
                .output(outputFile)
                .result(resultFile);

        String testMask = options.getOrDefault("-b", "q,i");
        String[] testMaskParts = testMask.split(",");

        SortedSet<String> benchmarks = new TreeSet<>();
        for (String p : testMaskParts) {
            String benchmark = BENCHMARK_FLAGS.get(p);
            if (benchmark != null) {
                optBuilder.include(benchmark);
                benchmarks.add(benchmark);
            }
        }

        System.out.println("Running benchmarks: " + benchmarks);
        new Runner(optBuilder.build()).run();
    }

    private static final Map<String, String> BENCHMARK_FLAGS = buildBenchmarkFlags();

    private static Map<String, String> buildBenchmarkFlags() {
        HashMap<String, String> map = new HashMap<>();
        map.put("q", QueryClient.class.getName());
        map.put("i", InsertClient.class.getName());
        map.put("cq", ConcurrentQueryClient.class.getName());
        map.put("ci", ConcurrentInsertClient.class.getName());
        map.put("lz", Compression.class.getName());
        map.put("reader", Deserializers.class.getName());
        map.put("writer", Serializers.class.getName());
        map.put("mixed", MixedWorkload.class.getName());
        return map;
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i+=2) {
            options.put(args[i], args[i+1]);
        }
        return options;
    }
}
