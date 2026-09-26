package com.clickhouse.benchmark;

import com.clickhouse.benchmark.clients.Compression;
import com.clickhouse.benchmark.clients.ConcurrentInsertClient;
import com.clickhouse.benchmark.clients.ConcurrentQueryClient;
import com.clickhouse.benchmark.clients.Deserializers;
import com.clickhouse.benchmark.clients.InsertClient;
import com.clickhouse.benchmark.clients.JDBCInsert;
import com.clickhouse.benchmark.clients.JDBCQuery;
import com.clickhouse.benchmark.clients.MixedWorkload;
import com.clickhouse.benchmark.clients.QueryClient;
import com.clickhouse.benchmark.clients.Serializers;
import org.openjdk.jmh.annotations.Benchmark;
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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.clickhouse.benchmark.TestEnvironment.isRemote;


public class BenchmarkRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkRunner.class);

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting Benchmarks");
        Map<String, String> options = parseArgs(args);
        System.out.println("Start Benchmarks with options: " + options);
        final String env = isRemote() ? "remote" : "local";
        final long time = System.currentTimeMillis();

        final int measurementIterations = Integer.parseInt(options.getOrDefault("-m", "10"));
        final int measurementTime = Integer.parseInt(options.getOrDefault("-t", "" + (isRemote() ? 30 : 10)));
        final String resultFile = String.format("jmh-results-%s-%s.json", env, time);
        final String outputFile = String.format("jmh-results-%s-%s.out", env, time);
        final String datasetName = options.getOrDefault("-d", "file://default.csv");
        final String[] limits = options.getOrDefault("-l", "300000,100000,10000").split(",");

        System.out.println("Measurement iterations: " + measurementIterations);
        System.out.println("Measurement time: " + measurementTime + "s");
        System.out.println("Env: " + env);
        System.out.println("Dataset: " + datasetName);
        System.out.println("Limits: " + Arrays.asList(limits));
        System.out.println("Epoch Time: " + time);

        ChainedOptionsBuilder optBuilder = new OptionsBuilder()
                .forks(1) // must be a fork. No fork only for debugging
                .mode(Mode.SampleTime)
                .param("datasetSourceName", datasetName)
                .param("limit", limits)
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
                .result(resultFile)
                .shouldFailOnError(true);

        String testMask = options.getOrDefault("-b", "q,i");
        String[] testMaskParts = testMask.split(",");

        SortedSet<String> benchmarks = new TreeSet<>();
        boolean runAll = testMaskParts[0].equalsIgnoreCase("all");
        if (runAll) {
            BENCHMARK_FLAGS.values().forEach((b) -> {
                optBuilder.include(b);
                benchmarks.add(b);
            });
        } else {
            for (String p : testMaskParts) {
                String benchmark = BENCHMARK_FLAGS.get(p);
                if (benchmark != null) {
                    optBuilder.include(benchmark);
                    benchmarks.add(benchmark);
                }
            }
        }

        if (runAll || Arrays.asList(testMaskParts).contains(COMPRESSION_MATRIX_FLAG)) {
            List<String> matrix = compressionMatrix(options);
            if (matrix.isEmpty()) {
                System.out.println("No compression benchmark matches the selected clients/methods/algorithms");
            }
            for (String benchmark : matrix) {
                optBuilder.include(Pattern.quote(benchmark) + "$");
                benchmarks.add(benchmark);
            }
            if (options.containsKey("-cf")) {
                optBuilder.param("format", options.get("-cf").split(","));
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
        map.put("lz", Compression.class.getName() + ".CompressingOutputStream");
        map.put("reader", Deserializers.class.getName());
        map.put("writer", Serializers.class.getName());
        map.put("mixed", MixedWorkload.class.getName());
        map.put("jq", JDBCQuery.class.getName());
        map.put("ji", JDBCInsert.class.getName());
        return map;
    }

    private static final String COMPRESSION_MATRIX_FLAG = "comp";

    private static final Pattern COMPRESSION_BENCHMARK_NAME =
            Pattern.compile("(query|insert)(V1|V2)(Native|Http)(Lz4|Zstd|Snappy|Brotli)");

    /**
     * Selects {@link Compression} matrix benchmarks by client ({@code -cc v1,v2}), compression method
     * ({@code -cm http,native}) and algorithm ({@code -ca lz4,zstd,snappy,brotli}). Each filter defaults to all
     * values. Combinations a client does not support have no benchmark method and are skipped.
     */
    private static List<String> compressionMatrix(Map<String, String> options) {
        Set<String> clients = filterValues(options, "-cc", "v1,v2");
        Set<String> methods = filterValues(options, "-cm", "http,native");
        Set<String> algorithms = filterValues(options, "-ca", "lz4,zstd,snappy,brotli");

        List<String> selected = new ArrayList<>();
        for (Method method : Compression.class.getMethods()) {
            Matcher m = COMPRESSION_BENCHMARK_NAME.matcher(method.getName());
            if (method.isAnnotationPresent(Benchmark.class) && m.matches()
                    && clients.contains(m.group(2).toLowerCase())
                    && methods.contains(m.group(3).toLowerCase())
                    && algorithms.contains(m.group(4).toLowerCase())) {
                selected.add(Compression.class.getName() + "." + method.getName());
            }
        }
        Collections.sort(selected);
        return selected;
    }

    private static Set<String> filterValues(Map<String, String> options, String key, String defaultValues) {
        Set<String> values = new HashSet<>();
        for (String value : options.getOrDefault(key, defaultValues).split(",")) {
            values.add(value.trim().toLowerCase());
        }
        return values;
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i+=2) {
            options.put(args[i], args[i+1]);
        }
        return options;
    }
}
