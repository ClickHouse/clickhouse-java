package com.clickhouse.benchmark;

import com.clickhouse.benchmark.clients.InsertClient;
import com.clickhouse.benchmark.clients.QueryClient;
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.clickhouse.benchmark.TestEnvironment.DB_NAME;
import static com.clickhouse.benchmark.TestEnvironment.isCloud;


public class BenchmarkRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkRunner.class);
    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting Benchmarks");
        Map<String, String> argMap = parseArguments(args);

        Options opt = new OptionsBuilder()
//                .param("datasetSourceName", argMap.getOrDefault("dataset", "simple"))
                .include(QueryClient.class.getSimpleName())
                .include(InsertClient.class.getSimpleName())
                .forks(1) // must be a fork. No fork only for debugging
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .threads(1)
                .addProfiler(GCProfiler.class)
                .addProfiler(MemPoolProfiler.class)
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(10))
                .measurementIterations(10)
                .jvmArgs("-Xms8g", "-Xmx8g")
                .measurementTime(TimeValue.seconds(10))
                .resultFormat(ResultFormatType.JSON)
                .result(String.format("jmh-results-%s-%s.json", isCloud() ? "cloud" : "local", System.currentTimeMillis()))
                .build();

        new Runner(opt).run();
    }

    public static Map<String, String> parseArguments(String[] args) {
        Map<String, String> argMap = new HashMap<>();
        for (String arg : args) {
            if (arg.startsWith("-")) {
                String[] parts = arg.substring(2).split("=", 2);
                if (parts.length == 2) {
                    String key = parts[0];
                    String value = parts[1];
                    if (key.equals("dataset") || key.equals("iterations")) {
                        argMap.put(key, value);
                    }
                }
            }
        }
        return argMap;
    }

    public static String getSelectQuery(String tableName) {
        return "SELECT * FROM `" + DB_NAME + "`.`" + tableName + "`";
    }

    public static String getSelectCountQuery(String tableName) {
        return String.format("SELECT COUNT(*) FROM `%s`.`%s`", DB_NAME, tableName);
    }

    public static String getInsertQuery(String tableName) {
        return String.format("INSERT INTO `%s`.`%s`", DB_NAME, tableName);
    }

    public static String getSyncQuery(String tableName) {
        return String.format("SYSTEM SYNC REPLICA `%s`.`%s`", DB_NAME, tableName);
    }
}
