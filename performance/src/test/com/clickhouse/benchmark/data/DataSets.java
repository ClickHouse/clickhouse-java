package com.clickhouse.benchmark.data;

import com.clickhouse.benchmark.BenchmarkRunner;
import com.clickhouse.benchmark.clients.BenchmarkBase;
import com.clickhouse.data.ClickHouseFormat;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataSets {
    private static final Map<String, DataSet> sets;
    static {
        sets = Map.of(
                "simple", new SimpleDataSet()
        );
    }

    public static DataSet from(String name) {
        return sets.get(name);
    }

    public static void initializeTables(DataSet set, boolean insertData) {
        BenchmarkBase.runQuery(set.getCreateTableString(), true);
        ClickHouseFormat format = set.getFormat();

        BenchmarkBase.insertData(set.getTableName(), set.getInputStream(format), format);
        if (!insertData) {
            BenchmarkBase.loadClickHouseRecords(set.getTableName(), set);
            BenchmarkBase.runQuery("TRUNCATE TABLE " + set.getTableName(), true);
        }
    }

    public static List<byte[]> convert(List<Map<String, Object>> data, ClickHouseFormat format) {
        List<byte[]> bytes = new ArrayList<>(data.size());

        switch (format) {
            case JSONEachRow:
                for (Map<String, Object> row : data) {
                    String json = row.entrySet().stream()
                            .map(entry -> "\"" + entry.getKey() + "\":\"" + entry.getValue() + "\"")
                            .collect(Collectors.joining(", ", "{", "}"));
                    json += "\n";
                    bytes.add(json.getBytes(StandardCharsets.UTF_8));
                }
                return bytes;
//                return data.stream().map(Map::toString).map(String::getBytes).collect(Collectors.toList());
            default:
                throw new IllegalArgumentException("Unsupported format: " + format);
        }
    }
}
