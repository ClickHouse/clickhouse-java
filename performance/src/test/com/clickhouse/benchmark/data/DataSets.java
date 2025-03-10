package com.clickhouse.benchmark.data;

import com.clickhouse.data.ClickHouseFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class DataSets {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSets.class);
    private static final Map<String, DataSet> sets;
    static {
        sets = Map.of(
                "simple", new SimpleDataSet()
        );
    }

    public static DataSet from(String name) {
        return sets.get(name);
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
            default:
                throw new IllegalArgumentException("Unsupported format: " + format);
        }
    }
}
