package com.clickhouse.benchmark.data;

import com.clickhouse.benchmark.clients.BenchmarkBase;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.clickhouse.benchmark.clients.BenchmarkBase.insertData;
import static com.clickhouse.benchmark.clients.BenchmarkBase.loadClickHouseRecords;
import static com.clickhouse.benchmark.clients.BenchmarkBase.runQuery;
import static com.clickhouse.benchmark.clients.BenchmarkBase.syncQuery;

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

    /**
     * A simple way to parse a CREATE TABLE statement into a TableSchema. Each column is expected to be on a new line (i.e. 'ColumnName ColumnType,\n').
     * @param createTableStatement - a CREATE TABLE statement
     * @return TableSchema
     */
    public static TableSchema parseSchema(String createTableStatement) {//TODO: Consider replacing this, as it's not very robust.
        TableSchema schema = new TableSchema();
        if (createTableStatement == null || createTableStatement.isEmpty()) {
            return schema;
        }

        try {
            createTableStatement = createTableStatement.substring(createTableStatement.indexOf("(") + 1, createTableStatement.indexOf(") Engine")).trim();
            BufferedReader br = new BufferedReader(new StringReader(createTableStatement));
            br.lines().forEach(line -> {
                line = line.trim();
                String name = line.substring(0, line.indexOf(" "));
                String type = line.substring(line.indexOf(" ") + 1);
                if (type.endsWith(","))//Removing trailing comma
                    type = type.substring(0, type.length() - 1);
                schema.addColumn(name, type);
            });
        } catch (Exception e) {
            LOGGER.error("Error parsing schema", e);
            return new TableSchema();
        }

        return schema;
    }

    public static void initializeTables(DataSet set, boolean insertData) {
        runQuery(set.getCreateTableString(), true);
        ClickHouseFormat format = set.getFormat();

        insertData(set.getTableName(), set.getInputStream(format), format);
        if (!insertData) {
            loadClickHouseRecords(set);
            runQuery("TRUNCATE TABLE " + set.getTableName(), true);
            syncQuery(set);
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
            default:
                throw new IllegalArgumentException("Unsupported format: " + format);
        }
    }
}
