package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TableSchemaParser {

    public static TableSchema readTSKV(InputStream content, String table, String sqlQuery, String database) {
        Properties p = new Properties();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(content))) {
            String line;
            List<ClickHouseColumn> columns = new ArrayList<>();
            while ((line = r.readLine()) != null) {
                p.clear();
                if (!line.trim().isEmpty()) {
                    p.load(new StringReader(line.replaceAll("\t", "\n")));
                    ClickHouseColumn column = ClickHouseColumn.of(p.getProperty("name"), p.getProperty("type"));
                    String defaultType = p.getProperty("default_type");
                    column.setHasDefault(defaultType != null && !defaultType.isEmpty());
                    columns.add(column);
                }
            }

            return new TableSchema(table, sqlQuery, database, columns);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse table schema", e);
        }
    }
}
