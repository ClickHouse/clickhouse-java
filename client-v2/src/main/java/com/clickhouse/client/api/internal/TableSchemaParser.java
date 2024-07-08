package com.clickhouse.client.api.internal;

import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.metadata.TableSchema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Properties;

public class TableSchemaParser {

    public TableSchemaParser() {
    }

    public TableSchema createFromBinaryResponse(ClickHouseResponse response, String tableName, String databaseName) {
        TableSchema schema = new TableSchema();
        schema.setTableName(tableName);
        schema.setDatabaseName(databaseName);
        Properties p = new Properties();
        response.records().forEach(record -> {
            String values = record.getValue(0).asString().replaceAll("\t", "\n");
            try {
                p.clear();
                p.load(new StringReader(values));
                schema.addColumn(p.getProperty("name"), p.getProperty("type"), p.getProperty("default_type"));
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse table schema", e);
            }
        });
        return schema;
    }

    public TableSchema readTSKV(InputStream content, String table, String database) {
        TableSchema schema = new TableSchema();
        schema.setTableName(table);
        schema.setDatabaseName(database);
        Properties p = new Properties();
        try (BufferedReader r = new BufferedReader(new java.io.InputStreamReader(content))) {
            String line;
            while ((line = r.readLine()) != null) {
                p.clear();
                p.load(new StringReader(line.replaceAll("\t", "\n")));
                schema.addColumn(p.getProperty("name"), p.getProperty("type"), p.getProperty("default_type"));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse table schema", e);
        }
        return schema;
    }
}
