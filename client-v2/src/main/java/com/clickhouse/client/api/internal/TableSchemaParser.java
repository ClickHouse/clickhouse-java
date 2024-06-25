package com.clickhouse.client.api.internal;

import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.metadata.TableSchema;

import java.io.IOException;
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
            } catch ( IOException e) {
                throw new RuntimeException("Failed to parse table schema", e);
            }
        });
        return schema;
    }

    public TableSchema createFromBytes(byte[] bytes, String tableName, String databaseName) {

        TableSchema schema = new TableSchema();
        schema.setTableName(tableName);
        schema.setDatabaseName(databaseName);
        Properties p = new Properties();
//        response.records().forEach(record -> {
//            String values = record.getValue(0).asString().replaceAll("\t", "\n");
//            try {
//                p.clear();
//                p.load(new StringReader(values));
//                schema.addColumn(p.getProperty("name"), p.getProperty("type"), p.getProperty("default_type"));
//            } catch ( IOException e) {
//                throw new RuntimeException("Failed to parse table schema", e);
//            }
//        });
        return schema;

    }
}
