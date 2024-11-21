package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.metadata.TableSchema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Properties;

public class TableSchemaParser {

    public static TableSchema readTSKV(InputStream content, String table, String sqlQuery, String database) {
        TableSchema schema = new TableSchema();
        schema.setTableName(table);
        schema.setQuery(sqlQuery);
        schema.setDatabaseName(database);
        Properties p = new Properties();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(content))) {
            String line;
            while ((line = r.readLine()) != null) {
                p.clear();
                int lineLength = line.length();
                if (!line.trim().isEmpty()) {
                    p.load(new StringReader(line.replaceAll("\t", "\n")));
                    schema.addColumn(p.getProperty("name"), p.getProperty("type"), p.getProperty("default_type"));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse table schema", e);
        }
        return schema;
    }
}
