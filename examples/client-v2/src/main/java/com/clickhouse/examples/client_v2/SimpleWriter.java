package com.clickhouse.examples.client_v2;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.exception.ClientException;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class SimpleWriter {

    private static final String TABLE_NAME = "hacker_news_articles";

    Client client;

    String database;

    public SimpleWriter(String endpoint, String user, String password, String database) {
        // Create a lightweight object to interact with ClickHouse server
        this.client = new Client.Builder()
                .addEndpoint(endpoint)
                .addUsername(user).addPassword(password)
                .enableCompression(false)
                .enableDecompression(false)
                .build();
        this.database = database;
    }

    public boolean isServerAlive() {
        return client.ping();
    }

    // Initializes a table for the specific dataset
    public void resetTable() {
        try (InputStream initSql = SimpleWriter.class.getResourceAsStream("/simple_writer_init.sql")) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(initSql));
            String sql = reader.lines().collect(Collectors.joining("\n"));
            log.debug("Executing Create Table: {}", sql);
            QuerySettings settings = new QuerySettings();
            settings.setDatabase(database);
            settings.setFormat(ClickHouseFormat.TabSeparated.name());
            settings.setCompress(false);
            client.query("drop table if exists hacker_news_articles", Collections.emptyMap(), settings).get(10, TimeUnit.SECONDS);
            QueryResponse response = client.query(sql, Collections.emptyMap(), settings).get(10, TimeUnit.SECONDS);
            response.ensureDone();
            log.info("Table initialized");
        } catch (Exception e) {
            log.error("Failed to initialize table", e);
        }
    }

    public void insertData_JSONEachRowFormat(InputStream inputStream) {
        InsertSettings insertSettings = new InsertSettings();
        insertSettings.setFormat(ClickHouseFormat.JSONEachRow);

        try {
            InsertResponse response = client.insert(TABLE_NAME, inputStream, insertSettings);
            log.info("Insert finished: {}", response.getOperationStatistics());
        } catch (Exception | ClientException e) {
            log.error("Failed to write JSONEachRow data", e);
            throw new RuntimeException(e);
        }
    }
}
