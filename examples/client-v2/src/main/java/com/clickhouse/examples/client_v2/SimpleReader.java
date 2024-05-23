package com.clickhouse.examples.client_v2;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SimpleReader {

    private static final String TABLE_NAME = "hacker_news_articles";

    Client client;

    String database;

    public SimpleReader(String endpoint, String user, String password, String database) {
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

    public void readData() {
        try {
            // Read data from the table
            log.info("Reading data from table: {}", TABLE_NAME);
            QuerySettings settings = new QuerySettings();
            settings.setDatabase(database);
            settings.setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes.name());
            settings.setCompress(false);
            final String sql = "select * from " + TABLE_NAME + " where title <> '' limit 10";
            QueryResponse response = client.query(sql, Collections.emptyMap(), settings)
                            .get(10, TimeUnit.SECONDS);
            response.ensureDone();

            ClickHouseBinaryFormatReader reader = new RowBinaryWithNamesAndTypesFormatReader(response.getInputStream(),
                    settings);

            while (reader.next()) {
                double id = reader.getDouble("id");
                String title = reader.getString("title");
                String url = reader.getString("url");

                log.info("id: {}, title: {}, url: {}", id, title, url);
            }

            log.info("Data read successfully: {}", response.getOperationStatistics());

        } catch (Exception e) {
            log.error("Failed to read data", e);
        }
    }
}
