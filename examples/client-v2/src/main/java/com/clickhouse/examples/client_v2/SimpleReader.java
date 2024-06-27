package com.clickhouse.examples.client_v2;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * <p>Example class showing how to read data from ClickHouse server using the new ClickHouse client.</p>
 * <p>Method {@link Client#query(String, QuerySettings)} returns a {@link Future} for {@link QueryResponse}. The last
 * one is an object providing access to query results and actual data returned by server. The data is accessible thru
 * an InputStream what makes it possible to be used with any data reader. Library provides most essential readers.
 * If response is in well known format like CSV than it is possible to use any 3rd-party library that can read from a stream.
 * </p>
 *
 * <p>Readers may be found in the package {@code com.clickhouse.client.api.data_formats}. Here are readers for RowBinary formats:</p>
 * <ul>
 *     <li>{@link com.clickhouse.client.api.data_formats.RowBinaryFormatReader}</li>
 *     <li>{@link com.clickhouse.client.api.data_formats.RowBinaryWithNamesFormatReader}</li>
 *     <li>{@link com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader}</li>
 * </ul>
 */
@Slf4j
public class SimpleReader {

    private static final String TABLE_NAME = "hacker_news_articles";

    Client client;

    String database;

    public SimpleReader(String endpoint, String user, String password, String database) {
        // Create a lightweight object to interact with ClickHouse server
        Client.Builder clientBuilder = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(password)
                .compressServerResponse(true)
                .setDefaultDatabase(database);

        this.client = clientBuilder.build();
        this.database = database;
    }

    public boolean isServerAlive() {
        return client.ping();
    }

    public void readData() {
        try {
            // Read data from the table
            log.info("Reading data from table: {}", TABLE_NAME);

            final String sql = "select * from " + TABLE_NAME + " where title <> '' limit 10";
            // Default format is RowBinaryWithNamesAndTypesFormatReader so reader have all information about columns
            QueryResponse response = client.query(sql).get(3, TimeUnit.SECONDS);
            // Create a reader to access the data in a convenient way
            ClickHouseBinaryFormatReader reader = new RowBinaryWithNamesAndTypesFormatReader(response.getInputStream());

            while (reader.hasNext()) {
                reader.next();
                double id = reader.getDouble("id");
                String title = reader.getString("title");
                String url = reader.getString("url");

                log.info("id: {}, title: {}, url: {}", id, title, url);
            }

            log.info("Data read successfully: {} ms", response.getMetrics().getMetric(ClientMetrics.OP_DURATION).getLong());
        } catch (Exception e) {
            log.error("Failed to read data", e);
        }
    }

}
