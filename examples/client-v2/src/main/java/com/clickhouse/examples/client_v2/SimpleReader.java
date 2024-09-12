package com.clickhouse.examples.client_v2;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
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

    public SimpleReader(String endpoint, String user, String password, String database) {
        // Create a lightweight object to interact with ClickHouse server
        Client.Builder clientBuilder = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(password)
                .compressServerResponse(true)
                .setDefaultDatabase(database);

        this.client = clientBuilder.build();
    }

    public boolean isServerAlive() {
        return client.ping();
    }

    public void readDataUsingBinaryFormat() {
        log.info("Reading data from table: {}", TABLE_NAME);
        final String sql = "select * from " + TABLE_NAME + " where title <> '' limit 10";

        // Default format is RowBinaryWithNamesAndTypesFormatReader so reader have all information about columns
        try (QueryResponse response = client.query(sql).get(3, TimeUnit.SECONDS);) {

            // Create a reader to access the data in a convenient way
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);

            while (reader.hasNext()) {
                reader.next(); // Read the next record from stream and parse it

                // get values
                double id = reader.getDouble("id");
                String title = reader.getString("title");
                String url = reader.getString("url");

                log.info("id: {}, title: {}, url: {}", id, title, url);
            }

            log.info("Data read successfully: {} ms", response.getMetrics().getMetric(ClientMetrics.OP_DURATION).getLong());
        } catch (Exception e) {
            log.error("Failed to read data", e);
        }
        // Response object must be closed to release resources
    }

    public void readDataAll() {
        try {
            log.info("Reading whole table and process record by record");
            final String sql = "select * from " + TABLE_NAME + " where title <> ''";

            // Read whole result set and process it record by record
            client.queryAll(sql).forEach(row -> {
                double id = row.getDouble("id");
                String title = row.getString("title");
                String url = row.getString("url");

                log.info("id: {}, title: {}, url: {}", id, title, url);
            });
        } catch (Exception e) {
            log.error("Failed to read data", e);
        }
    }

    public void readData() {
        log.info("Reading data from table: {} using Records iterator", TABLE_NAME);
        final String sql = "select * from " + TABLE_NAME + " where title <> '' limit 10";
        try (Records records = client.queryRecords(sql).get(3, TimeUnit.SECONDS);) {

            // Get some metrics
            log.info("Data read successfully: {} ms", TimeUnit.NANOSECONDS.toMillis(records.getServerTime()));
            log.info("Total rows: {}", records.getResultRows());

            // Iterate thru records
            for (GenericRecord record : records) {
                double id = record.getDouble("id");
                String title = record.getString("title");
                String url = record.getString("url");

                log.info("id: {}, title: {}, url: {}", id, title, url);
            }
        } catch (Exception e) {
            log.error("Failed to read data", e);
        }
    }
}
