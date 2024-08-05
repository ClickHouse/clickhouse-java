package com.clickhouse.examples.client_v2;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.data.ClickHouseFormat;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p>Example class showing how to pass raw data stream to the new ClickHouse client.</p>
 * <p>Input data is passed as a InputStream to the {@link com.clickhouse.client.api.Client#insert(String, InputStream, ClickHouseFormat, InsertSettings)}
 * and the format is specified there too.</p>
 */
@Slf4j
public class Stream2DbWriter {

    private static final String TABLE_NAME = "hacker_news_articles";

    Client client;

    String database;

    public Stream2DbWriter(String endpoint, String user, String password, String database) {
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

    // Initializes a table for the specific dataset
    public void resetTable() {
        try (InputStream initSql = Stream2DbWriter.class.getResourceAsStream("/simple_writer_init.sql")) {
            // Sending a simple query - no settings required
            client.query("drop table if exists hacker_news_articles").get(3, TimeUnit.SECONDS);

            // Reading the SQL file and executing it
            BufferedReader reader = new BufferedReader(new InputStreamReader(initSql));
            String sql = reader.lines().collect(Collectors.joining("\n"));
            log.debug("Executing Create Table: {}", sql);
            client.query(sql).get(3, TimeUnit.SECONDS);
            log.info("Table initialized");
        } catch (Exception e) {
            log.error("Failed to initialize table", e);
        }
    }

    /**
     * <p>Accepts an input stream of JSONEachRow formatted data and inserts it into the table</p>
     * <p>For more information on JSONEachRow format, see:
     * {@see <a href="https://clickhouse.com/docs/en/interfaces/formats#jsoneachrow">Format Documentation</a>}</p>
     *
     * <p>Any other format can be used instead of JSONEachRow, such as TabSeparated, RowBinary, etc. As data stream
     * is passed almost directly to a transport layer.</p>
     *
     * @param inputStream - input stream of JSONEachRow formatted data
     */
    public void insertData_JSONEachRowFormat(InputStream inputStream) {
        InsertSettings insertSettings = new InsertSettings();
        try (InsertResponse response = client.insert(TABLE_NAME, inputStream, ClickHouseFormat.JSONEachRow,
                insertSettings).get(3, TimeUnit.SECONDS)) {

            log.info("Insert finished: {} rows written", response.getMetrics().getMetric(ServerMetrics.NUM_ROWS_WRITTEN).getLong());
        } catch (Exception e) {
            log.error("Failed to write JSONEachRow data", e);
            throw new RuntimeException(e);
        }
    }
}
