package com.clickhouse.examples.client_v2;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.examples.client_v2.data.ArticleViewEvent;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
public class POJO2DbWriter {

    private static final String TABLE_NAME = "article_view_events";

    private static final int EVENTS_BATCH_SIZE = 10;

    Client client;

    String database;

    ArrayList<ArticleViewEvent> events;

    private AtomicBoolean classRegistered = new AtomicBoolean(false);

    public POJO2DbWriter(String endpoint, String user, String password, String database) {
        // Create a lightweight object to interact with ClickHouse server
        Client.Builder clientBuilder = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(password)
                .compressServerResponse(true)
                .setDefaultDatabase(database);

        this.client = clientBuilder.build();
        this.database = database;
        this.events = new ArrayList<>();

    }

    public boolean isServerAlive() {
        return client.ping();
    }

    public void resetTable() {
        try (InputStream initSql = POJO2DbWriter.class.getResourceAsStream("/article_view_event_init.sql")) {
            // Sending a simple query - no settings required
            client.query("drop table if exists " + TABLE_NAME).get(3, TimeUnit.SECONDS);

            // Reading the SQL file and executing it
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(initSql))) {
                String sql = reader.lines().collect(Collectors.joining("\n"));
                log.debug("Executing Create Table: {}", sql);
                client.query(sql).get(10, TimeUnit.SECONDS);
                log.info("Table initialized. Registering class.");
                client.register(ArticleViewEvent.class, client.getTableSchema(TABLE_NAME));
            }
        } catch (Exception e) {
            log.error("Failed to initialize table", e);
        }
    }

    public void printLastEvents() {
        try (QueryResponse response = client.query("select * from " + TABLE_NAME + " order by viewTime desc limit 10 format CSV")
                .get(10, TimeUnit.SECONDS)) {

            log.info("Last 10 events:");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    log.info(line);
                }
            }
        } catch (Exception e) {
            log.error("Failed to read data", e);
        }
    }

    public synchronized void submit(ArticleViewEvent event) {
        events.add(event);

        if (events.size() >= EVENTS_BATCH_SIZE) {
            flush();
        }
    }

    private void flush() {
        client.insert(TABLE_NAME, events, new InsertSettings());
    }
}
