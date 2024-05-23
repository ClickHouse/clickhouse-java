package com.clickhouse.examples.client_v2;

import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;

@Slf4j
public class Main {

    public static void main(String[] args) {
        final String endpoint = System.getProperty("chEndpoint", "http://localhost:8123");
        final String user = System.getProperty("chUser", "default");
        final String password = System.getProperty("chPassword", null);
        final String database = System.getProperty("chDatabase", "default");

        SimpleWriter writer = new SimpleWriter(endpoint, user, password, database);

        if (writer.isServerAlive()) {
            log.info("ClickHouse server is alive");
        } else {
            log.error("ClickHouse server is not alive");
            Runtime.getRuntime().exit(-503);
        }

        writer.resetTable();


        log.info("Inserting data from resources/sample_hacker_news_posts.json");
        try (InputStream is = Main.class.getResourceAsStream("/sample_hacker_news_posts.json")) {
            writer.insertData_JSONEachRowFormat(is);
        } catch (Exception e) {
            log.error("Failed to insert data", e);
        }

        SimpleReader reader = new SimpleReader(endpoint, user, password, database);

        reader.readData();

        log.info("Done");

        Runtime.getRuntime().exit(0);
    }
}
