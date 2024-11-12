package com.clickhouse.examples.client_v2;

import com.clickhouse.examples.client_v2.data.ArticleViewEvent;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
public class Main {

    public static void main(String[] args) {
        final String endpoint = System.getProperty("chEndpoint", "http://localhost:8123");
        final String user = System.getProperty("chUser", "default");
        final String password = System.getProperty("chPassword", "");
        final String database = System.getProperty("chDatabase", "default");

        //  Stream data from resources/sample_hacker_news_posts.json to ClickHouse
        Stream2DbWriter writer = new Stream2DbWriter(endpoint, user, password, database);

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

        // Read data back
        SimpleReader reader = new SimpleReader(endpoint, user, password, database);
        reader.readDataUsingBinaryFormat();
        reader.readDataAll();
        reader.readData();

        // Read as Text format
        TextFormatsReader textFormatsReader = new TextFormatsReader(endpoint, user, password, database);
        textFormatsReader.readAsJsonEachRow();
        textFormatsReader.readAsJsonEachRowButGSon();
        textFormatsReader.readJSONEachRowIntoArrayOfObject();
        textFormatsReader.readJSONEachRowIntoArrayOfObjectGson();
        textFormatsReader.readAsCSV();
        textFormatsReader.readAsTSV();

        // Insert data using POJO
        POJO2DbWriter pojoWriter = new POJO2DbWriter(endpoint, user, password, database);
        pojoWriter.resetTable();
        for (int i = 0; i < 10; i++) {
            pojoWriter.submit(new ArticleViewEvent(11132929d, LocalDateTime.now(), UUID.randomUUID().toString()));
        }

        pojoWriter.printLastEvents();

        // Insert data using POJO with JSON
        ExperimentalJSONExample jsonExample = new ExperimentalJSONExample(endpoint, user, password, database);
        jsonExample.writeData();
        jsonExample.readData();

        log.info("Done");
        Runtime.getRuntime().exit(0);
    }
}
