package com.clickhouse.examples.client_v2;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.Strictness;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.Future;

@Slf4j
public class TextFormatsReader {

    private static final String TABLE_NAME = "hacker_news_articles";

    Client client;

    public TextFormatsReader(String endpoint, String user, String password, String database) {
        // Create a lightweight object to interact with ClickHouse server
        Client.Builder clientBuilder = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(password)
                .compressServerResponse(true)
                .setDefaultDatabase(database);

        this.client = clientBuilder.build();
    }


    public void readAsJsonEachRow() {
        log.info("Reading data from table as JSONEachRow: {}", TABLE_NAME);
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.JSONEachRow);
        Future<QueryResponse> response = client.query("SELECT * FROM " + TABLE_NAME, settings);
        final ObjectMapper objectMapper = new ObjectMapper();
        try (QueryResponse queryResponse = response.get();
             MappingIterator<JsonNode> jsonIter = objectMapper.readerFor(JsonNode.class)
                     .readValues(queryResponse.getInputStream())) {

            while (jsonIter.hasNext()) {
                JsonNode node = jsonIter.next();
                if (node.get("type").asText().equalsIgnoreCase("story")) {
                    StringBuilder row = new StringBuilder();
                    row.append("story: ").append(node.get("title").asText()).append(", by: ").append(node.get("by").asText());
                    log.info(">: {}", row);
                }
            }
        } catch (Exception e) {
            // Log of
            log.error("Failed to read data from ClickHouse", e);
        }
    }

    public void readAsJsonEachRowButGSon() {
        log.info("Reading data from table as JSON (GSON version): {}", TABLE_NAME);

        // GSON doesn't have built-in support for JSON-L (JSONEachRow) format, so we will use JSON where
        // data is located in $.data[] field what is array of JSON objects
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.JSON);
        Future<QueryResponse> response = client.query("SELECT * FROM " + TABLE_NAME, settings);

        Gson gson = new GsonBuilder().setStrictness(Strictness.LENIENT).create(); //
        try (QueryResponse queryResponse = response.get();
             JsonReader reader = gson.newJsonReader(new InputStreamReader(queryResponse.getInputStream()))) {

            reader.beginObject(); // root object
            while (reader.hasNext()) {
                String name = reader.nextName();
                if (name.equals("data")) {

                    reader.beginArray(); // array of JSON objects
                    while (reader.hasNext()) {
                        reader.beginObject(); // JSON object
                        StringBuilder row = new StringBuilder();
                        boolean isStory = false;
                        while (reader.hasNext()) {
                            try {
                                String fieldName = reader.nextName(); //
                                if (fieldName.equals("type")) {
                                    String type = reader.nextString();
                                    isStory = type.equalsIgnoreCase("story");
                                } else if (isStory && fieldName.equals("title")) {
                                    row.append(" story: ").append(reader.nextString());
                                } else if (isStory && fieldName.equals("by")) {
                                    row.append(" by: ").append(reader.nextString());
                                } else {
                                    reader.skipValue();
                                }
                            } catch (Exception e) {
                                System.out.println(">! " + reader.nextString());
                                log.error("Failed to read field", e);
                            }
                        }
                        if (isStory) {
                            log.info(">: " + row);
                        }
                        reader.endObject();
                    }
                    reader.endArray();
                } else {
                    reader.skipValue();
                }
            }
        } catch (Exception e) {
            log.error("Failed to read data from ClickHouse", e);
        }
    }

    public void readJSONEachRowIntoArrayOfObject() {
        log.info("Reading data from table as JSONEachRow into Array: {}", TABLE_NAME);
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.JSONEachRow);
        Future<QueryResponse> response = client.query("SELECT * FROM " + TABLE_NAME, settings);
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try (QueryResponse queryResponse = response.get();
             MappingIterator<Record> jsonIter = objectMapper.readerFor(Record.class)
                     .readValues(queryResponse.getInputStream())) {

            ArrayList<Story> stories = new ArrayList<>();
            while (jsonIter.hasNext()) {
                Record record = jsonIter.next();
                if (record.getType().equalsIgnoreCase("story")) {
                    stories.add(new Story(record.getTitle(), record.getBy()));
                }
            }

            stories.forEach(s -> {
                log.info(">: {}", s);
            });

        } catch (Exception e) {
            // Log of
            log.error("Failed to read data from ClickHouse", e);
        }
    }

    public void readJSONEachRowIntoArrayOfObjectGson() {
        log.info("Reading data from table as JSONEachRow into Array (GSON version): {}", TABLE_NAME);

        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.JSONEachRow);
        Future<QueryResponse> response = client.query("SELECT * FROM " + TABLE_NAME, settings);

        Gson gson = new GsonBuilder().setStrictness(Strictness.LENIENT).create(); //
        TypeAdapter<Record> recordReader = gson.getAdapter(Record.class);

        try (QueryResponse queryResponse = response.get();
             BufferedReader reader = new BufferedReader(new InputStreamReader(queryResponse.getInputStream()))) {

            ArrayList<Story> stories = new ArrayList<>();
            String line = reader.readLine();
            while (line != null) {
                Record record = recordReader.fromJson(line);
                if (record.getType().equalsIgnoreCase("story")) {
                    stories.add(new Story(record.getTitle(), record.getBy()));
                }
                line = reader.readLine();
            }

            stories.forEach(s -> {
                log.info(">: {}", s);
            });

        } catch (Exception e) {
            log.error("Failed to read data from ClickHouse", e);
        }
    }


    public void readAsCSV() {
        log.info("Reading data from table as CSV document: {}", TABLE_NAME);

        // CSVWithNames format is used to get column names in the first row so columns can be addressed by name
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.CSVWithNames);
        Future<QueryResponse> response = client.query("SELECT * FROM " + TABLE_NAME, settings);

        try (QueryResponse queryResponse = response.get();
             BufferedReader reader = new BufferedReader(new InputStreamReader(queryResponse.getInputStream()));
             CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader)) {

            for (CSVRecord record : parser) {
                String type = record.get("type");
                if (type.equalsIgnoreCase("story")) {
                    StringBuilder row = new StringBuilder();
                    row.append("story: ").append(record.get("title")).append(", by: ").append(record.get("by"));
                    log.info(">: {}", row);
                }
            }

        } catch (Exception e) {
            log.error("Failed to read data from ClickHouse", e);
        }
    }

    public void readAsTSV() {
        log.info("Reading data from table as TSV document: {}", TABLE_NAME);

        // CSVWithNames format is used to get column names in the first row so columns can be addressed by name
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.TabSeparatedWithNames);
        Future<QueryResponse> response = client.query("SELECT * FROM " + TABLE_NAME, settings);


        CSVFormat TSVFormat = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setDelimiter('\t')
                .build();
        try (QueryResponse queryResponse = response.get();
             BufferedReader reader = new BufferedReader(new InputStreamReader(queryResponse.getInputStream()));
             CSVParser parser = TSVFormat.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader)) {

            for (CSVRecord record : parser) {
                String type = record.get("type");
                if (type.equalsIgnoreCase("story")) {
                    StringBuilder row = new StringBuilder();
                    row.append("story: ").append(record.get("title")).append(", by: ").append(record.get("by"));
                    log.info(">: {}", row);
                }
            }

        } catch (Exception e) {
            log.error("Failed to read data from ClickHouse", e);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Story {
        public String title;
        public String by;

        public String toString() {
            return "story: " + title + ", by: " + by;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Record {
        public String type;
        public String title;
        public String by;
    }
}
