package com.clickhouse.examples.client_v2;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.examples.client_v2.data.PojoWithJSON;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ExperimentalJSONExample {

    Client client;

    public ExperimentalJSONExample(String endpoint, String user, String password, String database) {
        // Create a lightweight object to interact with ClickHouse server
        Client.Builder clientBuilder = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(password)
                .compressServerResponse(true)
                // allow experimental JSON type
                .serverSetting("allow_experimental_json_type", "1")
                // allow JSON transcoding as a string
                .serverSetting(ServerSettings.INPUT_FORMAT_BINARY_READ_JSON_AS_STRING, "1")
                .serverSetting(ServerSettings.OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING, "1")
                .setDefaultDatabase(database);

        this.client = clientBuilder.build();
    }

    final String tableName = "pojo_with_json_table";
    final String createSQL = PojoWithJSON.createTable(tableName);
    final String originalJsonStr = "{\"a\":{\"b\":\"42\"},\"c\":[\"1\",\"2\",\"3\"]}";


    public void writeData() {
        CommandSettings commandSettings = new CommandSettings();
        commandSettings.serverSetting("allow_experimental_json_type", "1");

        try {
            client.execute("DROP TABLE IF EXISTS " + tableName, commandSettings).get(1, TimeUnit.SECONDS);
            client.execute(createSQL, commandSettings).get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        client.register(PojoWithJSON.class, client.getTableSchema(tableName, "default"));
        PojoWithJSON pojo = new PojoWithJSON();
        pojo.setEventPayload(originalJsonStr);
        List<Object> data = Arrays.asList(pojo);

        InsertSettings insertSettings = new InsertSettings()
                .serverSetting(ServerSettings.INPUT_FORMAT_BINARY_READ_JSON_AS_STRING, "1");
        try (InsertResponse response = client.insert(tableName, data, insertSettings).get(30, TimeUnit.SECONDS)) {
            log.info("Data write metrics: {}", response.getMetrics());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void readData() {
        try (QueryResponse resp = client.query("SELECT * FROM " + tableName).get(1, TimeUnit.SECONDS)) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(resp);
            assert reader.next() != null;
            String jsonStr = reader.getString(1);
            log.info("Read JSON string: {}", jsonStr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
