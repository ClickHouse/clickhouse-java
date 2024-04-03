package com.clickhouse.client;

import com.clickhouse.client.api.InsertSettings;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.generators.InsertDataGenerator;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.SocketException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class InsertTests {
    private Client client;

    @BeforeEach
    public void setUp() {
        client = new Client.Builder()
                .addEndpoint("http://localhost:8123")
                .addUsername("default")
                .addPassword("")
                .build();
    }

    @Test
    public void insertSimplePOJOs() throws ClickHouseException, SocketException {
        InsertSettings settings = new InsertSettings.Builder()
                .addDeduplicationToken("1234567890")
                .addQueryId(String.valueOf(UUID.randomUUID()))
                .build();

        String table = "simple_pojo_table";
        List<Object> simplePOJOs = InsertDataGenerator.generateSimplePOJOs();
        List<ClickHouseColumn> columns = new ArrayList<>();
        try(ClickHouseResponse response = client.insert(table, simplePOJOs, settings, columns)) {
            // do something with response
            assertNotEquals(response, null);
        }
    }

    @Test
    public void insertSimpleRowBinary() throws ClickHouseException, SocketException {
        InsertSettings settings = new InsertSettings.Builder()
                .addFormat(ClickHouseFormat.RowBinary)
                .addDeduplicationToken("1234567890")
                .addQueryId(String.valueOf(UUID.randomUUID()))
                .build();

        String table = "row_binary_table";
        InputStream dataStream = InsertDataGenerator.generateSimpleRowBinaryData();
        try(ClickHouseResponse response = client.insert(table, dataStream, settings)) {
            // do something with response
            assertNotEquals(response, null);
        }
    }
}
