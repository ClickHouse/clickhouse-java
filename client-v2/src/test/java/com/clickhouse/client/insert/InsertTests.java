package com.clickhouse.client.insert;

import com.clickhouse.client.*;
import com.clickhouse.client.api.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.generators.InsertDataGenerator;
import com.clickhouse.data.ClickHouseFormat;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.testng.Assert.*;

public class InsertTests extends BaseIntegrationTest {
    private Client client;
    private InsertSettings settings;

    @BeforeMethod(groups = { "unit" }, enabled = true)
    public void setUp() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort())
                .addUsername("default")
                .addPassword("")
                .build();
        settings = new InsertSettings()
                .setDeduplicationToken(RandomStringUtils.randomAlphabetic(36))
                .setQueryId(String.valueOf(UUID.randomUUID()));
    }

    private void createTable(String tableQuery) throws ClickHouseException {
        try (ClickHouseClient client = ClickHouseClient.builder().config(new ClickHouseConfig())
                        .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                        .build()) {
            client.read(getServer(ClickHouseProtocol.HTTP)).query(tableQuery).executeAndWait().close();
        }
    }

    @Test(groups = { "unit" }, enabled = false)
    public void registerSimplePOJOs() {
        System.out.println("registerSimplePOJOs");
        client.register(SamplePOJO.class, InsertDataGenerator.generateSimpleTableSchema("simple_pojo_table"));
    }

    @Test(groups = { "unit" }, enabled = true)
    public void insertSimplePOJOs() throws ClickHouseException, IOException, ExecutionException, InterruptedException, InvocationTargetException, IllegalAccessException {
        String tableName = "simple_pojo_table";
        createTable(String.format("CREATE TABLE %s (id Int32, name String, value Float64) ENGINE = Memory", tableName));

        client.register(SamplePOJO.class, InsertDataGenerator.generateSimpleTableSchema(tableName));
        List<Object> simplePOJOs = InsertDataGenerator.generateSimplePOJOs();
        InsertResponse response = client.insert(tableName, simplePOJOs, settings);

        ClickHouseResponseSummary summary = response.getSummary();
        assertNotEquals(summary, null);
        assertEquals(simplePOJOs.size(), summary.getWrittenRows());
    }

    @Test(groups = { "unit" }, enabled = false)
    public void insertSimpleRowBinary() throws ClickHouseException, IOException, ExecutionException, InterruptedException {
        InsertSettings settings = new InsertSettings()
                .setFormat(ClickHouseFormat.RowBinary)
                .setDeduplicationToken("1234567890")
                .setQueryId(String.valueOf(UUID.randomUUID()));

        String table = "row_binary_table";
        InputStream dataStream = InsertDataGenerator.generateSimpleRowBinaryData();
        Future<InsertResponse> response = client.insert(table, dataStream, settings);
        assertNotEquals(response.get(), null);
    }
}
