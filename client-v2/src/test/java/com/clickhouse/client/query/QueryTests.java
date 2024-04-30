package com.clickhouse.client.query;


import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.Protocol;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryStreamReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;

public class QueryTests extends BaseIntegrationTest {

    private Client client;

    @BeforeMethod(groups = {"integration"})
    public void setUp() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort())
                .addUsername("default")
                .addPassword("")
                .build();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(node.getPort());
    }

    @Test(groups = {"integration"})
    public void testSimpleQueryWithTSV() {
        prepareSimpleDataSet();

        QuerySettings settings = new QuerySettings()
                .setFormat(ClickHouseFormat.TabSeparated.name());

        Future<QueryResponse> response = client.query("SELECT * FROM " + DATASET_TABLE,
                Collections.emptyMap(), settings);

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(response.get().getInputStream()));
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (InterruptedException | ExecutionException e) {
            Assert.fail("failed to get response", e);
        } catch (IOException e) {
            Assert.fail("failed to read response", e);

        }
    }

    @Test(groups = {"integration"}, enabled = false)
    public void testQueryJSONWith64BitIntegers() throws ExecutionException, InterruptedException {
        // won't work because format settings are set thru separate statement.
        prepareSimpleDataSet();
        List<QuerySettings> settingsList = Arrays.asList(
                new QuerySettings()
                        .setFormat(ClickHouseFormat.JSONEachRow.name())
                        .setSetting("format_json_quote_64bit_integers", "true"),

                new QuerySettings().setFormat(ClickHouseFormat.JSONEachRow.name()));
        List<Boolean> expected = Arrays.asList(true, false);

        Iterator<Boolean> expectedIterator = expected.iterator();
        for (QuerySettings settings : settingsList) {
            Future<QueryResponse> response = client.query("SELECT * FROM " + DATASET_TABLE, null, settings);
            QueryResponse queryResponse = response.get();
            ArrayList<JsonNode> records = new ArrayList<>();
            final ObjectMapper objectMapper = new ObjectMapper();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(queryResponse.getInputStream()))) {
                String line = null;
                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                    records.add(objectMapper.readTree(line));
                    Assert.assertEquals(records.get(0).get("param4").isTextual(),expectedIterator.next());
                }
            } catch (IOException e) {
                Assert.fail("failed to read response", e);
            }
        }
    }

    @Test(groups = {"integration"})
    public void testAsyncResponse() {

    }

    @Test(groups = {"integration"})
    public void testBlockingResponse() {

    }

    @DataProvider(name = "rowBinaryFormats")
    ClickHouseFormat[] getRowBinaryFormats() {

        return new ClickHouseFormat[]{
                ClickHouseFormat.RowBinary,
                ClickHouseFormat.RowBinaryWithNames,
                ClickHouseFormat.RowBinaryWithNamesAndTypes,
                ClickHouseFormat.Native
        };
    }

    @Test(groups = {"integration"}, dataProvider = "rowBinaryFormats")
    public void testRowBinaryQueries(ClickHouseFormat format) throws ExecutionException, InterruptedException {
        final int rows = 10;
        // TODO: replace with dataset with all primitive types of data
        // TODO: reusing same table name may lead to a conflict in tests?
        List<Map<String, Object>> data = prepareDataSet(DATASET_TABLE + "_" + format.name(), DATASET_COLUMNS, DATASET_VALUE_GENERATORS, rows);
        QuerySettings settings = new QuerySettings().setFormat(format.name());
        Future<QueryResponse> response = client.query("SELECT * FROM " + DATASET_TABLE + "_" + format.name(), null, settings);

        QueryResponse queryResponse = response.get();

        Map<String, Object> record = new HashMap<>();
        ClickHouseBinaryStreamReader reader = ClickHouseBinaryStreamReader.create(queryResponse, settings);

        Iterator<Map<String, Object>> dataIterator = data.iterator();
        try {
            while (dataIterator.hasNext()) {
                Map<String, Object> expectedRecord = dataIterator.next();
                System.out.println(expectedRecord);

                reader.readToMap(record, client.getTableSchema(DATASET_TABLE + "_" + format.name()));
                System.out.println(record);
                Assert.assertEquals(record, expectedRecord);
            }
        } catch (IOException e) {
            Assert.fail("failed to read response", e);
        }
    }

    @Test(groups = {"integration"})
    public void testQueryExceptionHandling() {

    }

    private final static Random RANDOM = new Random();

    private final static List<String> DATASET_COLUMNS = List.of(
            "param1 UInt32",
            "param2 Int32",
            "param3 String",
            "param4 Int64");


    private final static List<Function<String, Object>> DATASET_VALUE_GENERATORS = List.of(
            c -> RANDOM.nextLong(0xFFFFFFFFL),
            c -> RANDOM.nextInt(Integer.MAX_VALUE),
            c -> "value_" + RANDOM.nextInt(Integer.MAX_VALUE),
            c -> RANDOM.nextLong()
    );

    private final static String DATASET_TABLE = "query_test_table";

    private void prepareSimpleDataSet() {
        prepareDataSet(DATASET_TABLE, DATASET_COLUMNS, DATASET_VALUE_GENERATORS, 1);
    }

    private List<Map<String, Object>> prepareDataSet(String table, List<String> columns, List<Function<String, Object>> valueGenerators,
                                                     int rows) {
        List<Map<String, Object>> data = new ArrayList<>(rows);
        try (
            ClickHouseClient client = ClickHouseClient.builder().config(new ClickHouseConfig())
                        .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                        .build()) {

            // Create table
            StringBuilder createStmtBuilder = new StringBuilder();
            createStmtBuilder.append("CREATE TABLE IF NOT EXISTS default.").append(table).append(" (");
            for (String column : columns) {
                createStmtBuilder.append(column).append(", ");
            }
            createStmtBuilder.setLength(createStmtBuilder.length() - 2);
            createStmtBuilder.append(") ENGINE = Memory");

            ClickHouseRequest<?> request = client.read(getServer(ClickHouseProtocol.HTTP))
                    .query(createStmtBuilder.toString());
            request.executeAndWait();

            // Insert data
            StringBuilder insertStmtBuilder = new StringBuilder();
            insertStmtBuilder.append("INSERT INTO default.").append(table).append(" VALUES ");
            for (int i = 0; i < rows; i++) {
                insertStmtBuilder.append("(");
                Map<String, Object> values = new HashMap<>();
                Iterator<String> columnIterator = columns.iterator();
                for (Function<String, Object> valueGenerator : valueGenerators) {
                    Object value = valueGenerator.apply(null);
                    if (value instanceof String) {
                        insertStmtBuilder.append('\'').append(value).append('\'').append(", ");
                    } else {
                        insertStmtBuilder.append(value).append(", ");
                    }
                    values.put(columnIterator.next().split(" ")[0], value);

                }
                insertStmtBuilder.setLength(insertStmtBuilder.length() - 2);
                insertStmtBuilder.append("), ");
                data.add(values);
            }
            request = client.write(getServer(ClickHouseProtocol.HTTP))
                    .query(insertStmtBuilder.toString());
            request.executeAndWait();
        } catch (Exception e) {
            Assert.fail("failed to prepare data set", e);
        }
        return data;
    }
}
