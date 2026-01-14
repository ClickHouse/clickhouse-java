package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseVersion;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class FlattenedJSONWriterTest extends BaseIntegrationTest {
    private Client client;
    private InsertSettings settings;
    private static final int EXECUTE_CMD_TIMEOUT = 30;

    @BeforeMethod(groups = { "integration" })
    public void setUp() throws IOException {
        int bufferSize = (7 * 65500);
        client = newClient()
                .setSocketSndbuf(bufferSize)
                .setSocketRcvbuf(bufferSize)
                .setClientNetworkBufferSize(bufferSize)
                .build();

        settings = new InsertSettings()
                .setDeduplicationToken(RandomStringUtils.randomAlphabetic(36))
                .setQueryId(String.valueOf(UUID.randomUUID()));
    }

    protected Client.Builder newClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        Client.Builder builder = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .serverSetting(ServerSettings.ASYNC_INSERT, "0")
                .serverSetting(ServerSettings.WAIT_END_OF_QUERY, "1");

        if (isVersionMatch("[24.10,)")) {
            builder.serverSetting(ServerSettings.INPUT_FORMAT_BINARY_READ_JSON_AS_STRING, "1");
        }

        return builder;
    }

    protected boolean isVersionMatch(String versionExpression) {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        try(Client client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .build()) {
            List<GenericRecord> serverVersion = client.queryAll("SELECT version()");
            return ClickHouseVersion.of(serverVersion.get(0).getString(1)).check(versionExpression);
        }
    }

    protected void initTable(String tableName, String createTableSQL, CommandSettings commandSettings) throws Exception {
        if (commandSettings == null) {
            commandSettings = new CommandSettings();
        }

        if (isVersionMatch("[24.8,)")) {
            commandSettings.serverSetting("allow_experimental_variant_type", "1")
                    .serverSetting("allow_experimental_dynamic_type", "1")
                    .serverSetting("allow_experimental_json_type", "1");
        }

        client.execute("DROP TABLE IF EXISTS " + tableName, commandSettings).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS);
        client.execute(createTableSQL, commandSettings).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS);
    }

    private String buildJsonString(String key, Object value) {
        if (value == null) {
            return "{\"" + key + "\":null}";
        } else if (value instanceof String) {
            return "{\"" + key + "\":\"" + value + "\"}";
        } else if (value instanceof Boolean) {
            return "{\"" + key + "\":" + value + "}";
        } else if (value instanceof Number) {
            return "{\"" + key + "\":" + value + "}";
        } else {
            return "{\"" + key + "\":\"" + value.toString() + "\"}";
        }
    }

    @Test(groups = { "integration" })
    public void testWriteString() throws Exception {
        if (!isVersionMatch("[24.10,)")) {
            return;
        }

        String tableName = "flattened_json_writer_test_string_" + UUID.randomUUID().toString().replace('-', '_');
        String createSQL = "CREATE TABLE \"" + tableName + "\" " +
                "(id Int32, " +
                "data JSON" +
                ") ENGINE = MergeTree ORDER BY id";

        CommandSettings commandSettings = new CommandSettings();
        commandSettings.serverSetting("allow_experimental_json_type", "1");
        initTable(tableName, createSQL, commandSettings);

        TableSchema schema = client.getTableSchema(tableName);
        String key = "name";
        String value = "John Doe";
        String jsonData = buildJsonString(key, value);

        ClickHouseFormat format = ClickHouseFormat.RowBinary;
        try (InsertResponse response = client.insert(tableName, out -> {
            RowBinaryFormatWriter rowWriter = new RowBinaryFormatWriter(out, schema.getColumns(), format);
            rowWriter.setInteger("id", 1);
            rowWriter.setString("data", jsonData);
            rowWriter.commitRow();
        }, format, settings).get()) {
            Assert.assertEquals(response.getWrittenRows(), 1);
        }

        List<GenericRecord> records = client.queryAll("SELECT * FROM \"" + tableName + "\"");
        Assert.assertEquals(records.size(), 1);
        GenericRecord record = records.get(0);
        Assert.assertEquals(record.getInteger("id"), Integer.valueOf(1));
        // JSON column returns as Map when queried
        Object data = record.getValues().get("data");
        Assert.assertNotNull(data);
        Assert.assertTrue(data instanceof Map);
        Map<String, Object> jsonMap = (Map<String, Object>) data;
        Assert.assertEquals(jsonMap.get(key), value);
    }

    @Test(groups = { "integration" })
    public void testWriteInt() throws Exception {
        if (!isVersionMatch("[24.10,)")) {
            return;
        }

        String tableName = "flattened_json_writer_test_int_" + UUID.randomUUID().toString().replace('-', '_');
        String createSQL = "CREATE TABLE \"" + tableName + "\" " +
                "(id Int32, " +
                "data JSON" +
                ") ENGINE = MergeTree ORDER BY id";

        CommandSettings commandSettings = new CommandSettings();
        commandSettings.serverSetting("allow_experimental_json_type", "1");
        initTable(tableName, createSQL, commandSettings);

        TableSchema schema = client.getTableSchema(tableName);
        String key = "age";
        Integer value = 42;
        String jsonData = buildJsonString(key, value);

        ClickHouseFormat format = ClickHouseFormat.RowBinary;
        try (InsertResponse response = client.insert(tableName, out -> {
            RowBinaryFormatWriter rowWriter = new RowBinaryFormatWriter(out, schema.getColumns(), format);
            rowWriter.setInteger("id", 1);
            rowWriter.setString("data", jsonData);
            rowWriter.commitRow();
        }, format, settings).get()) {
            Assert.assertEquals(response.getWrittenRows(), 1);
        }

        List<GenericRecord> records = client.queryAll("SELECT * FROM \"" + tableName + "\"");
        Assert.assertEquals(records.size(), 1);
        GenericRecord record = records.get(0);
        Assert.assertEquals(record.getInteger("id"), Integer.valueOf(1));
        Map<String, Object> jsonMap = (Map<String, Object>) record.getValues().get("data");
        Assert.assertEquals(((Long) jsonMap.get(key)).intValue(), value);
    }

    @Test(groups = { "integration" })
    public void testWriteLong() throws Exception {
        if (!isVersionMatch("[24.10,)")) {
            return;
        }

        String tableName = "flattened_json_writer_test_long_" + UUID.randomUUID().toString().replace('-', '_');
        String createSQL = "CREATE TABLE \"" + tableName + "\" " +
                "(id Int32, " +
                "data JSON" +
                ") ENGINE = MergeTree ORDER BY id";

        CommandSettings commandSettings = new CommandSettings();
        commandSettings.serverSetting("allow_experimental_json_type", "1");
        initTable(tableName, createSQL, commandSettings);

        TableSchema schema = client.getTableSchema(tableName);
        String key = "timestamp";
        Long value = 1234567890L;
        String jsonData = buildJsonString(key, value);

        ClickHouseFormat format = ClickHouseFormat.RowBinary;
        try (InsertResponse response = client.insert(tableName, out -> {
            RowBinaryFormatWriter rowWriter = new RowBinaryFormatWriter(out, schema.getColumns(), format);
            rowWriter.setInteger("id", 1);
            rowWriter.setString("data", jsonData);
            rowWriter.commitRow();
        }, format, settings).get()) {
            Assert.assertEquals(response.getWrittenRows(), 1);
        }

        List<GenericRecord> records = client.queryAll("SELECT * FROM \"" + tableName + "\"");
        Assert.assertEquals(records.size(), 1);
        GenericRecord record = records.get(0);
        Assert.assertEquals(record.getInteger("id"), Integer.valueOf(1));
        Map<String, Object> jsonMap = (Map<String, Object>) record.getValues().get("data");
        // JSON numbers are read as Long
        Object readValue = jsonMap.get(key);
        Assert.assertTrue(readValue instanceof Long || readValue instanceof Integer);
        Assert.assertEquals(((Number) readValue).longValue(), value.longValue());
    }

    @Test(groups = { "integration" })
    public void testWriteDouble() throws Exception {
        if (!isVersionMatch("[24.10,)")) {
            return;
        }

        String tableName = "flattened_json_writer_test_double_" + UUID.randomUUID().toString().replace('-', '_');
        String createSQL = "CREATE TABLE \"" + tableName + "\" " +
                "(id Int32, " +
                "data JSON" +
                ") ENGINE = MergeTree ORDER BY id";

        CommandSettings commandSettings = new CommandSettings();
        commandSettings.serverSetting("allow_experimental_json_type", "1");
        initTable(tableName, createSQL, commandSettings);

        TableSchema schema = client.getTableSchema(tableName);
        String key = "pi";
        Double value = 3.141592653589793;
        String jsonData = buildJsonString(key, value);

        ClickHouseFormat format = ClickHouseFormat.RowBinary;
        try (InsertResponse response = client.insert(tableName, out -> {
            RowBinaryFormatWriter rowWriter = new RowBinaryFormatWriter(out, schema.getColumns(), format);
            rowWriter.setInteger("id", 1);
            rowWriter.setString("data", jsonData);
            rowWriter.commitRow();
        }, format, settings).get()) {
            Assert.assertEquals(response.getWrittenRows(), 1);
        }

        List<GenericRecord> records = client.queryAll("SELECT * FROM \"" + tableName + "\"");
        Assert.assertEquals(records.size(), 1);
        GenericRecord record = records.get(0);
        Assert.assertEquals(record.getInteger("id"), Integer.valueOf(1));
        Map<String, Object> jsonMap = (Map<String, Object>) record.getValues().get("data");
        Object readValue = jsonMap.get(key);
        Assert.assertTrue(readValue instanceof Number);
        Assert.assertEquals(((Number) readValue).doubleValue(), value, 0.000000000000001);
    }

    @Test(groups = { "integration" })
    public void testWriteBoolean() throws Exception {
        if (!isVersionMatch("[24.10,)")) {
            return;
        }

        String tableName = "flattened_json_writer_test_boolean_" + UUID.randomUUID().toString().replace('-', '_');
        String createSQL = "CREATE TABLE \"" + tableName + "\" " +
                "(id Int32, " +
                "data JSON" +
                ") ENGINE = MergeTree ORDER BY id";

        CommandSettings commandSettings = new CommandSettings();
        commandSettings.serverSetting("allow_experimental_json_type", "1");
        initTable(tableName, createSQL, commandSettings);

        TableSchema schema = client.getTableSchema(tableName);
        String key = "isActive";
        Boolean value = true;
        String jsonData = buildJsonString(key, value);

        ClickHouseFormat format = ClickHouseFormat.RowBinary;
        try (InsertResponse response = client.insert(tableName, out -> {
            RowBinaryFormatWriter rowWriter = new RowBinaryFormatWriter(out, schema.getColumns(), format);
            rowWriter.setInteger("id", 1);
            rowWriter.setString("data", jsonData);
            rowWriter.commitRow();
        }, format, settings).get()) {
            Assert.assertEquals(response.getWrittenRows(), 1);
        }

        List<GenericRecord> records = client.queryAll("SELECT * FROM \"" + tableName + "\"");
        Assert.assertEquals(records.size(), 1);
        GenericRecord record = records.get(0);
        Assert.assertEquals(record.getInteger("id"), Integer.valueOf(1));
        Map<String, Object> jsonMap = (Map<String, Object>) record.getValues().get("data");
        Assert.assertEquals(jsonMap.get(key), value);
    }

    @Test(groups = { "integration" })
    public void testWriteBigDecimal() throws Exception {
        if (!isVersionMatch("[24.10,)")) {
            return;
        }

        String tableName = "flattened_json_writer_test_bigdecimal_" + UUID.randomUUID().toString().replace('-', '_');
        String createSQL = "CREATE TABLE \"" + tableName + "\" " +
                "(id Int32, " +
                "data JSON" +
                ") ENGINE = MergeTree ORDER BY id";

        CommandSettings commandSettings = new CommandSettings();
        commandSettings.serverSetting("allow_experimental_json_type", "1");
        initTable(tableName, createSQL, commandSettings);

        TableSchema schema = client.getTableSchema(tableName);
        String key = "price";
        BigDecimal value = new BigDecimal("123.45678901234567890");
        String jsonData = buildJsonString(key, value);

        ClickHouseFormat format = ClickHouseFormat.RowBinary;
        try (InsertResponse response = client.insert(tableName, out -> {
            RowBinaryFormatWriter rowWriter = new RowBinaryFormatWriter(out, schema.getColumns(), format);
            rowWriter.setInteger("id", 1);
            rowWriter.setString("data", jsonData);
            rowWriter.commitRow();
        }, format, settings).get()) {
            Assert.assertEquals(response.getWrittenRows(), 1);
        }

        List<GenericRecord> records = client.queryAll("SELECT * FROM \"" + tableName + "\"");
        Assert.assertEquals(records.size(), 1);
        GenericRecord record = records.get(0);
        Assert.assertEquals(record.getInteger("id"), Integer.valueOf(1));
        Map<String, Object> jsonMap = (Map<String, Object>) record.getValues().get("data");
        Object readValue = jsonMap.get(key);
        // JSON numbers might be read as Double
        Assert.assertTrue(readValue instanceof Number);
    }

    @Test(groups = { "integration" })
    public void testMultipleKeyValuePairs() throws Exception {
        if (!isVersionMatch("[24.10,)")) {
            return;
        }

        String tableName = "flattened_json_writer_test_multiple_" + UUID.randomUUID().toString().replace('-', '_');
        String createSQL = "CREATE TABLE \"" + tableName + "\" " +
                "(id Int32, " +
                "data JSON" +
                ") ENGINE = MergeTree ORDER BY id";

        CommandSettings commandSettings = new CommandSettings();
        commandSettings.serverSetting("allow_experimental_json_type", "1");
        initTable(tableName, createSQL, commandSettings);

        TableSchema schema = client.getTableSchema(tableName);

        ClickHouseFormat format = ClickHouseFormat.RowBinary;
        try (InsertResponse response = client.insert(tableName, out -> {
            RowBinaryFormatWriter rowWriter = new RowBinaryFormatWriter(out, schema.getColumns(), format);
            
            // Insert multiple rows with different JSON objects
            String[] jsonDataArray = {
                "{\"name\":\"Alice\"}",
                "{\"age\":30}",
                "{\"active\":true}",
                "{\"score\":95.5}"
            };
            
            for (int i = 0; i < jsonDataArray.length; i++) {
                rowWriter.setInteger("id", i + 1);
                rowWriter.setString("data", jsonDataArray[i]);
                rowWriter.commitRow();
            }
        }, format, settings).get()) {
            Assert.assertEquals(response.getWrittenRows(), 4);
        }

        List<GenericRecord> records = client.queryAll("SELECT * FROM \"" + tableName + "\" ORDER BY id");
        Assert.assertEquals(records.size(), 4);
        
        GenericRecord record1 = records.get(0);
        Assert.assertEquals(record1.getInteger("id"), Integer.valueOf(1));
        Map<String, Object> jsonMap1 = (Map<String, Object>) record1.getValues().get("data");
        Assert.assertEquals(jsonMap1.get("name"), "Alice");
        
        GenericRecord record2 = records.get(1);
        Assert.assertEquals(record2.getInteger("id"), Integer.valueOf(2));
        Map<String, Object> jsonMap2 = (Map<String, Object>) record2.getValues().get("data");
        Object ageValue = jsonMap2.get("age");
        Assert.assertTrue(ageValue instanceof Number);
        Assert.assertEquals(((Number) ageValue).intValue(), 30);
        
        GenericRecord record3 = records.get(2);
        Assert.assertEquals(record3.getInteger("id"), Integer.valueOf(3));
        Map<String, Object> jsonMap3 = (Map<String, Object>) record3.getValues().get("data");
        Assert.assertEquals(jsonMap3.get("active"), true);
        
        GenericRecord record4 = records.get(3);
        Assert.assertEquals(record4.getInteger("id"), Integer.valueOf(4));
        Map<String, Object> jsonMap4 = (Map<String, Object>) record4.getValues().get("data");
        Object scoreValue = jsonMap4.get("score");
        Assert.assertTrue(scoreValue instanceof Number);
        Assert.assertEquals(((Number) scoreValue).doubleValue(), 95.5, 0.0001);
    }

    @Test(groups = { "integration" })
    public void testWriteNull() throws Exception {
        if (!isVersionMatch("[24.10,)")) {
            return;
        }

        String tableName = "flattened_json_writer_test_null_" + UUID.randomUUID().toString().replace('-', '_');
        String createSQL = "CREATE TABLE \"" + tableName + "\" " +
                "(id Int32, " +
                "data JSON" +
                ") ENGINE = MergeTree ORDER BY id";

        CommandSettings commandSettings = new CommandSettings();
        commandSettings.serverSetting("allow_experimental_json_type", "1");
        initTable(tableName, createSQL, commandSettings);

        TableSchema schema = client.getTableSchema(tableName);
        String key = "nullable";
        String jsonData = buildJsonString(key, null);

        ClickHouseFormat format = ClickHouseFormat.RowBinary;
        try (InsertResponse response = client.insert(tableName, out -> {
            RowBinaryFormatWriter rowWriter = new RowBinaryFormatWriter(out, schema.getColumns(), format);
            rowWriter.setInteger("id", 1);
            rowWriter.setString("data", jsonData);
            rowWriter.commitRow();
        }, format, settings).get()) {
            Assert.assertEquals(response.getWrittenRows(), 1);
        }

        List<GenericRecord> records = client.queryAll("SELECT * FROM \"" + tableName + "\"");
        Assert.assertEquals(records.size(), 1);
        GenericRecord record = records.get(0);
        Assert.assertEquals(record.getInteger("id"), Integer.valueOf(1));
        Map<String, Object> jsonMap = (Map<String, Object>) record.getValues().get("data");
        Assert.assertNull(jsonMap.get(key));
    }
}
