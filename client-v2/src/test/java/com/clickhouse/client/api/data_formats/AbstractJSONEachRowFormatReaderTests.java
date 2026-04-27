package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

public abstract class AbstractJSONEachRowFormatReaderTests extends BaseIntegrationTest {

    protected Client client;

    protected abstract String getProcessor();

    @BeforeMethod(groups = {"integration"})
    public void setUp() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setOption(ClientConfigProperties.JSON_PROCESSOR.getKey(), getProcessor())
                .build();
    }

    @AfterMethod(groups = {"integration"})
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    private QuerySettings newJsonEachRowSettings() {
        return new QuerySettings()
                .setFormat(ClickHouseFormat.JSONEachRow);
    }

    @Test(groups = {"integration"})
    public void testBasicParsing() throws Exception {
        String sql = "SELECT 1 as id, 'test' as name, true as active " +
                     "UNION ALL SELECT 2, 'clickhouse', false";

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get()) {
            ClickHouseTextFormatReader reader = client.newTextFormatReader(response);

            // First row
            Assert.assertTrue(reader.hasNext());
            Map<String, Object> row1 = reader.next();
            Assert.assertNotNull(row1);
            Assert.assertEquals(reader.getInteger("id"), 1);
            Assert.assertEquals(reader.getString("name"), "test");
            Assert.assertEquals(reader.getBoolean("active"), true);

            // Second row
            Assert.assertTrue(reader.hasNext());
            Map<String, Object> row2 = reader.next();
            Assert.assertNotNull(row2);
            Assert.assertEquals(reader.getInteger("id"), 2);
            Assert.assertEquals(reader.getString("name"), "clickhouse");
            Assert.assertEquals(reader.getBoolean("active"), false);

            // No more rows
            Assert.assertNull(reader.next());
        }
    }

    @Test(groups = {"integration"})
    public void testSchemaInference() throws Exception {
        String sql = "SELECT toInt64(42) as col_int, toFloat64(3.14) as col_float, " +
                     "true as col_bool, 'val' as col_str";

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get()) {
            ClickHouseTextFormatReader reader = client.newTextFormatReader(response);

            Assert.assertNotNull(reader.getSchema());
            Assert.assertEquals(reader.getSchema().getColumns().size(), 4);

            Assert.assertEquals(reader.getSchema().getColumnByIndex(1).getDataType(), ClickHouseDataType.Int64);
            Assert.assertEquals(reader.getSchema().getColumnByIndex(2).getDataType(), ClickHouseDataType.Float64);
            Assert.assertEquals(reader.getSchema().getColumnByIndex(3).getDataType(), ClickHouseDataType.Bool);
            Assert.assertEquals(reader.getSchema().getColumnByIndex(4).getDataType(), ClickHouseDataType.String);
        }
    }

    @Test(groups = {"integration"})
    public void testDataTypes() throws Exception {
        String sql = "SELECT toInt8(120) as b, toInt16(30000) as s, toInt32(1000000) as i, " +
                     "toInt64(10000000000) as l, toFloat32(1.23) as f, toFloat64(1.23456789) as d, " +
                     "true as bool, 'hello' as str";

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get()) {
            ClickHouseTextFormatReader reader = client.newTextFormatReader(response);

            reader.next();
            Assert.assertEquals(reader.getByte("b"), (byte) 120);
            Assert.assertEquals(reader.getShort("s"), (short) 30000);
            Assert.assertEquals(reader.getInteger("i"), 1000000);
            Assert.assertEquals(reader.getLong("l"), 10000000000L);
            Assert.assertEquals(reader.getFloat("f"), 1.23f, 0.001f);
            Assert.assertEquals(reader.getDouble("d"), 1.23456789d, 0.00000001d);
            Assert.assertEquals(reader.getBoolean("bool"), true);
            Assert.assertEquals(reader.getString("str"), "hello");
        }
    }

    @Test(groups = {"integration"})
    public void testEmptyData() throws Exception {
        String sql = "SELECT * FROM remote('127.0.0.1', system.one) WHERE dummy > 1";

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get()) {
            ClickHouseTextFormatReader reader = client.newTextFormatReader(response);

            Assert.assertFalse(reader.hasNext());
            Assert.assertNull(reader.next());
            Assert.assertEquals(reader.getSchema().getColumns().size(), 0);
        }
    }

    @Test(groups = {"integration"}, expectedExceptions = IllegalArgumentException.class)
    public void testNewBinaryFormatReaderRejectsJsonEachRow() throws Exception {
        String sql = "SELECT 1 as id";

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get()) {
            client.newBinaryFormatReader(response);
        }
    }
}
