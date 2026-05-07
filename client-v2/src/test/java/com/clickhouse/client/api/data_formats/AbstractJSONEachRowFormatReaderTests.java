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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get();
             ClickHouseTextFormatReader reader = client.newTextFormatReader(response)) {

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
        // Covers all branches of guessDataType: numeric (Int64), numeric (Float64),
        // Boolean (Bool) and the catch-all branch that maps strings to String.
        String sql = "SELECT toInt64(42) as col_int, toFloat64(3.14) as col_float, " +
                     "true as col_bool, 'val' as col_str";

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get();
             ClickHouseTextFormatReader reader = client.newTextFormatReader(response)) {

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

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get();
             ClickHouseTextFormatReader reader = client.newTextFormatReader(response)) {

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

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get();
             ClickHouseTextFormatReader reader = client.newTextFormatReader(response)) {

            Assert.assertFalse(reader.hasNext());
            Assert.assertNull(reader.next());
            Assert.assertEquals(reader.getSchema().getColumns().size(), 0);
        }
    }

    @Test(groups = {"integration"})
    public void testIndexedAccessors() throws Exception {
        String sql = "SELECT toInt8(120) as b, toInt16(30000) as s, toInt32(1000000) as i, " +
                     "toInt64(10000000000) as l, toFloat32(1.5) as f, toFloat64(2.5) as d, " +
                     "true as bool, 'hello' as str";

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get();
             ClickHouseTextFormatReader reader = client.newTextFormatReader(response)) {

            reader.next();
            Assert.assertEquals(reader.getByte(1), (byte) 120);
            Assert.assertEquals(reader.getShort(2), (short) 30000);
            Assert.assertEquals(reader.getInteger(3), 1000000);
            Assert.assertEquals(reader.getLong(4), 10000000000L);
            Assert.assertEquals(reader.getFloat(5), 1.5f, 0.0001f);
            Assert.assertEquals(reader.getDouble(6), 2.5d, 0.0001d);
            Assert.assertEquals(reader.getBoolean(7), true);
            Assert.assertEquals(reader.getString(8), "hello");
            Assert.assertEquals(reader.getEnum8(1), (byte) 120);
            Assert.assertEquals(reader.getEnum16(2), (short) 30000);
        }
    }

    @Test(groups = {"integration"})
    public void testReadValueAndHasValue() throws Exception {
        String sql = "SELECT 7 as id, 'abc' as name, CAST(NULL AS Nullable(String)) as missing";

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get();
             ClickHouseTextFormatReader reader = client.newTextFormatReader(response)) {

            reader.next();

            Number id = reader.readValue("id");
            Assert.assertNotNull(id);
            Assert.assertEquals(id.intValue(), 7);
            Assert.assertEquals((String) reader.readValue(2), "abc");

            Assert.assertTrue(reader.hasValue("id"));
            Assert.assertTrue(reader.hasValue(2));
            Assert.assertFalse(reader.hasValue("missing"));
            Assert.assertFalse(reader.hasValue(3));
            Assert.assertFalse(reader.hasValue("not_a_column"));
        }
    }

    @Test(groups = {"integration"})
    public void testBigNumberAccessors() throws Exception {
        String sql = "SELECT toInt64(123456789012345) as bi, toDecimal64(12345.6789, 4) as bd";

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get();
             ClickHouseTextFormatReader reader = client.newTextFormatReader(response)) {

            reader.next();
            Assert.assertEquals(reader.getBigInteger("bi"), BigInteger.valueOf(123456789012345L));
            Assert.assertEquals(reader.getBigInteger(1), BigInteger.valueOf(123456789012345L));
            Assert.assertEquals(reader.getBigDecimal("bd").compareTo(new BigDecimal("12345.6789")), 0);
            Assert.assertEquals(reader.getBigDecimal(2).compareTo(new BigDecimal("12345.6789")), 0);
        }
    }

    @Test(groups = {"integration"})
    public void testTemporalAccessors() throws Exception {
        // toDate produces an ISO date string that LocalDate.parse accepts. The
        // reader's getLocalDateTime / getLocalTime / getOffsetDateTime delegate
        // to the JDK's default ISO parsers, so the remaining columns are
        // emitted as strings already shaped to those formats.
        String sql = "SELECT toDate('2024-05-06') as d, " +
                     "'2024-05-06T07:08:09' as dt, " +
                     "'09:10:11' as t, " +
                     "'2024-05-06T07:08:09+02:00' as odt";

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get();
             ClickHouseTextFormatReader reader = client.newTextFormatReader(response)) {

            reader.next();
            Assert.assertEquals(reader.getLocalDate("d"), LocalDate.of(2024, 5, 6));
            Assert.assertEquals(reader.getLocalDate(1), LocalDate.of(2024, 5, 6));
            Assert.assertEquals(reader.getLocalDateTime("dt"),
                    LocalDateTime.of(2024, 5, 6, 7, 8, 9));
            Assert.assertEquals(reader.getLocalDateTime(2),
                    LocalDateTime.of(2024, 5, 6, 7, 8, 9));
            Assert.assertEquals(reader.getLocalTime("t"), LocalTime.of(9, 10, 11));
            Assert.assertEquals(reader.getLocalTime(3), LocalTime.of(9, 10, 11));
            Assert.assertEquals(reader.getOffsetDateTime("odt"),
                    OffsetDateTime.parse("2024-05-06T07:08:09+02:00"));
            Assert.assertEquals(reader.getOffsetDateTime(4),
                    OffsetDateTime.parse("2024-05-06T07:08:09+02:00"));
        }
    }

    @Test(groups = {"integration"})
    public void testUuidAndListAccessors() throws Exception {
        String sql = "SELECT toUUID('11111111-2222-3333-4444-555555555555') as u, " +
                     "[1, 2, 3] as arr";

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get();
             ClickHouseTextFormatReader reader = client.newTextFormatReader(response)) {

            reader.next();
            UUID expected = UUID.fromString("11111111-2222-3333-4444-555555555555");
            Assert.assertEquals(reader.getUUID("u"), expected);
            Assert.assertEquals(reader.getUUID(1), expected);

            List<Number> values = reader.getList("arr");
            Assert.assertNotNull(values);
            Assert.assertEquals(values.size(), 3);
            Assert.assertEquals(values.get(0).intValue(), 1);
            Assert.assertEquals(values.get(1).intValue(), 2);
            Assert.assertEquals(values.get(2).intValue(), 3);

            List<Number> byIndex = reader.getList(2);
            Assert.assertNotNull(byIndex);
            Assert.assertEquals(byIndex.size(), 3);
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
