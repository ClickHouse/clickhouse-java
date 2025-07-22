/*
 * Copyright (c) 2025 Riege Software. All rights reserved.
 * Use is subject to license terms.
 */
package com.clickhouse.client.e2e;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.DataTypeUtils;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.data.ClickHouseDataType;

public class ParameterizedQueryTest extends BaseIntegrationTest {

    private Client client;

    @BeforeMethod(groups = {"integration"})
    public void setUp() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = newClient().build();
    }

    @AfterMethod(groups = {"integration"})
    public void tearDown() {
        client.close();
    }

    @Test(groups = {"integration"})
    void testParamsWithInstant() throws Exception {
        String tableName = "test_dt64_instant";
        ZoneId tzBER = ZoneId.of("Europe/Berlin");
        ZoneId tzLAX = ZoneId.of("America/Los_Angeles");
        LocalDateTime localDateTime = LocalDateTime.of(2025, 7, 20, 5, 5, 42, 232323232);
        List<ZonedDateTime> testValues = Arrays.asList(
            localDateTime.atZone(tzBER),
            localDateTime.atZone(tzLAX));

        AtomicInteger rowId = new AtomicInteger(-1);

        // Insert two rows via helper method

        prepareDataSet(
            tableName,
            Arrays.asList(
                "id UInt16",
                "d Date",
                "dt DateTime",
                "dt64_3 DateTime64(3)",
                "dt64_9 DateTime64(9)",
                "dt64_3_lax DateTime64(3, 'America/Los_Angeles')"),
            Arrays.asList(
                () -> Integer.valueOf(rowId.incrementAndGet() % 2),
                () -> testValues.get(rowId.get() % 2).toLocalDate()
                    .toString(),
                () -> DataTypeUtils.format(
                    testValues.get(rowId.get() % 2).toInstant(),
                    ClickHouseDataType.DateTime),
                () -> DataTypeUtils.format(
                    testValues.get(rowId.get() % 2).toInstant()),
                () -> DataTypeUtils.format(
                    testValues.get(rowId.get() % 2).toInstant()),
                () -> DataTypeUtils.format(
                    testValues.get(rowId.get() % 2).toInstant())),
            2);

        // Insert one row using query parameters

        ZoneId tzUTC = ZoneId.of("UTC");
        ZoneId tzServer = ZoneId.of(client.getServerTimeZone());
        Instant manualTestValue = localDateTime.atZone(tzUTC).toInstant();
        client.query(
            "INSERT INTO " + tableName + " (id, d, dt, dt64_3, dt64_9, dt64_3_lax) "
                + "VALUES ("
                + rowId.incrementAndGet() + ", "
                + "'" + DataTypeUtils.format(manualTestValue, ClickHouseDataType.Date, tzUTC) + "', "
                + "'" + DataTypeUtils.format(manualTestValue, ClickHouseDataType.DateTime) + "', "
                + "{manualTestValue:DateTime64}, "
                + "{manualTestValue:DateTime64(9)}, "
                + "{manualTestValue:DateTime64})",
            Collections.singletonMap("manualTestValue", manualTestValue));

        try (QueryResponse response = client.query(
                "SELECT * FROM " + tableName + " ORDER by id ASC").get();
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response))
        {
            reader.next();
            Assert.assertEquals(
                reader.getLocalDate(2),
                localDateTime.toLocalDate());
            Assert.assertEquals(
                reader.getZonedDateTime(3),
                localDateTime.atZone(tzBER).withZoneSameInstant(tzServer)
                    .truncatedTo(ChronoUnit.SECONDS));
            Assert.assertEquals(
                reader.getZonedDateTime(4),
                localDateTime.atZone(tzBER).withZoneSameInstant(tzServer)
                .truncatedTo(ChronoUnit.MILLIS));
            Assert.assertEquals(
                reader.getZonedDateTime(5),
                localDateTime.atZone(tzBER).withZoneSameInstant(tzServer));
            Assert.assertEquals(
                reader.getZonedDateTime(6),
                localDateTime.atZone(tzBER).withZoneSameInstant(tzLAX)
                .truncatedTo(ChronoUnit.MILLIS));

            reader.next();
            Assert.assertEquals(
                reader.getLocalDate(2),
                localDateTime.toLocalDate());
            Assert.assertEquals(
                reader.getZonedDateTime(3),
                localDateTime.atZone(tzLAX).withZoneSameInstant(tzServer)
                    .truncatedTo(ChronoUnit.SECONDS));
            Assert.assertEquals(
                reader.getZonedDateTime(4),
                localDateTime.atZone(tzLAX).withZoneSameInstant(tzServer)
                .truncatedTo(ChronoUnit.MILLIS));
            Assert.assertEquals(
                reader.getZonedDateTime(5),
                localDateTime.atZone(tzLAX).withZoneSameInstant(tzServer));
            Assert.assertEquals(
                reader.getZonedDateTime(6),
                localDateTime.atZone(tzLAX).truncatedTo(ChronoUnit.MILLIS));

            reader.next();
            Assert.assertEquals(
                reader.getLocalDate(2),
                localDateTime.toLocalDate());
            Assert.assertEquals(
                reader.getZonedDateTime(3),
                localDateTime.atZone(tzUTC).truncatedTo(ChronoUnit.SECONDS));
            Assert.assertEquals(
                reader.getZonedDateTime(4),
                localDateTime.atZone(tzServer).truncatedTo(ChronoUnit.MILLIS));
            Assert.assertEquals(
                reader.getZonedDateTime(5),
                localDateTime.atZone(tzServer));
            Assert.assertEquals(
                reader.getZonedDateTime(6),
                localDateTime.atZone(tzServer).withZoneSameInstant(tzLAX)
                .truncatedTo(ChronoUnit.MILLIS));
        }

        // test some queries with parameters

        List<Object[]> queryTests = Arrays.<Object[]>asList(new Object[][]{
            // date column
            { "d", "=", Instant.now(), -1, new int[0] },
            { "d", "=", testValues.get(1).toInstant(), -1, new int[0] },
            { "d", "=", testValues.get(1).toInstant().truncatedTo(ChronoUnit.DAYS),
                -1, new int[] { 0, 1, 2 } },
            { "d", "<", testValues.get(1).toInstant(),
                -1, new int[] { 0, 1, 2 } },
            { "d", ">", testValues.get(1).toInstant(), -1, new int[0] },

            // datetime column
            { "dt", "=", Instant.now(), -1, new int[0] },
            { "dt", "=", testValues.get(1).toInstant(), -1, new int[0] },
            { "dt", "=", testValues.get(1).toInstant().truncatedTo(ChronoUnit.SECONDS),
                -1, new int[] { 1 } },
            { "dt", "<", testValues.get(1).toInstant(), -1, new int[] { 0, 1, 2 } },
            { "dt", ">", testValues.get(1).toInstant(), -1, new int[0] },

            // dt63_3 column
            { "dt64_3", "=", Instant.now(), -1, new int[0] },
            { "dt64_3", "=", testValues.get(1).toInstant(), -1, new int[]{ 1 } },
            { "dt64_3", "=", testValues.get(1).toInstant().truncatedTo(ChronoUnit.MILLIS),
                -1, new int[]{ 1 } },
            { "dt64_3", "<", testValues.get(1).toInstant(),
                -1, new int[] { 0, 2 } },
            { "dt64_3", ">", testValues.get(1).toInstant(), -1, new int[0] },

            // dt63_9 column
            { "dt64_9", "=", Instant.now(), 9, new int[0] },
            { "dt64_9", "=", testValues.get(1).toInstant(), 9,
                new int[]{ 1 } },
            { "dt64_9", "=", testValues.get(1).toInstant().truncatedTo(ChronoUnit.MILLIS),
                9, new int[0] },
            { "dt64_9", "<", testValues.get(1).toInstant(), 9, new int[] { 0, 2 } },
            { "dt64_9", ">", testValues.get(1).toInstant(), 9, new int[0] },

            // dt63_3_lax column
            { "dt64_3_lax", "=", Instant.now(), -1, new int[0] },
            { "dt64_3_lax", "=", testValues.get(1).toInstant(), -1,
                new int[]{ 1 } },
            { "dt64_3_lax", "=", testValues.get(1).toInstant().truncatedTo(ChronoUnit.MILLIS),
                -1, new int[]{ 1 } },
            { "dt64_3_lax", "<", testValues.get(1).toInstant(),
                -1,  new int[] { 0, 2 } },
            { "dt64_3_lax", ">", testValues.get(1).toInstant(), -1, new int[0] },

        });
        for (Object[] queryTest : queryTests) {
            Assert.assertEquals(
                queryInstant(
                    tableName,
                    (String) queryTest[0],
                    (String) queryTest[1],
                    (Instant) queryTest[2],
                    ((Integer) queryTest[3]).intValue()),
                queryTest[4],
                "Test: " + (String) queryTest[0] + " " + (String) queryTest[1] + " "
                    + queryTest[2]);
        }
    }

    @Test(dataProvider = "stringParameters")
    void testStringParams(String paramValue) throws Exception {
        String table = "test_params_unicode";
        String column = "val";
        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute("CREATE TABLE " + table + "(" + column + " String) Engine = Memory").get();
        client.query(
            "INSERT INTO " + table + "(" + column + ") VALUES ('" + paramValue + "')").get();
        try (QueryResponse r = client.query(
            "SELECT " + column + " FROM " + table + " WHERE " + column + "='" + paramValue + "'").get();
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(r))
        {
            reader.next();
            Assert.assertEquals(reader.getString(1), paramValue);
        }
        try (QueryResponse r = client.query(
            "SELECT " + column + " FROM " + table + " WHERE " + column + "={x:String}",
            Collections.singletonMap("x", paramValue)).get();
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(r))
        {
            reader.next();
            Assert.assertEquals(reader.getString(1), paramValue);
        }
    }

    private int[] queryInstant(String tableName, String fieldName, String operator,
        Instant param, int scale) throws InterruptedException, ExecutionException, Exception
    {
        try (QueryResponse response = client.query(
                "SELECT id FROM " + tableName + " WHERE " + fieldName + " "
                    + operator + " {x:DateTime64" + (scale > 0 ? "(" + scale  + ")" : "") + "} "
                    + "ORDER by id ASC",
                Collections.singletonMap("x", param)).get();
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response))
        {
            List<Integer> ints = new ArrayList<>(3);
            while (reader.hasNext()) {
                reader.next();
                ints.add(Integer.valueOf(reader.getInteger(1)));
            }
            return ints.stream().mapToInt(Integer::intValue).toArray();
        }
    }

    private Client.Builder newClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        return new Client.Builder()
            .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
            .setUsername("default")
            .setPassword(ClickHouseServerForTest.getPassword())
            .compressClientRequest(false)
            .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
            .serverSetting(ServerSettings.WAIT_ASYNC_INSERT, "1")
            .serverSetting(ServerSettings.ASYNC_INSERT, "0");
    }

    private void prepareDataSet(String table, List<String> columns, List<Supplier<Object>> valueGenerators,
        int rows)
    {
        List<Map<String, Object>> data = new ArrayList<>(rows);
        try {
            // Drop table
            client.execute("DROP TABLE IF EXISTS " + table).get(10, TimeUnit.SECONDS);

            // Create table
            CommandSettings settings = new CommandSettings();
            StringBuilder createStmtBuilder = new StringBuilder();
            createStmtBuilder.append("CREATE TABLE IF NOT EXISTS ").append(table).append(" (");
            for (String column : columns) {
                createStmtBuilder.append(column).append(", ");
            }
            createStmtBuilder.setLength(createStmtBuilder.length() - 2);
            createStmtBuilder.append(") ENGINE = MergeTree ORDER BY tuple()");
            client.execute(createStmtBuilder.toString(), settings).get(10, TimeUnit.SECONDS);

            // Insert data
            StringBuilder insertStmtBuilder = new StringBuilder();
            insertStmtBuilder.append("INSERT INTO ").append(table).append(" VALUES ");
            for (int i = 0; i < rows; i++) {
                insertStmtBuilder.append("(");
                Map<String, Object> values = writeValuesRow(insertStmtBuilder, columns, valueGenerators);
                insertStmtBuilder.setLength(insertStmtBuilder.length() - 2);
                insertStmtBuilder.append("), ");
                data.add(values);
            }
            insertStmtBuilder.setLength(insertStmtBuilder.length() - 2);
            String s = insertStmtBuilder.toString();
            client.execute(s).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            Assert.fail("failed to prepare data set", e);
        }
    }

    private Map<String, Object> writeValuesRow(StringBuilder insertStmtBuilder, List<String> columns,
        List<Supplier<Object>> valueGenerators)
    {
        Map<String, Object> values = new HashMap<>();
        Iterator<String> columnIterator = columns.iterator();
        for (Supplier<Object> valueGenerator : valueGenerators) {
            Object value = valueGenerator.get();
            if (value instanceof String) {
                insertStmtBuilder.append('\'').append(value).append('\'').append(", ");
            } else {
                insertStmtBuilder.append(value).append(", ");
            }
            values.put(columnIterator.next().split(" ")[0], value);

        }
        return values;
    }

    @DataProvider(name = "stringParameters")
    private static Object[][] createStringParameterValues() {
        return new Object[][] {
            { "foo" },
            { "with-dashes" },
            { "☺" },
            { "foo/bar" },
            { "foobar 20" },
            { " leading_and_trailing_spaces   " },
            { "multi\nline\r\ndos" },
            { "nicely\"quoted\'string\'" },
        };
    }

}
