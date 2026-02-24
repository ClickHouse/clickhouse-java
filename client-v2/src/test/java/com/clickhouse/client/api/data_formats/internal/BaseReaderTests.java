package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.data.ClickHouseVersion;
import com.clickhouse.data.ClickHouseDataType;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Test(groups = {"integration"})
public class BaseReaderTests extends BaseIntegrationTest {

    private Client client;

    @BeforeMethod(groups = {"integration"})
    public void setUp() {
        client = newClient().build();
    }

    @AfterMethod(groups = {"integration"})
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test(groups = {"integration"})
    public void testReadingLocalDateFromDynamic() throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        final String table = "test_reading_local_date_from_dynamic";
        final LocalDate expectedDate = LocalDate.of(2025, 7, 15);

        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table, "id Int32", "field Dynamic"),
                (CommandSettings) new CommandSettings().serverSetting("allow_experimental_dynamic_type", "1")).get();

        client.execute("INSERT INTO " + table + " VALUES (1, '" + expectedDate + "'::Date)").get();

        // Test with RowBinaryWithNamesAndTypesFormatReader via query()
        try (QueryResponse response = client.query("SELECT * FROM " + table).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            reader.next();

            LocalDate actualDate = reader.getLocalDate("field");
            Assert.assertEquals(actualDate, expectedDate);
        }

        // Test with GenericRecord via queryAll()
        List<GenericRecord> records = client.queryAll("SELECT * FROM " + table);
        Assert.assertEquals(records.size(), 1);
        LocalDate actualDate = records.get(0).getLocalDate("field");
        Assert.assertEquals(actualDate, expectedDate);
    }

    @Test(groups = {"integration"})
    public void testReadingLocalDateTimeFromDynamic() throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        final String table = "test_reading_local_datetime_from_dynamic";
        final LocalDateTime expectedDateTime = LocalDateTime.of(2025, 7, 15, 14, 30, 45);

        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table, "id Int32", "field Dynamic"),
                (CommandSettings) new CommandSettings().serverSetting("allow_experimental_dynamic_type", "1")).get();

        client.execute("INSERT INTO " + table + " VALUES (1, '" + expectedDateTime + "'::DateTime64(3))").get();

        // Test with RowBinaryWithNamesAndTypesFormatReader via query()
        try (QueryResponse response = client.query("SELECT * FROM " + table).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            reader.next();

            LocalDateTime actualDateTime = reader.getLocalDateTime("field");
            Assert.assertEquals(actualDateTime, expectedDateTime);
        }

        // Test with GenericRecord via queryAll()
        List<GenericRecord> records = client.queryAll("SELECT * FROM " + table);
        Assert.assertEquals(records.size(), 1);
        LocalDateTime actualDateTime = records.get(0).getLocalDateTime("field");
        Assert.assertEquals(actualDateTime, expectedDateTime);
    }

    @Test(groups = {"integration"})
    public void testReadingLocalTimeFromDynamic() throws Exception {
        if (isVersionMatch("(,25.5]")) {
            return;
        }

        final String table = "test_reading_local_time_from_dynamic";
        final LocalTime expectedTime = LocalTime.of(14, 30, 45, 123000000);

        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table, "id Int32", "field Dynamic"),
                (CommandSettings) new CommandSettings()
                        .serverSetting("allow_experimental_dynamic_type", "1")
                        .serverSetting("allow_experimental_time_time64_type", "1")).get();

        client.execute("INSERT INTO " + table + " VALUES (1, '14:30:45.123'::Time64(3))",
                (CommandSettings) new CommandSettings().serverSetting("allow_experimental_time_time64_type", "1")).get();

        // Test with RowBinaryWithNamesAndTypesFormatReader via query()
        try (QueryResponse response = client.query("SELECT * FROM " + table).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            reader.next();

            LocalTime actualTime = reader.getLocalTime("field");
            Assert.assertEquals(actualTime, expectedTime);
        }

        // Test with GenericRecord via queryAll()
        List<GenericRecord> records = client.queryAll("SELECT * FROM " + table);
        Assert.assertEquals(records.size(), 1);
        LocalTime actualTime = records.get(0).getLocalTime("field");
        Assert.assertEquals(actualTime, expectedTime);
    }

    @Test(groups = {"integration"})
    public void testReadingZonedDateTimeFromDynamic() throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        final String table = "test_reading_zoned_datetime_from_dynamic";
        final ZoneId zoneId = ZoneId.of("Europe/Berlin");
        final ZonedDateTime expectedZonedDateTime = ZonedDateTime.of(2025, 7, 15, 14, 30, 45, 0, zoneId);

        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table, "id Int32", "field Dynamic"),
                (CommandSettings) new CommandSettings().serverSetting("allow_experimental_dynamic_type", "1")).get();

        client.execute("INSERT INTO " + table + " VALUES (1, '2025-07-15 14:30:45'::DateTime64(3, 'Europe/Berlin'))").get();

        // Test with RowBinaryWithNamesAndTypesFormatReader via query()
        try (QueryResponse response = client.query("SELECT * FROM " + table).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            reader.next();

            ZonedDateTime actualZonedDateTime = reader.getZonedDateTime("field");
            Assert.assertEquals(actualZonedDateTime, expectedZonedDateTime);
        }

        // Test with GenericRecord via queryAll()
        List<GenericRecord> records = client.queryAll("SELECT * FROM " + table);
        Assert.assertEquals(records.size(), 1);
        ZonedDateTime actualZonedDateTime = records.get(0).getZonedDateTime("field");
        Assert.assertEquals(actualZonedDateTime, expectedZonedDateTime);
    }

    @Test(groups = {"integration"})
    public void testReadingInstantFromDynamic() throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        final String table = "test_reading_instant_from_dynamic";
        final Instant expectedInstant = Instant.parse("2025-07-15T12:30:45.123Z");

        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table, "id Int32", "field Dynamic"),
                (CommandSettings) new CommandSettings().serverSetting("allow_experimental_dynamic_type", "1")).get();

        client.execute("INSERT INTO " + table + " VALUES (1, '2025-07-15 12:30:45.123'::DateTime64(3, 'UTC'))").get();

        // Test with RowBinaryWithNamesAndTypesFormatReader via query()
        try (QueryResponse response = client.query("SELECT * FROM " + table).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            reader.next();

            Instant actualInstant = reader.getInstant("field");
            Assert.assertEquals(actualInstant, expectedInstant);
        }

        // Test with GenericRecord via queryAll()
        List<GenericRecord> records = client.queryAll("SELECT * FROM " + table);
        Assert.assertEquals(records.size(), 1);
        Instant actualInstant = records.get(0).getInstant("field");
        Assert.assertEquals(actualInstant, expectedInstant);
    }

    @Test(groups = {"integration"})
    public void testReadingOffsetDateTimeFromDynamic() throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        final String table = "test_reading_offset_datetime_from_dynamic";
        final OffsetDateTime expectedOffsetDateTime = OffsetDateTime.of(2025, 7, 15, 14, 30, 45, 0, ZoneOffset.ofHours(2));

        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table, "id Int32", "field Dynamic"),
                (CommandSettings) new CommandSettings().serverSetting("allow_experimental_dynamic_type", "1")).get();

        client.execute("INSERT INTO " + table + " VALUES (1, '2025-07-15 14:30:45'::DateTime64(3, 'Europe/Berlin'))").get();

        // Test with RowBinaryWithNamesAndTypesFormatReader via query()
        try (QueryResponse response = client.query("SELECT * FROM " + table).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            reader.next();

            OffsetDateTime actualOffsetDateTime = reader.getOffsetDateTime("field");
            Assert.assertEquals(actualOffsetDateTime, expectedOffsetDateTime);
        }

        // Test with GenericRecord via queryAll()
        List<GenericRecord> records = client.queryAll("SELECT * FROM " + table);
        Assert.assertEquals(records.size(), 1);
        OffsetDateTime actualOffsetDateTime = records.get(0).getOffsetDateTime("field");
        Assert.assertEquals(actualOffsetDateTime, expectedOffsetDateTime);
    }   
    
    
    @Test(groups = {"integration"})
    public void testReadingLocalDateFromVariant() throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        final String table = "test_reading_local_date_from_variant";
        final LocalDate expectedDate = LocalDate.of(2025, 7, 15);

        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table, "id Int32", "field Variant(Date, String)"),
                (CommandSettings) new CommandSettings().serverSetting("allow_experimental_variant_type", "1")).get();

        client.execute("INSERT INTO " + table + " VALUES (1, '" + expectedDate + "'::Date)").get();

        // Test with RowBinaryWithNamesAndTypesFormatReader via query()
        try (QueryResponse response = client.query("SELECT * FROM " + table).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            reader.next();

            LocalDate actualDate = reader.getLocalDate("field");
            Assert.assertEquals(actualDate, expectedDate);
        }

        // Test with GenericRecord via queryAll()
        List<GenericRecord> records = client.queryAll("SELECT * FROM " + table);
        Assert.assertEquals(records.size(), 1);
        LocalDate actualDate = records.get(0).getLocalDate("field");
        Assert.assertEquals(actualDate, expectedDate);
    }

    @Test(groups = {"integration"})
    public void testReadingLocalDateTimeFromVariant() throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        final String table = "test_reading_local_datetime_from_variant";
        final LocalDateTime expectedDateTime = LocalDateTime.of(2025, 7, 15, 14, 30, 45);

        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table, "id Int32", "field Variant(DateTime64(3), String)"),
                (CommandSettings) new CommandSettings().serverSetting("allow_experimental_variant_type", "1")).get();

        client.execute("INSERT INTO " + table + " VALUES (1, '" + expectedDateTime + "'::DateTime64(3))").get();

        // Test with RowBinaryWithNamesAndTypesFormatReader via query()
        try (QueryResponse response = client.query("SELECT * FROM " + table).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            reader.next();

            LocalDateTime actualDateTime = reader.getLocalDateTime("field");
            Assert.assertEquals(actualDateTime, expectedDateTime);
        }

        // Test with GenericRecord via queryAll()
        List<GenericRecord> records = client.queryAll("SELECT * FROM " + table);
        Assert.assertEquals(records.size(), 1);
        LocalDateTime actualDateTime = records.get(0).getLocalDateTime("field");
        Assert.assertEquals(actualDateTime, expectedDateTime);
    }

    @Test(groups = {"integration"})
    public void testReadingLocalTimeFromVariant() throws Exception {
        if (isVersionMatch("(,25.5]")) {
            return;
        }

        final String table = "test_reading_local_time_from_variant";
        final LocalTime expectedTime = LocalTime.of(14, 30, 45, 123000000);

        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table, "id Int32", "field Variant(Time64(3), String)"),
                (CommandSettings) new CommandSettings()
                        .serverSetting("allow_experimental_variant_type", "1")
                        .serverSetting("allow_experimental_time_time64_type", "1")).get();

        client.execute("INSERT INTO " + table + " VALUES (1, '14:30:45.123'::Time64(3))", (CommandSettings) new CommandSettings()
                .serverSetting("allow_experimental_time_time64_type", "1")).get();

        // Test with RowBinaryWithNamesAndTypesFormatReader via query()
        try (QueryResponse response = client.query("SELECT * FROM " + table).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            reader.next();

            LocalTime actualTime = reader.getLocalTime("field");
            Assert.assertEquals(actualTime, expectedTime);
        }

        // Test with GenericRecord via queryAll()
        List<GenericRecord> records = client.queryAll("SELECT * FROM " + table);
        Assert.assertEquals(records.size(), 1);
        LocalTime actualTime = records.get(0).getLocalTime("field");
        Assert.assertEquals(actualTime, expectedTime);
    }

    @Test(groups = {"integration"})
    public void testReadingZonedDateTimeFromVariant() throws Exception {
        if (isVersionMatch("(,25.3]")) {
            return;
        }

        final String table = "test_reading_zoned_datetime_from_variant";
        final ZoneId zoneId = ZoneId.of("Europe/Berlin");
        final ZonedDateTime expectedZonedDateTime = ZonedDateTime.of(2025, 7, 15, 14, 30, 45, 0, zoneId);

        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table, "id Int32", "field Variant(DateTime64(3, 'Europe/Berlin'), String)"),
                (CommandSettings) new CommandSettings().serverSetting("allow_experimental_variant_type", "1")
                        .serverSetting("allow_experimental_time_time64_type", "1")).get();

        client.execute("INSERT INTO " + table + " VALUES (1, '2025-07-15 14:30:45'::DateTime64(3, 'Europe/Berlin'))").get();

        // Test with RowBinaryWithNamesAndTypesFormatReader via query()
        try (QueryResponse response = client.query("SELECT * FROM " + table).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            reader.next();

            ZonedDateTime actualZonedDateTime = reader.getZonedDateTime("field");
            Assert.assertEquals(actualZonedDateTime, expectedZonedDateTime);
        }

        // Test with GenericRecord via queryAll()
        List<GenericRecord> records = client.queryAll("SELECT * FROM " + table);
        Assert.assertEquals(records.size(), 1);
        ZonedDateTime actualZonedDateTime = records.get(0).getZonedDateTime("field");
        Assert.assertEquals(actualZonedDateTime, expectedZonedDateTime);
    }

    @Test(groups = {"integration"})
    public void testReadingInstantFromVariant() throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        final String table = "test_reading_instant_from_variant";
        final Instant expectedInstant = Instant.parse("2025-07-15T12:30:45.123Z");

        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table, "id Int32", "field Variant(DateTime64(3, 'UTC'), String)"),
                (CommandSettings) new CommandSettings().serverSetting("allow_experimental_variant_type", "1")).get();

        client.execute("INSERT INTO " + table + " VALUES (1, '2025-07-15 12:30:45.123'::DateTime64(3, 'UTC'))").get();

        // Test with RowBinaryWithNamesAndTypesFormatReader via query()
        try (QueryResponse response = client.query("SELECT * FROM " + table).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            reader.next();

            Instant actualInstant = reader.getInstant("field");
            Assert.assertEquals(actualInstant, expectedInstant);
        }

        // Test with GenericRecord via queryAll()
        List<GenericRecord> records = client.queryAll("SELECT * FROM " + table);
        Assert.assertEquals(records.size(), 1);
        Instant actualInstant = records.get(0).getInstant("field");
        Assert.assertEquals(actualInstant, expectedInstant);
    }

    @Test(groups = {"integration"})
    public void testReadingOffsetDateTimeFromVariant() throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        final String table = "test_reading_offset_datetime_from_variant";
        final OffsetDateTime expectedOffsetDateTime = OffsetDateTime.of(2025, 7, 15, 14, 30, 45, 0, ZoneOffset.ofHours(2));

        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table, "id Int32", "field Variant(DateTime64(3, 'Europe/Berlin'), String)"),
                (CommandSettings) new CommandSettings().serverSetting("allow_experimental_variant_type", "1")).get();

        client.execute("INSERT INTO " + table + " VALUES (1, '2025-07-15 14:30:45'::DateTime64(3, 'Europe/Berlin'))").get();

        // Test with RowBinaryWithNamesAndTypesFormatReader via query()
        try (QueryResponse response = client.query("SELECT * FROM " + table).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            reader.next();

            OffsetDateTime actualOffsetDateTime = reader.getOffsetDateTime("field");
            Assert.assertEquals(actualOffsetDateTime, expectedOffsetDateTime);
        }

        // Test with GenericRecord via queryAll()
        List<GenericRecord> records = client.queryAll("SELECT * FROM " + table);
        Assert.assertEquals(records.size(), 1);
        OffsetDateTime actualOffsetDateTime = records.get(0).getOffsetDateTime("field");
        Assert.assertEquals(actualOffsetDateTime, expectedOffsetDateTime);
    }

    @Test(groups = {"integration"})
    public void testGetObjectArrayWithNullableElements() throws Exception {
        final String table = "test_get_object_array_with_nullable_elements";
        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table,
                "id Int32",
                "arr_nullable Array(Nullable(Int32))",
                "arr2d_nullable Array(Array(Nullable(Int32)))")).get();

        client.execute("INSERT INTO " + table + " VALUES (1, [1, NULL, 2], [[1, NULL], [NULL, 3]])").get();

        try (QueryResponse response = client.query("SELECT * FROM " + table).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            Assert.assertNotNull(reader.next());

            Object[] arrNullable = reader.getObjectArray("arr_nullable");
            Assert.assertNotNull(arrNullable);
            Assert.assertEquals(arrNullable.length, 3);
            Assert.assertEquals(arrNullable[0], 1);
            Assert.assertNull(arrNullable[1]);
            Assert.assertEquals(arrNullable[2], 2);

            Object[] arr2dNullable = reader.getObjectArray("arr2d_nullable");
            Assert.assertNotNull(arr2dNullable);
            Assert.assertEquals(arr2dNullable.length, 2);
            Assert.assertTrue(arr2dNullable[0] instanceof Object[]);
            Assert.assertTrue(arr2dNullable[1] instanceof Object[]);

            Object[] inner0 = (Object[]) arr2dNullable[0];
            Assert.assertEquals(inner0.length, 2);
            Assert.assertEquals(inner0[0], 1);
            Assert.assertNull(inner0[1]);

            Object[] inner1 = (Object[]) arr2dNullable[1];
            Assert.assertEquals(inner1.length, 2);
            Assert.assertNull(inner1[0]);
            Assert.assertEquals(inner1[1], 3);
        }

        List<GenericRecord> records = client.queryAll("SELECT * FROM " + table);
        Assert.assertEquals(records.size(), 1);
        GenericRecord record = records.get(0);

        Object[] arrNullableRecord = record.getObjectArray("arr_nullable");
        Assert.assertNotNull(arrNullableRecord);
        Assert.assertEquals(arrNullableRecord.length, 3);
        Assert.assertEquals(arrNullableRecord[0], 1);
        Assert.assertNull(arrNullableRecord[1]);
        Assert.assertEquals(arrNullableRecord[2], 2);

        Object[] arr2dNullableRecord = record.getObjectArray("arr2d_nullable");
        Assert.assertNotNull(arr2dNullableRecord);
        Assert.assertEquals(arr2dNullableRecord.length, 2);

        Object[] innerRecord0 = (Object[]) arr2dNullableRecord[0];
        Assert.assertEquals(innerRecord0.length, 2);
        Assert.assertEquals(innerRecord0[0], 1);
        Assert.assertNull(innerRecord0[1]);

        Object[] innerRecord1 = (Object[]) arr2dNullableRecord[1];
        Assert.assertEquals(innerRecord1.length, 2);
        Assert.assertNull(innerRecord1[0]);
        Assert.assertEquals(innerRecord1[1], 3);
    }

    @Test(groups = {"integration"})
    public void testGetObjectArrayWhenValueIsList() throws Exception {
        final String table = "test_get_object_array_when_value_is_list";
        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table,
                "id Int32",
                "arr Array(Int32)",
                "arr2d Array(Array(Int32))")).get();
        client.execute("INSERT INTO " + table + " VALUES (1, [10, 20, 30], [[1, 2], [3]])").get();

        try (Client listClient = newClient()
                .typeHintMapping(Collections.singletonMap(ClickHouseDataType.Array, Object.class))
                .build()) {
            try (QueryResponse response = listClient.query("SELECT * FROM " + table).get()) {
                ClickHouseBinaryFormatReader reader = listClient.newBinaryFormatReader(response);
                Assert.assertNotNull(reader.next());

                Object[] arr = reader.getObjectArray("arr");
                Assert.assertNotNull(arr);
                Assert.assertEquals(arr.length, 3);
                Assert.assertEquals(arr[0], 10);
                Assert.assertEquals(arr[1], 20);
                Assert.assertEquals(arr[2], 30);

                Object[] arr2d = reader.getObjectArray("arr2d");
                Assert.assertNotNull(arr2d);
                Assert.assertEquals(arr2d.length, 2);
                Assert.assertTrue(arr2d[0] instanceof List<?>);
                Assert.assertTrue(arr2d[1] instanceof List<?>);
                Assert.assertEquals((List<?>) arr2d[0], Arrays.asList(1, 2));
                Assert.assertEquals((List<?>) arr2d[1], Collections.singletonList(3));
            }

            List<GenericRecord> records = listClient.queryAll("SELECT * FROM " + table);
            Assert.assertEquals(records.size(), 1);

            Object[] arrRecord = records.get(0).getObjectArray("arr");
            Assert.assertNotNull(arrRecord);
            Assert.assertEquals(arrRecord.length, 3);
            Assert.assertEquals(arrRecord[0], 10);
            Assert.assertEquals(arrRecord[1], 20);
            Assert.assertEquals(arrRecord[2], 30);

            Object[] arr2dRecord = records.get(0).getObjectArray("arr2d");
            Assert.assertNotNull(arr2dRecord);
            Assert.assertEquals(arr2dRecord.length, 2);
            Assert.assertTrue(arr2dRecord[0] instanceof List<?>);
            Assert.assertTrue(arr2dRecord[1] instanceof List<?>);
            Assert.assertEquals((List<?>) arr2dRecord[0], Arrays.asList(1, 2));
            Assert.assertEquals((List<?>) arr2dRecord[1], Collections.singletonList(3));
        }
    }


    public static String tableDefinition(String table, String... columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE " + table + " ( ");
        Arrays.stream(columns).forEach(s -> {
            sb.append(s).append(", ");
        });
        sb.setLength(sb.length() - 2);
        sb.append(") Engine = MergeTree ORDER BY ()");
        return sb.toString();
    }


    private boolean isVersionMatch(String versionExpression) {
        List<GenericRecord> serverVersion = client.queryAll("SELECT version()");
        return ClickHouseVersion.of(serverVersion.get(0).getString(1)).check(versionExpression);
    }

    private Client.Builder newClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword());
    }

}