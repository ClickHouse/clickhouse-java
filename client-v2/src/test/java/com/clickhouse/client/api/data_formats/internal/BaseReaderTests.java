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