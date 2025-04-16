package com.clickhouse.client.datatypes;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseVersion;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

public class RowBinaryFormatWriterTest extends BaseIntegrationTest {
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

    protected void initTable(String tableName, String createTableSQL, CommandSettings settings) throws Exception {
        if (settings == null) {
            settings = new CommandSettings();
        }

        if (isVersionMatch("[24.8,)")) {
            settings.serverSetting("allow_experimental_variant_type", "1")
                    .serverSetting("allow_experimental_dynamic_type", "1")
                    .serverSetting("allow_experimental_json_type", "1");
        }

        client.execute("DROP TABLE IF EXISTS " + tableName, settings).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS);
        client.execute(createTableSQL, settings).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS);
    }

    private static void assertEqualsKinda(Object actual, Object expected) {
        if (actual instanceof ZonedDateTime) {
            actual = ((ZonedDateTime) actual).toInstant().getEpochSecond();
        }

        if (expected instanceof ZonedDateTime) {
            expected = ((ZonedDateTime) expected).toInstant().getEpochSecond();
        }

        if (actual instanceof Object[]) {
            actual = Arrays.asList((Object[]) actual);
        }

        if (expected instanceof Object[]) {
            expected = Arrays.asList((Object[]) expected);
        }

        if (actual instanceof BinaryStreamReader.ArrayValue) {
            actual = ((BinaryStreamReader.ArrayValue) actual).asList();
        }

        if (expected instanceof BinaryStreamReader.ArrayValue) {
            expected = ((BinaryStreamReader.ArrayValue) expected).asList();
        }

        if (actual instanceof double[]) {
            List<Double> tempList = new ArrayList<>();
            for (double d : (double[]) actual) {
                tempList.add(d);
            }
            actual = tempList;
        }

        if (expected instanceof double[]) {
            List<Double> tempList = new ArrayList<>();
            for (double d : (double[]) expected) {
                tempList.add(d);
            }
            expected = tempList;
        }

        if (actual instanceof List && expected instanceof List) {
            compareLists((List<?>) actual, (List<?>) expected);
            return;
        }

        if (actual instanceof BigDecimal) {
            actual = ((BigDecimal) actual).stripTrailingZeros();
        }

        if (expected instanceof BigDecimal) {
            expected = ((BigDecimal) expected).stripTrailingZeros();
        }

        assertEquals(String.valueOf(actual), String.valueOf(expected));
    }

    private static void compareLists(List<?> list1, List<?> list2) {
        // Iterate and compare each element
        for (int i = 0; i < list1.size(); i++) {
            Object item1 = list1.get(i);
            Object item2 = list2.get(i);
            if (item1 instanceof List && item2 instanceof List) {
                compareLists((List<?>) item1, (List<?>) item2);
            } else {
                assertEqualsKinda(item1, item2);
            }
        }
    }


    private void writeTest(String tableName, String tableCreate, Field[][] rows) throws Exception {
        initTable(tableName, tableCreate, new CommandSettings());
        TableSchema schema = client.getTableSchema(tableName);

        ClickHouseFormat format = ClickHouseFormat.RowBinaryWithDefaults;
        try (InsertResponse response = client.insert(tableName, out -> {
            RowBinaryFormatWriter w = new RowBinaryFormatWriter(out, schema, format);
            for (Field[] row : rows) {
                for (Field field : row) {
                    w.setValue(schema.nameToColumnIndex(field.name), field.value);
                }
                w.commitRow();
            }
        }, format, settings).get()) {
            System.out.println("Rows written: " + response.getWrittenRows());
        }

        List<GenericRecord> records = client.queryAll("SELECT * FROM \"" + tableName  + "\" ORDER BY id" );

        int id = 1;
        for (GenericRecord record : records) {
            Map<String, Object> row = record.getValues();
            //Validate data
            for (Field field : rows[id - 1]) {
                assertEqualsKinda(row.get(field.name), field.comparisonValue);
            }
            id++;
        }
    }

    private static class Field {
        String name;
        Object value;
        Object comparisonValue;

        Field(String name) {
            this.name = name;
            this.value = null;
            this.comparisonValue = null;
        }

        Field(String name, Object value) {
            this.name = name;
            this.value = value;
            this.comparisonValue = value;
        }

        public Field set(Object comparisonValue) {//For comparison purposes
            this.comparisonValue = comparisonValue;
            return this;
        }
    }





    @Test (groups = { "integration" })
    public void writeMissingFieldsTest() throws Exception {
        String tableName = "rowBinaryFormatWriterTest_writeNumbersTest_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  int8 Int8, int8_nullable Nullable(Int8), int8_default Int8 DEFAULT 3 " +
                "  ) Engine = MergeTree ORDER BY id";

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        System.out.println("Random seed: " + seed);

        Field[][] rows = new Field[][] {{
                new Field("id", 1), //Row ID
                new Field("int8", rand.nextInt(256) - 128)//Missing the nullable and default fields
        }};

        writeTest(tableName, tableCreate, rows);
    }


    @Test (groups = { "integration" })
    public void writeNumbersTest() throws Exception {
        String tableName = "rowBinaryFormatWriterTest_writeNumbersTest_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  int8 Int8, int8_nullable Nullable(Int8), int8_default Int8 DEFAULT 3, " +
                "  int16 Int16, int16_nullable Nullable(Int16), int16_default Int16 DEFAULT 3, " +
                "  int32 Int32, int32_nullable Nullable(Int32), int32_default Int32 DEFAULT 3, " +
                "  int64 Int64, int64_nullable Nullable(Int64), int64_default Int64 DEFAULT 3, " +
                "  int128 Int128, int128_nullable Nullable(Int128), int128_default Int128 DEFAULT 3, " +
                "  int256 Int256, int256_nullable Nullable(Int256), int256_default Int256 DEFAULT 3, " +
                "  uint8 UInt8, uint8_nullable Nullable(UInt8), uint8_default UInt8 DEFAULT 3, " +
                "  uint16 UInt16, uint16_nullable Nullable(UInt16), uint16_default UInt16 DEFAULT 3, " +
                "  uint32 UInt32, uint32_nullable Nullable(UInt32), uint32_default UInt32 DEFAULT 3, " +
                "  uint64 UInt64, uint64_nullable Nullable(UInt64), uint64_default UInt64 DEFAULT 3, " +
                "  uint128 UInt128, uint128_nullable Nullable(UInt128), uint128_default UInt128 DEFAULT 3, " +
                "  uint256 UInt256, uint256_nullable Nullable(UInt256), uint256_default UInt256 DEFAULT 3, " +
                "  float32 Float32, float32_nullable Nullable(Float32), float32_default Float32 DEFAULT 3, " +
                "  float64 Float64, float64_nullable Nullable(Float64), float64_default Float64 DEFAULT 3, " +
//                "  bfloat16 BFloat16, bfloat16_nullable Nullable(BFloat16), bfloat16_default BFloat16 DEFAULT 3, " +
                "  ) Engine = MergeTree ORDER BY id";

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        System.out.println("Random seed: " + seed);

        Field[][] rows = new Field[][] {{
                new Field("id", 1), //Row ID
                new Field("int8", rand.nextInt(256) - 128), new Field("int8_nullable"), new Field("int8_default").set(3), //Int8
                new Field("int16", rand.nextInt(65536) - 32768), new Field("int16_nullable"), new Field("int16_default").set(3), //Int16
                new Field("int32", rand.nextInt()), new Field("int32_nullable"), new Field("int32_default").set(3), //Int32
                new Field("int64", rand.nextLong()), new Field("int64_nullable"), new Field("int64_default").set(3), //Int64
                new Field("int128", new BigInteger(127, rand)), new Field("int128_nullable"), new Field("int128_default").set(3), //Int128
                new Field("int256", new BigInteger(255, rand)), new Field("int256_nullable"), new Field("int256_default").set(3), //Int256
                new Field("uint8", rand.nextInt(256)), new Field("uint8_nullable"), new Field("uint8_default").set(3), //UInt8
                new Field("uint16", rand.nextInt(65536)), new Field("uint16_nullable"), new Field("uint16_default").set(3), //UInt16
                new Field("uint32", rand.nextInt() & 0xFFFFFFFFL), new Field("uint32_nullable"), new Field("uint32_default").set(3), //UInt32
                new Field("uint64", new BigInteger(64, rand)), new Field("uint64_nullable"), new Field("uint64_default").set(3), //UInt64
                new Field("uint128", new BigInteger(128, rand)), new Field("uint128_nullable"), new Field("uint128_default").set(3), //UInt128
                new Field("uint256", new BigInteger(256, rand)), new Field("uint256_nullable"), new Field("uint256_default").set(3), //UInt256
                new Field("float32", rand.nextFloat()), new Field("float32_nullable"), new Field("float32_default").set("3.0"), //Float32
                new Field("float64", rand.nextDouble()), new Field("float64_nullable"), new Field("float64_default").set("3.0"), //Float64
//                new Field("bfloat16", rand.nextDouble()), new Field("bfloat16_nullable"), new Field("bfloat16_default").set("3.0"), //BFloat16
        }};

        writeTest(tableName, tableCreate, rows);
    }


    @Test (groups = { "integration" })
    public void writeDecimalsTest() throws Exception {
        String tableName = "rowBinaryFormatWriterTest_writeNumbersTest_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  decimal Decimal(4, 2), decimal_nullable Nullable(Decimal(4, 2)), decimal_default Decimal(4, 2) DEFAULT 3, " +
                "  decimal32 Decimal(8, 4), decimal32_nullable Nullable(Decimal(8, 4)), decimal32_default Decimal(8, 4) DEFAULT 3, " +
                "  decimal64 Decimal(18, 6), decimal64_nullable Nullable(Decimal(18, 6)), decimal64_default Decimal(18, 6) DEFAULT 3, " +
                "  decimal128 Decimal(36, 8), decimal128_nullable Nullable(Decimal(36, 8)), decimal128_default Decimal(36, 8) DEFAULT 3, " +
                "  decimal256 Decimal(74, 10), decimal256_nullable Nullable(Decimal(74, 10)), decimal256_default Decimal(74, 10) DEFAULT 3" +
                "  ) Engine = MergeTree ORDER BY id";

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        System.out.println("Random seed: " + seed);

        BigDecimal decimal = new BigDecimal(new BigInteger(5, rand) + "." + rand.nextInt(100));
        BigDecimal decimal32 = new BigDecimal(new BigInteger(7, rand) + "." + rand.nextInt(10000));
        BigDecimal decimal64 = new BigDecimal(new BigInteger(18, rand) + "." + rand.nextInt(1000000));
        BigDecimal decimal128 = new BigDecimal(new BigInteger(20, rand) + "." + rand.nextInt(1000000));
        BigDecimal decimal256 = new BigDecimal(new BigInteger(57, rand) + "." + rand.nextInt(1000000));

        Field[][] rows = new Field[][] {{
                new Field("id", 1),
                new Field("decimal", decimal).set(decimal), new Field("decimal_nullable"), new Field("decimal_default").set("3"),  //Decimal(4)
                new Field("decimal32", decimal32).set(decimal32), new Field("decimal32_nullable"), new Field("decimal32_default").set("3"), //Decimal32
                new Field("decimal64", decimal64).set(decimal64), new Field("decimal64_nullable"), new Field("decimal64_default").set("3"), //Decimal64
                new Field("decimal128", decimal128).set(decimal128), new Field("decimal128_nullable"), new Field("decimal128_default").set("3"), //Decimal128
                new Field("decimal256", decimal256).set(decimal256), new Field("decimal256_nullable"), new Field("decimal256_default").set("3") //Decimal256
        }};

        writeTest(tableName, tableCreate, rows);
    }


    @Test (groups = { "integration" })
    public void writeStringsTest() throws Exception {
        String tableName = "rowBinaryFormatWriterTest_writeStringsTests_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  string String, string_nullable Nullable(String), string_default String DEFAULT '3', " +
                "  fixed_string FixedString(10), fixed_string_nullable Nullable(FixedString(10)), fixed_string_default FixedString(10) DEFAULT 'tenletters', " +
                "  uuid UUID, uuid_nullable Nullable(UUID), uuid_default UUID DEFAULT '61f0c404-5cb3-11e7-907b-a6006ad3dba0', " +
                "  enum Enum('a' = 1, 'b' = 2), enum_nullable Nullable(Enum('a' = 1, 'b' = 2)), enum_default Enum('a' = 1, 'b' = 2) DEFAULT 'a', " +
                "  enum8 Enum8('a' = 1, 'b' = 2), enum8_nullable Nullable(Enum8('a' = 1, 'b' = 2)), enum8_default Enum8('a' = 1, 'b' = 2) DEFAULT 'a', " +
                "  enum16 Enum16('a' = 1, 'b' = 2), enum16_nullable Nullable(Enum16('a' = 1, 'b' = 2)), enum16_default Enum16('a' = 1, 'b' = 2) DEFAULT 'a', " +
                "  ) Engine = MergeTree ORDER BY id";

        // Insert random (valid) values
        Field[][] rows = new Field[][] {{
                    new Field("id", 1), //Row ID
                    new Field("string", RandomStringUtils.randomAlphabetic(1024)), new Field("string_nullable"), new Field("string_default").set("3"), //String
                    new Field("fixed_string", RandomStringUtils.randomAlphabetic(10)), new Field("fixed_string_nullable"), new Field("fixed_string_default").set("tenletters"), //FixedString
                    new Field("uuid", UUID.randomUUID()), new Field("uuid_nullable"), new Field("uuid_default").set("61f0c404-5cb3-11e7-907b-a6006ad3dba0"), //UUID
                    new Field("enum", "a"), new Field("enum_nullable"), new Field("enum_default").set("a"), //Enum8
                    new Field("enum8", "a"), new Field("enum8_nullable"), new Field("enum8_default").set("a"), //Enum8
                    new Field("enum16", "b"), new Field("enum16_nullable"), new Field("enum16_default").set("a") //Enum16
                }, {
                    new Field("id", 2), //Row ID
                    new Field("string", RandomStringUtils.randomAlphabetic(1024)), new Field("string_nullable"), new Field("string_default").set("3"), //String
                    new Field("fixed_string", RandomStringUtils.randomAlphabetic(10)), new Field("fixed_string_nullable"), new Field("fixed_string_default").set("tenletters"), //FixedString
                    new Field("uuid", UUID.randomUUID()), new Field("uuid_nullable"), new Field("uuid_default").set("61f0c404-5cb3-11e7-907b-a6006ad3dba0"), //UUID
                    new Field("enum", "a"), new Field("enum_nullable"), new Field("enum_default").set("a"), //Enum8
                    new Field("enum8", "a"), new Field("enum8_nullable"), new Field("enum8_default").set("a"), //Enum8
                    new Field("enum16", "b"), new Field("enum16_nullable"), new Field("enum16_default").set("a") //Enum16
                }
        };

        writeTest(tableName, tableCreate, rows);
    }


    @Test (groups = { "integration" })
    public void writeDatetimeTests() throws Exception {
        String tableName = "rowBinaryFormatWriterTest_writeDatetimeTests_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  datetime DateTime, datetime_nullable Nullable(DateTime), datetime_default DateTime DEFAULT '2020-01-01 00:00:00', " +
                "  datetime32 DateTime32, datetime32_nullable Nullable(DateTime32), datetime32_default DateTime32 DEFAULT '2020-01-01 00:00:00', " +
                "  datetime64 DateTime64, datetime64_nullable Nullable(DateTime64), datetime64_default DateTime64 DEFAULT '2025-01-01 00:00:00', " +
                "  date Date, date_nullable Nullable(Date), date_default Date DEFAULT '2020-01-01', " +
                "  date32 Date32, date32_nullable Nullable(Date32), date32_default Date32 DEFAULT '2025-01-01', " +
                "  ) Engine = MergeTree ORDER BY id";

        // Insert random (valid) values
        Field[][] rows = new Field[][] {{
                    new Field("id", 1), //Row ID
                    new Field("datetime", ZonedDateTime.now()), new Field("datetime_nullable"), new Field("datetime_default").set(ZonedDateTime.parse("2020-01-01T00:00:00+00:00[UTC]")), //DateTime
                    new Field("datetime32", ZonedDateTime.now()), new Field("datetime32_nullable"), new Field("datetime32_default").set(ZonedDateTime.parse("2020-01-01T00:00:00+00:00[UTC]")), //DateTime
                    new Field("datetime64", ZonedDateTime.now()), new Field("datetime64_nullable"), new Field("datetime64_default").set(ZonedDateTime.parse("2025-01-01T00:00:00+00:00[UTC]")), //DateTime64
                    new Field("date", ZonedDateTime.parse("2021-01-01T00:00:00+00:00[UTC]")), new Field("date_nullable"), new Field("date_default").set(ZonedDateTime.parse("2020-01-01T00:00:00+00:00[UTC]").toEpochSecond()), //Date
                    new Field("date32", ZonedDateTime.parse("2021-01-01T00:00:00+00:00[UTC]")), new Field("date32_nullable"), new Field("date32_default").set(ZonedDateTime.parse("2025-01-01T00:00:00+00:00[UTC]").toEpochSecond()) //Date
                }
        };

        writeTest(tableName, tableCreate, rows);
    }

    @Test (groups = { "integration" })
    public void writeTupleTests() throws Exception {
        String tableName = "rowBinaryFormatWriterTest_writeTuplesTests_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  tuple Tuple(Int8, Int16), tuple_default Tuple(Int8, Int16) DEFAULT (3, 4), " +
                "  ) Engine = MergeTree ORDER BY id";

        // Insert random (valid) values
        Field[][] rows = new Field[][] {{
                    new Field("id", 1), //Row ID
                    new Field("tuple", Arrays.asList((byte) 1, (short) 2)).set(new Object[]{(byte) 1, (short) 2}), new Field("tuple_default").set(Arrays.asList((byte) 3, (short) 4)) //Tuple
                }
        };

        writeTest(tableName, tableCreate, rows);
    }

    @Test (groups = { "integration" })
    public void writeIpAddressTests() throws Exception {
        String tableName = "rowBinaryFormatWriterTest_writeIpAddressTests_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  ipv4 IPv4, ipv4_nullable Nullable(IPv4), ipv4_default IPv4 DEFAULT '127.0.0.1', " +
                "  ipv6 IPv6, ipv6_nullable Nullable(IPv6), ipv6_default IPv6 DEFAULT '::1', " +
                "  ) Engine = MergeTree ORDER BY id";

        // Insert random (valid) values
        Field[][] rows = new Field[][] {{
                new Field("id", 1), //Row ID
                new Field("ipv4", Inet4Address.getByName("127.0.0.1")).set(Inet4Address.getByName("127.0.0.1")), new Field("ipv4_nullable"), new Field("ipv4_default").set(Inet4Address.getByName("127.0.0.1")),
                new Field("ipv6", Inet6Address.getByName("::1")).set(Inet6Address.getByName("::1")), new Field("ipv6_nullable"), new Field("ipv6_default").set(Inet6Address.getByName("::1"))
        }};

        writeTest(tableName, tableCreate, rows);
    }

    @Test (groups = { "integration" })
    public void writeArrayTests() throws Exception {
        String tableName = "rowBinaryFormatWriterTest_writeArrayTests_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  array Array(Int8), array_default Array(Int8) DEFAULT [3], " +
                "  ) Engine = MergeTree ORDER BY id";

        // Insert random (valid) values
        Field[][] rows = new Field[][] {{
                    new Field("id", 1), //Row ID
                    new Field("array", Arrays.asList((byte) 1, (byte) 2)).set(new Object[]{(byte) 1, (byte) 2}), new Field("array_default").set(Arrays.asList((byte) 3)) //Array
                }
        };

        writeTest(tableName, tableCreate, rows);
    }

    @Test (groups = { "integration" })
    public void writeGeometryTests() throws Exception {
        if (!isVersionMatch("[24.6,)")) {
            System.out.println("Skipping test: ClickHouse version is not compatible with LINESTRING type");
            return;
        }

        String tableName = "rowBinaryFormatWriterTest_writeGeometryTests_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  point Point, point_default Point DEFAULT (0,0), " +
                "  ring Ring, ring_default Ring DEFAULT [(0, 0), (10, 0), (10, 10), (0, 10)], " +
                "  linestring LineString, linestring_default LineString DEFAULT [(0, 0), (10, 0), (10, 10), (0, 10)], " +
                "  multilinestring MultiLineString, multilinestring_default MultiLineString DEFAULT [[(0, 0), (10, 0), (10, 10), (0, 10)]], " +
                "  polygon Polygon, polygon_default Polygon DEFAULT [[(0, 0), (10, 0), (10, 10), (0, 10)]], " +
                "  multipolygon MultiPolygon, multipolygon_default MultiPolygon DEFAULT [[[(0, 0), (10, 0), (10, 10), (0, 10)]]], " +
                "  ) Engine = MergeTree ORDER BY id";

        // Insert random (valid) values
        Field[][] rows = new Field[][] {{
                new Field("id", 1), //Row ID
                new Field("point", Arrays.asList(1.0, 2.0)).set(Arrays.asList(1.0, 2.0)), new Field("point_default").set(Arrays.asList(0.0, 0.0)),
                new Field("ring", Arrays.asList(Arrays.asList(1.0, 2.0), Arrays.asList(3.0, 4.0))).set(Arrays.asList(Arrays.asList(1.0, 2.0), Arrays.asList(3.0, 4.0))), new Field("ring_default").set(Arrays.asList(Arrays.asList(0.0, 0.0), Arrays.asList(10.0, 0.0), Arrays.asList(10.0, 10.0), Arrays.asList(0.0, 10.0))),
                new Field("linestring", Arrays.asList(Arrays.asList(1.0, 2.0), Arrays.asList(3.0, 4.0))).set(Arrays.asList(Arrays.asList(1.0, 2.0), Arrays.asList(3.0, 4.0))), new Field("linestring_default").set(Arrays.asList(Arrays.asList(0.0, 0.0), Arrays.asList(10.0, 0.0), Arrays.asList(10.0, 10.0), Arrays.asList(0.0, 10.0))),
                new Field("polygon", Arrays.asList(Arrays.asList(Arrays.asList(1.0, 2.0), Arrays.asList(3.0, 4.0)))).set(Arrays.asList(Arrays.asList(Arrays.asList(1.0, 2.0), Arrays.asList(3.0, 4.0)))) , new Field("polygon_default").set(Arrays.asList(Arrays.asList(Arrays.asList(0.0, 0.0), Arrays.asList(10.0, 0.0), Arrays.asList(10.0, 10.0), Arrays.asList(0.0, 10.0)))) ,
                new Field("multilinestring", Arrays.asList(Arrays.asList(Arrays.asList(1.0, 2.0), Arrays.asList(3.0, 4.0)))).set(Arrays.asList(Arrays.asList(Arrays.asList(1.0, 2.0), Arrays.asList(3.0, 4.0)))) , new Field("multilinestring_default").set(Arrays.asList(Arrays.asList(Arrays.asList(0.0, 0.0), Arrays.asList(10.0, 0.0), Arrays.asList(10.0, 10.0), Arrays.asList(0.0, 10.0)))) ,
                new Field("multipolygon", Arrays.asList(Arrays.asList(Arrays.asList(Arrays.asList(1.0, 2.0), Arrays.asList(3.0, 4.0))))).set(Arrays.asList(Arrays.asList(Arrays.asList(Arrays.asList(1.0, 2.0), Arrays.asList(3.0, 4.0))))) , new Field("multipolygon_default").set(Arrays.asList(Arrays.asList(Arrays.asList(Arrays.asList(0.0, 0.0), Arrays.asList(10.0, 0.0), Arrays.asList(10.0, 10.0), Arrays.asList(0.0, 10.0))))),
        }};

        writeTest(tableName, tableCreate, rows);
    }




    @Test (groups = { "integration" })
    public void writeMapTests() throws Exception {
        String tableName = "rowBinaryFormatWriterTest_writeMapTests_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  map Map(String, Int16) " +
                "  ) Engine = MergeTree ORDER BY id";

        Map<String, Integer> tmpMap = new HashMap<>();
        tmpMap.put("a", 1);
        tmpMap.put("b", 2);

        // Insert random (valid) values
        Field[][] rows = new Field[][] {{
                    new Field("id", 1), //Row ID
                    new Field("map", tmpMap).set(tmpMap), //Map
                }
        };

        writeTest(tableName, tableCreate, rows);
    }

    @Test (groups = { "integration" })
    public void writeNestedTests() throws Exception {
        String tableName = "rowBinaryFormatWriterTest_writeNestedTests_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  nested Nested(n1 Int8, n2 Int16) " +
                "  ) Engine = MergeTree ORDER BY id";

        // Insert random (valid) values
        Field[][] rows = new Field[][] {{
                    new Field("id", 1), //Row ID
                    new Field("nested.n1", Arrays.asList(1)).set(Arrays.asList(1)), //Nested
                    new Field("nested.n2", Arrays.asList(2)).set(Arrays.asList(2)), //Nested
                }
        };

        writeTest(tableName, tableCreate, rows);
    }

    @Test (groups = { "integration" })
    public void writeNullableTests() throws Exception {
        String tableName = "rowBinaryFormatWriterTest_writeNullableTests_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  nullable Nullable(Int8), nullable_default Nullable(Int8) DEFAULT 3, " +
                "  ) Engine = MergeTree ORDER BY id";

        // Insert random (valid) values
        Field[][] rows = new Field[][] {{
                    new Field("id", 1), //Row ID
                    new Field("nullable", null).set(null), new Field("nullable_default").set(3) //Nullable
                }
        };

        writeTest(tableName, tableCreate, rows);
    }

    @Test (groups = { "integration" })
    public void writeLowCardinalityTests() throws Exception {
        String tableName = "rowBinaryFormatWriterTest_writeLowCardinalityTests_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  lowcardinality LowCardinality(String), lowcardinality_default LowCardinality(String) DEFAULT '3', " +
                "  ) Engine = MergeTree ORDER BY id";

        String randomString = RandomStringUtils.randomAlphabetic(1024);

        // Insert random (valid) values
        Field[][] rows = new Field[][] {{
                    new Field("id", 1), //Row ID
                    new Field("lowcardinality", randomString).set(randomString), new Field("lowcardinality_default").set("3") //LowCardinality
                }
        };

        writeTest(tableName, tableCreate, rows);
    }

    @Test (groups = { "integration" })
    public void writeBooleanTypeTests() throws Exception {
        String tableName = "rowBinaryFormatWriterTest_writeBooleanTypeTests_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  boolean Bool, boolean_default Bool DEFAULT 1, " +
                "  ) Engine = MergeTree ORDER BY id";

        // Insert random (valid) values
        Field[][] rows = new Field[][] {{
                    new Field("id", 1), //Row ID
                    new Field("boolean", false).set(false), new Field("boolean_default").set(true) //Boolean
                }
        };

        writeTest(tableName, tableCreate, rows);
    }


    //TODO: Do we support this?
    @Test (groups = { "integration" }, enabled = false)
    public void writeAggregateFunctionTests() throws Exception {
        String tableName = "rowBinaryFormatWriterTest_writeAggregateFunctionTests_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  aggregate_function AggregateFunction(count, Int8), " +
                "  ) Engine = MergeTree ORDER BY id";

        // Insert random (valid) values
        Field[][] rows = new Field[][] {{
                    new Field("id", 1), //Row ID
                    new Field("aggregate_function", Arrays.asList((byte) 1)).set(Arrays.asList((byte) 1)), //AggregateFunction
                }
        };

        writeTest(tableName, tableCreate, rows);
    }


    //TODO: Do we support this?
    @Test (groups = { "integration" }, enabled = false)
    public void writeSimpleAggregateFunctionTests() throws Exception {
        String tableName = "rowBinaryFormatWriterTest_writeSimpleAggregateFunctionTests_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  simple_aggregate_function SimpleAggregateFunction(count, Int8), " +
                "  ) Engine = MergeTree ORDER BY id";

        // Insert random (valid) values
        Field[][] rows = new Field[][] {{
                    new Field("id", 1), //Row ID
                    new Field("simple_aggregate_function", Arrays.asList((byte) 1)).set(Arrays.asList((byte) 1)), //SimpleAggregateFunction
                }
        };

        writeTest(tableName, tableCreate, rows);
    }


    //TODO: Currently experimental
    @Test (groups = { "integration" })
    public void writeDynamicTests() throws Exception {
        if (!isVersionMatch("[24.8,)")) {
            System.out.println("Skipping test: ClickHouse version is not compatible with DYNAMIC type");
            return;
        }

        String tableName = "rowBinaryFormatWriterTest_writeDynamicTests_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  dynamic Dynamic " +
                "  ) Engine = MergeTree ORDER BY id";

        // Insert random (valid) values
        Field[][] rows = new Field[][] {{
                    new Field("id", 1), //Row ID
                    new Field("dynamic", Arrays.asList((byte) 1, (short) 2)).set(Arrays.asList((byte) 1, (short) 2)), //Dynamic
                }
        };

        writeTest(tableName, tableCreate, rows);
    }



    //TODO: Currently experimental
    @Test (groups = { "integration" })
    public void writeJsonTests() throws Exception {
        if (!isVersionMatch("[24.10,)")) {
            System.out.println("Skipping test: ClickHouse version is not compatible with JSON type");
            return;
        }

        String tableName = "rowBinaryFormatWriterTest_writeJsonTests_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  json JSON, json_default JSON DEFAULT '{\"a\": 1}' " +
                "  ) Engine = MergeTree ORDER BY id";

        Map<String, Object> tmpMap = new HashMap<>();
        tmpMap.put("a", 1);

        // Insert random (valid) values
        Field[][] rows = new Field[][] {{
                    new Field("id", 1), //Row ID
                    new Field("json", "{\"a\": 1}").set(tmpMap), new Field("json_default").set(tmpMap)//Json
        }};

        writeTest(tableName, tableCreate, rows);
    }


    //TODO: Currently experimental
    @Test (groups = { "integration" })
    public void writeVariantTests() throws Exception {
        if (!isVersionMatch("[24.8,)")) {
            System.out.println("Skipping test: ClickHouse version is not compatible with VARIANT type");
            return;
        }

        String tableName = "rowBinaryFormatWriterTest_writeVariantTests_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (id Int32, " +
                "  variant Variant(String, Int8)," +
                "  ) Engine = MergeTree ORDER BY id";

        // Insert random (valid) values
        Field[][] rows = new Field[][] {{
                    new Field("id", 1), //Row ID
                    new Field("variant", (byte) 1).set((byte) 1), //Variant
                }, {
                new Field("id", 2), //Row ID
                new Field("variant", "hello").set("hello"), //Variant
        }};

        writeTest(tableName, tableCreate, rows);
    }
}
