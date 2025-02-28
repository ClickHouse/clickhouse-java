package com.clickhouse.client.datatypes;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseFormat;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .compressClientRequest(true)
                .useHttpCompression(true)
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .serverSetting(ServerSettings.ASYNC_INSERT, "0")
                .serverSetting(ServerSettings.WAIT_END_OF_QUERY, "1");
    }

    protected void initTable(String tableName, String createTableSQL, CommandSettings settings) throws Exception {
        if (settings == null) {
            settings = new CommandSettings();
        }

        client.execute("DROP TABLE IF EXISTS " + tableName, settings).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS);
        client.execute(createTableSQL, settings).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS);
    }

    private static void assertEqualsKinda(Object actual, Object expected) {
        assertEquals(String.valueOf(actual), String.valueOf(expected));
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
                "  decimal Decimal(4, 2), decimal_nullable Nullable(Decimal(4, 2)), decimal_default Decimal(4, 2) DEFAULT 3, " +
                "  decimal32 Decimal(8, 4), decimal32_nullable Nullable(Decimal(8, 4)), decimal32_default Decimal(8, 4) DEFAULT 3, " +
                "  decimal64 Decimal(18, 6), decimal64_nullable Nullable(Decimal(18, 6)), decimal64_default Decimal(18, 6) DEFAULT 3, " +
                "  decimal128 Decimal(36, 8), decimal128_nullable Nullable(Decimal(36, 8)), decimal128_default Decimal(36, 8) DEFAULT 3, " +
                "  decimal256 Decimal(74, 10), decimal256_nullable Nullable(Decimal(74, 10)), decimal256_default Decimal(74, 10) DEFAULT 3" +
                "  ) Engine = MergeTree ORDER BY id";
        initTable(tableName, tableCreate, new CommandSettings());

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        System.out.println("Random seed: " + seed);

        Object[][] rows = new Object[][] {
                {1,
                        rand.nextInt(256) - 128, null, null, //Int8
                        rand.nextInt(65536) - 32768, null, null, //Int16
                        rand.nextInt(), null, null, //Int32
                        rand.nextLong(), null, null, //Int64
                        new BigInteger(127, rand), null, null, //Int128
                        new BigInteger(255, rand), null, null, //Int256
                        rand.nextInt(256), null, null, //UInt8
                        rand.nextInt(65536), null, null, //UInt16
                        rand.nextInt() & 0xFFFFFFFFL, null, null, //UInt32
                        BigInteger.valueOf(rand.nextLong(Long.MAX_VALUE)), null, null, //UInt64
                        new BigInteger(128, rand), null, null, //UInt128
                        new BigInteger(256, rand), null, null, //UInt256
                        rand.nextFloat(), null, null, //Float32
                        rand.nextDouble(), null, null, //Float64
                        new BigDecimal(new BigInteger(7, rand) + "." + rand.nextInt(10,100)), null, null,  //Decimal(4)
                        new BigDecimal(new BigInteger(5, rand) + "." + rand.nextInt(1000, 10000)), null, null, //Decimal32
                        new BigDecimal(new BigInteger(18, rand) + "." + rand.nextInt(100000, 1000000)), null, null, //Decimal64
                        new BigDecimal(new BigInteger(20, rand) + "." + rand.nextLong(10000000, 100000000)), null, null, //Decimal128
                        new BigDecimal(new BigInteger(57, rand) + "." + rand.nextLong(1000000000, 10000000000L)), null, null //Decimal256
                },
                {2,
                        rand.nextInt(256) - 128, null, null, //Int8
                        rand.nextInt(65536) - 32768, null, null, //Int16
                        rand.nextInt(), null, null, //Int32
                        rand.nextLong(), null, null, //Int64
                        new BigInteger(127, rand), null, null, //Int128
                        new BigInteger(255, rand), null, null, //Int256
                        rand.nextInt(256), null, null, //UInt8
                        rand.nextInt(65536), null, null, //UInt16
                        rand.nextInt() & 0xFFFFFFFFL, null, null, //UInt32
                        BigInteger.valueOf(rand.nextLong(Long.MAX_VALUE)), null, null, //UInt64
                        new BigInteger(128, rand), null, null, //UInt128
                        new BigInteger(256, rand), null, null, //UInt256
                        rand.nextFloat(), null, null, //Float32
                        rand.nextDouble(), null, null, //Float64
                        new BigDecimal(new BigInteger(7, rand) + "." + rand.nextInt(10,100)), null, null,  //Decimal(4)
                        new BigDecimal(new BigInteger(5, rand) + "." + rand.nextInt(1000, 10000)), null, null, //Decimal32
                        new BigDecimal(new BigInteger(18, rand) + "." + rand.nextInt(100000, 1000000)), null, null, //Decimal64
                        new BigDecimal(new BigInteger(20, rand) + "." + rand.nextLong(10000000, 100000000)), null, null, //Decimal128
                        new BigDecimal(new BigInteger(57, rand) + "." + rand.nextLong(1000000000, 10000000000L)), null, null //Decimal256
                },
        };

        TableSchema schema = client.getTableSchema(tableName);

        ClickHouseFormat format = ClickHouseFormat.RowBinaryWithDefaults;
        try (InsertResponse response = client.insert(tableName, out -> {
            RowBinaryFormatWriter w = new RowBinaryFormatWriter(out, schema, format);
            for (Object[] row : rows) {
                for (int i = 0; i < row.length; i++) {
                    w.setValue(i + 1, row[i]);
                }
                w.commitRow();
            }
        }, format, settings).get()) {
            System.out.println("Rows written: " + response.getWrittenRows());
        }

        List<GenericRecord> records = client.queryAll("SELECT * FROM \"" + tableName  + "\"" );

        int id = 1;
        for (GenericRecord record : records) {
            Map<String, Object> r = record.getValues();
            assertEquals(r.get("id"), id);
            assertEqualsKinda(r.get("int8"), String.valueOf(rows[id - 1][1]));
            assertEqualsKinda(r.get("int8_nullable"), rows[id - 1][2]);
            assertEqualsKinda(r.get("int8_default"), 3);
            assertEqualsKinda(r.get("int16"), rows[id - 1][4]);
            assertEqualsKinda(r.get("int16_nullable"), rows[id - 1][5]);
            assertEqualsKinda(r.get("int16_default"), 3);
            assertEqualsKinda(r.get("int32"), rows[id - 1][7]);
            assertEqualsKinda(r.get("int32_nullable"), rows[id - 1][8]);
            assertEqualsKinda(r.get("int32_default"), 3);
            assertEqualsKinda(r.get("int64"), rows[id - 1][10]);
            assertEqualsKinda(r.get("int64_nullable"), rows[id - 1][11]);
            assertEqualsKinda(r.get("int64_default"), 3);
            assertEqualsKinda(r.get("int128"), rows[id - 1][13]);
            assertEqualsKinda(r.get("int128_nullable"), rows[id - 1][14]);
            assertEqualsKinda(r.get("int128_default"), 3);
            assertEqualsKinda(r.get("int256"), rows[id - 1][16]);
            assertEqualsKinda(r.get("int256_nullable"), rows[id - 1][17]);
            assertEqualsKinda(r.get("int256_default"), 3);
            assertEqualsKinda(r.get("uint8"), rows[id - 1][19]);
            assertEqualsKinda(r.get("uint8_nullable"), rows[id - 1][20]);
            assertEqualsKinda(r.get("uint8_default"), 3);
            assertEqualsKinda(r.get("uint16"), rows[id - 1][22]);
            assertEqualsKinda(r.get("uint16_nullable"), rows[id - 1][23]);
            assertEqualsKinda(r.get("uint16_default"), 3);
            assertEqualsKinda(r.get("uint32"), rows[id - 1][25]);
            assertEqualsKinda(r.get("uint32_nullable"), rows[id - 1][26]);
            assertEqualsKinda(r.get("uint32_default"), 3);
            assertEqualsKinda(r.get("uint64"), rows[id - 1][28]);
            assertEqualsKinda(r.get("uint64_nullable"), rows[id - 1][29]);
            assertEqualsKinda(r.get("uint64_default"), 3);
            assertEqualsKinda(r.get("uint128"), rows[id - 1][31]);
            assertEqualsKinda(r.get("uint128_nullable"), rows[id - 1][32]);
            assertEqualsKinda(r.get("uint128_default"), 3);
            assertEqualsKinda(r.get("uint256"), rows[id - 1][34]);
            assertEqualsKinda(r.get("uint256_nullable"), rows[id - 1][35]);
            assertEqualsKinda(r.get("uint256_default"), 3);
            assertEqualsKinda(r.get("float32"), rows[id - 1][37]);
            assertEqualsKinda(r.get("float32_nullable"), rows[id - 1][38]);
            assertEqualsKinda(r.get("float32_default"), 3.0);
            assertEqualsKinda(r.get("float64"), rows[id - 1][40]);
            assertEqualsKinda(r.get("float64_nullable"), rows[id - 1][41]);
            assertEqualsKinda(r.get("float64_default"), 3.0);
            assertEqualsKinda(r.get("decimal"), rows[id - 1][43]);
            assertEqualsKinda(r.get("decimal_nullable"), rows[id - 1][44]);
            assertEqualsKinda(r.get("decimal_default"), "3.00");
            assertEqualsKinda(r.get("decimal32"), rows[id - 1][46]);
            assertEqualsKinda(r.get("decimal32_nullable"), rows[id - 1][47]);
            assertEqualsKinda(r.get("decimal32_default"), "3.0000");
            assertEqualsKinda(r.get("decimal64"), rows[id - 1][49]);
            assertEqualsKinda(r.get("decimal64_nullable"), rows[id - 1][50]);
            assertEqualsKinda(r.get("decimal64_default"), "3.000000");
            assertEqualsKinda(r.get("decimal128"), rows[id - 1][52]);
            assertEqualsKinda(r.get("decimal128_nullable"), rows[id - 1][53]);
            assertEqualsKinda(r.get("decimal128_default"), "3.00000000");
            assertEqualsKinda(r.get("decimal256"), rows[id - 1][55]);
            assertEqualsKinda(r.get("decimal256_nullable"), rows[id - 1][56]);
            assertEqualsKinda(r.get("decimal256_default"), "3.0000000000");
            id++;
        }
    }

//    @Test
//    public void testAdvancedWriter() throws Exception {
//        String tableName = "very_long_table_name_with_uuid_" + UUID.randomUUID().toString().replace('-', '_');
//        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
//                " (name String, " +
//                "  v1 Float32, " +
//                "  v2 Float32, " +
//                "  attrs Nullable(String), " +
//                "  corrected_time DateTime('UTC') DEFAULT now()," +
//                "  special_attr Nullable(Int8) DEFAULT -1)" +
//                "  Engine = MergeTree ORDER by ()";
//
//        initTable(tableName, tableCreate);
//
//        ZonedDateTime correctedTime = Instant.now().atZone(ZoneId.of("UTC"));
//        Object[][] rows = new Object[][] {
//                {"foo1", 0.3f, 0.6f, "a=1,b=2,c=5", correctedTime, 10},
//                {"foo2", 0.6f, 0.1f, "a=1,b=2,c=5", correctedTime, null},
//                {"foo3", 0.7f, 0.4f, "a=1,b=2,c=5", null, null},
//                {"foo4", 0.8f, 0.5f, null, null, null},
//        };
//
//        TableSchema schema = client.getTableSchema(tableName);
//
//        ClickHouseFormat format = ClickHouseFormat.RowBinaryWithDefaults;
//        try (InsertResponse response = client.insert(tableName, out -> {
//            RowBinaryFormatWriter w = new RowBinaryFormatWriter(out, schema, format);
//            for (Object[] row : rows) {
//                for (int i = 0; i < row.length; i++) {
//                    w.setValue(i + 1, row[i]);
//                }
//                w.commitRow();
//            }
//        }, format, new InsertSettings()).get()) {
//            System.out.println("Rows written: " + response.getWrittenRows());
//        }
//
//        List<GenericRecord> records = client.queryAll("SELECT * FROM \"" + tableName  + "\"" );
//
//        for (GenericRecord record : records) {
//            System.out.println("> " + record.getString(1) + ", " + record.getFloat(2) + ", " + record.getFloat(3));
//        }
//    }
}
