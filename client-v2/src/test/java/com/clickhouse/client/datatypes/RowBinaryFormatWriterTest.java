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
                assertEqualsKinda(row.get(field.name), field.getValue());
            }
            id++;
        }
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

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        System.out.println("Random seed: " + seed);

        Field[][] rows = new Field[][] {{
                new Field("id", 1),
                new Field("int8", rand.nextInt(256) - 128), new Field("int8_nullable"), new Field("int8_default").set(3), //Int8
                new Field("int16", rand.nextInt(65536) - 32768), new Field("int16_nullable"), new Field("int16_default").set(3), //Int16
                new Field("int32", rand.nextInt()), new Field("int32_nullable"), new Field("int32_default").set(3), //Int32
                new Field("int64", rand.nextLong()), new Field("int64_nullable"), new Field("int64_default").set(3), //Int64
                new Field("int128", new BigInteger(127, rand)), new Field("int128_nullable"), new Field("int128_default").set(3), //Int128
                new Field("int256", new BigInteger(255, rand)), new Field("int256_nullable"), new Field("int256_default").set(3), //Int256
                new Field("uint8", rand.nextInt(256)), new Field("uint8_nullable"), new Field("uint8_default").set(3), //UInt8
                new Field("uint16", rand.nextInt(65536)), new Field("uint16_nullable"), new Field("uint16_default").set(3), //UInt16
                new Field("uint32", rand.nextInt() & 0xFFFFFFFFL), new Field("uint32_nullable"), new Field("uint32_default").set(3), //UInt32
                new Field("uint64", BigInteger.valueOf(rand.nextLong(Long.MAX_VALUE))), new Field("uint64_nullable"), new Field("uint64_default").set(3), //UInt64
                new Field("uint128", new BigInteger(128, rand)), new Field("uint128_nullable"), new Field("uint128_default").set(3), //UInt128
                new Field("uint256", new BigInteger(256, rand)), new Field("uint256_nullable"), new Field("uint256_default").set(3), //UInt256
                new Field("float32", rand.nextFloat()), new Field("float32_nullable"), new Field("float32_default").set("3.0"), //Float32
                new Field("float64", rand.nextDouble()), new Field("float64_nullable"), new Field("float64_default").set("3.0"), //Float64
                new Field("decimal", new BigDecimal(new BigInteger(5, rand) + "." + rand.nextInt(10,100))), new Field("decimal_nullable"), new Field("decimal_default").set("3.00"),  //Decimal(4)
                new Field("decimal32", new BigDecimal(new BigInteger(7, rand) + "." + rand.nextInt(1000, 10000))), new Field("decimal32_nullable"), new Field("decimal32_default").set("3.0000"), //Decimal32
                new Field("decimal64", new BigDecimal(new BigInteger(18, rand) + "." + rand.nextInt(100000, 1000000))), new Field("decimal64_nullable"), new Field("decimal64_default").set("3.000000"), //Decimal64
                new Field("decimal128", new BigDecimal(new BigInteger(20, rand) + "." + rand.nextLong(10000000, 100000000))), new Field("decimal128_nullable"), new Field("decimal128_default").set("3.00000000"), //Decimal128
                new Field("decimal256", new BigDecimal(new BigInteger(57, rand) + "." + rand.nextLong(1000000000, 10000000000L))), new Field("decimal256_nullable"), new Field("decimal256_default").set("3.0000000000") //Decimal256
            }
        };

        writeTest(tableName, tableCreate, rows);
    }


    private static class Field {
        String name;
        Object value;
        Object defaultValue;

        Field(String name) {
            this.name = name;
            this.value = null;
            this.defaultValue = null;
        }

        Field(String name, Object value) {
            this.name = name;
            this.value = value;
        }

        public Field set(Object defaultValue) {//For default value for comparison purposes
            this.defaultValue = defaultValue;
            return this;
        }

        public Object getValue() {
            if (value == null && defaultValue != null) {
                return defaultValue;
            }

            return value;
        }
    }
}
