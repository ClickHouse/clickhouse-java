package com.clickhouse.client;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseRenameMethod;
import com.clickhouse.client.config.ClickHouseSslMode;
import com.clickhouse.client.data.BinaryStreamUtils;
import com.clickhouse.client.data.ClickHouseBigDecimalValue;
import com.clickhouse.client.data.ClickHouseBigIntegerValue;
import com.clickhouse.client.data.ClickHouseByteValue;
import com.clickhouse.client.data.ClickHouseDateTimeValue;
import com.clickhouse.client.data.ClickHouseEnumValue;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.client.data.ClickHouseIntegerValue;
import com.clickhouse.client.data.ClickHouseIpv4Value;
import com.clickhouse.client.data.ClickHouseIpv6Value;
import com.clickhouse.client.data.ClickHouseLongValue;
import com.clickhouse.client.data.ClickHouseOffsetDateTimeValue;
import com.clickhouse.client.data.ClickHouseStringValue;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public abstract class ClientIntegrationTest extends BaseIntegrationTest {
    protected ClickHouseResponseSummary execute(ClickHouseRequest<?> request, String sql) throws ClickHouseException {
        try (ClickHouseResponse response = request.query(sql).executeAndWait()) {
            for (ClickHouseRecord record : response.records()) {
                for (ClickHouseValue value : record) {
                    Assert.assertNotNull(value, "Value should never be null");
                }
            }

            return response.getSummary();
        }
    }

    protected abstract ClickHouseProtocol getProtocol();

    protected abstract Class<? extends ClickHouseClient> getClientClass();

    protected ClickHouseClient getClient() {
        return ClickHouseClient.newInstance(getProtocol());
    }

    protected ClickHouseClient getSecureClient() {
        return ClickHouseClient.builder()
                .nodeSelector(ClickHouseNodeSelector.of(getProtocol()))
                .option(ClickHouseClientOption.SSL, true)
                .option(ClickHouseClientOption.SSL_MODE, ClickHouseSslMode.STRICT)
                .option(ClickHouseClientOption.SSL_ROOT_CERTIFICATE, "containers/clickhouse-server/certs/myCA.crt")
                .option(ClickHouseClientOption.SSL_CERTIFICATE, "client.crt")
                .option(ClickHouseClientOption.SSL_KEY, "client.key")
                .defaultCredentials(ClickHouseCredentials.fromUserAndPassword("me", ""))
                .build();
    }

    protected ClickHouseNode getServer() {
        return getServer(getProtocol());
    }

    protected ClickHouseNode getSecureServer() {
        return getSecureServer(getProtocol());
    }

    @DataProvider(name = "compressionMatrix")
    protected Object[][] getCompressionMatrix() {
        return new Object[][] {
                new Object[] { false, false },
                new Object[] { true, false },
                new Object[] { true, true },
                new Object[] { false, true } };
    }

    @DataProvider(name = "renameMethods")
    protected Object[][] getRenameMethods() {
        return new Object[][] {
                new Object[] { null, "a b c", " ", "d.E_f" },
                new Object[] { ClickHouseRenameMethod.NONE, "a b c", " ", "d.E_f" },
                new Object[] { ClickHouseRenameMethod.REMOVE_PREFIX, "a b c", " ", "E_f" },
                new Object[] { ClickHouseRenameMethod.TO_CAMELCASE, "aBC", "", "d.EF" },
                new Object[] { ClickHouseRenameMethod.TO_CAMELCASE_WITHOUT_PREFIX, "aBC", "", "EF" },
                new Object[] { ClickHouseRenameMethod.TO_UNDERSCORE, "a_b_c", "", "d._e_f" },
                new Object[] { ClickHouseRenameMethod.TO_UNDERSCORE_WITHOUT_PREFIX, "a_b_c", "", "E_f" }, };
    }

    @DataProvider(name = "simpleTypeProvider")
    protected Object[][] getSimpleTypes() {
        return new Object[][] {
                { ClickHouseDataType.Enum.name() + "('v-1' = -1, 'v0' = 0, 'v+1' = 1)", "v0", "v-1", "v+1" },
                { ClickHouseDataType.Enum8.name() + "('v-1' = -1, 'v0' = 0, 'v+1' = 1)", "v0", "v-1", "v+1" },
                { ClickHouseDataType.Enum16.name() + "('v-1' = -1, 'v0' = 0, 'v+1' = 1)", "v0", "v-1", "v+1" },
                { ClickHouseDataType.Int8.name(), "0", "-1", "1" },
                { ClickHouseDataType.UInt8.name(), "0", "255", "1" },
                { ClickHouseDataType.Int16.name(), "0", "-1", "1" },
                { ClickHouseDataType.UInt16.name(), "0", "65535", "1" },
                { ClickHouseDataType.Int32.name(), "0", "-1", "1" },
                { ClickHouseDataType.UInt32.name(), "0", "4294967295", "1" },
                { ClickHouseDataType.Int64.name(), "0", "-1", "1" },
                { ClickHouseDataType.UInt64.name(), "0", "18446744073709551615", "1" },
                { ClickHouseDataType.Int128.name(), "0", "-1", "1" },
                { ClickHouseDataType.UInt128.name(), "0", "340282366920938463463374607431768211455", "1" },
                { ClickHouseDataType.Int256.name(), "0", "-1", "1" },
                { ClickHouseDataType.UInt256.name(), "0",
                        "115792089237316195423570985008687907853269984665640564039457584007913129639935", "1" },
                { ClickHouseDataType.Float32.name(), "0.0", "-1.0", "1.0" },
                { ClickHouseDataType.Float64.name(), "0.0", "-1.0", "1.0" },
                { ClickHouseDataType.Date.name(), "1970-01-01", "1970-01-01", "1970-01-02" },
                { ClickHouseDataType.Date32.name(), "1970-01-01", "1969-12-31", "1970-01-02" },
                { ClickHouseDataType.DateTime.name(), "1970-01-01 00:00:00", "1970-01-01 00:00:00",
                        "1970-01-01 00:00:01" },
                { ClickHouseDataType.DateTime32.name(), "1970-01-01 00:00:00", "1970-01-01 00:00:00",
                        "1970-01-01 00:00:01" },
                { ClickHouseDataType.DateTime64.name() + "(3)", "1970-01-01 00:00:00", "1969-12-31 23:59:59.999",
                        "1970-01-01 00:00:00.001" },
                { ClickHouseDataType.Decimal.name() + "(10,9)", "0E-9", "-1.000000000", "1.000000000" },
                { ClickHouseDataType.Decimal32.name() + "(1)", "0.0", "-1.0", "1.0" },
                { ClickHouseDataType.Decimal64.name() + "(3)", "0.000", "-1.000", "1.000" },
                { ClickHouseDataType.Decimal128.name() + "(5)", "0.00000", "-1.00000", "1.00000" },
                { ClickHouseDataType.Decimal256.name() + "(7)", "0E-7", "-1.0000000", "1.0000000" },
                { ClickHouseDataType.FixedString.name() + "(3)", "0\0\0", "-1\0", "1\0\0" },
                { ClickHouseDataType.String.name(), "0", "-1", "1" },
                { ClickHouseDataType.UUID.name(), "00000000-0000-0000-0000-000000000000",
                        "00000000-0000-0000-ffff-ffffffffffff", "00000000-0000-0000-0000-000000000001" } };
    }

    @Test(groups = { "unit" })
    public void testInitialization() throws Exception {
        Assert.assertNotNull(getProtocol(), "The client should support a non-null protocol");
        Assert.assertNotEquals(getProtocol(), ClickHouseProtocol.ANY,
                "The client should support a specific protocol instead of ANY");

        try (ClickHouseClient client1 = ClickHouseClient.builder().build();
                ClickHouseClient client2 = ClickHouseClient.builder().option(ClickHouseClientOption.ASYNC, false)
                        .build();
                ClickHouseClient client3 = ClickHouseClient.newInstance();
                ClickHouseClient client4 = ClickHouseClient.newInstance(getProtocol());
                ClickHouseClient client5 = getClient()) {
            for (ClickHouseClient client : new ClickHouseClient[] { client1, client2, client3, client4, client5 }) {
                Assert.assertEquals(client.getClass(), getClientClass());
                Assert.assertTrue(client.accept(getProtocol()), "The client should support protocl: " + getProtocol());
            }
        }
    }

    @Test(groups = { "integration" })
    public void testOpenCloseClient() throws Exception {
        int count = 10;
        int timeout = 3000;
        ClickHouseNode server = getServer();
        for (int i = 0; i < count; i++) {
            try (ClickHouseClient client = getClient();
                    ClickHouseResponse response = client.connect(server).query("select 1").execute().get()) {
                Assert.assertEquals(response.firstRecord().getValue(0).asInteger(), 1);
            }
            Assert.assertTrue(getClient().ping(server, timeout));
        }
    }

    @Test(dataProvider = "compressionMatrix", groups = { "integration" })
    public void testCompression(boolean compressRequest, boolean compressResponse)
            throws ClickHouseException {
        ClickHouseNode server = getServer();
        String uuid = UUID.randomUUID().toString();
        for (ClickHouseFormat format : new ClickHouseFormat[] {
                ClickHouseFormat.RowBinaryWithNamesAndTypes,
                ClickHouseFormat.TabSeparatedWithNamesAndTypes }) {
            try (ClickHouseClient client = getClient()) {
                ClickHouseRequest<?> request = client.connect(server)
                        .format(format)
                        .compressServerResponse(compressResponse)
                        .decompressClientRequest(compressRequest);
                boolean hasResult = false;
                try (ClickHouseResponse resp = request
                        .query("select :uuid").params(ClickHouseStringValue.of(uuid)).executeAndWait()) {
                    Assert.assertEquals(resp.firstRecord().getValue(0).asString(), uuid);
                    hasResult = true;
                }
                Assert.assertTrue(hasResult, "Should have at least one result");

                // empty results
                try (ClickHouseResponse resp = request
                        .query("create database if not exists system")
                        .executeAndWait()) {
                    ClickHouseResponseSummary summary = resp.getSummary();
                    Assert.assertEquals(summary.getReadRows(), 0L);
                    Assert.assertEquals(summary.getWrittenRows(), 0L);
                }

                // let's also check if failures can be captured successfully as well
                ClickHouseException exp = null;
                try (ClickHouseResponse resp = request
                        .use(uuid)
                        .query("select currentUser(), timezone(), version(), getSetting('readonly') readonly FORMAT RowBinaryWithNamesAndTypes")
                        .executeAndWait()) {
                    Assert.fail("Query should fail");
                } catch (ClickHouseException e) {
                    exp = e;
                }
                Assert.assertEquals(exp.getErrorCode(), 81);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testFormat() throws ClickHouseException {
        String sql = "select 1, 2";
        ClickHouseNode node = getServer();

        try (ClickHouseClient client = getClient()) {
            try (ClickHouseResponse response = client.connect(node)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).query(sql).executeAndWait()) {
                Assert.assertEquals(response.getColumns().size(), 2);
                int counter = 0;
                for (ClickHouseRecord record : response.records()) {
                    Assert.assertEquals(record.getValue(0).asShort(), 1);
                    Assert.assertEquals(record.getValue(1).asShort(), 2);
                    counter++;
                }
                Assert.assertEquals(counter, 1);
            }

            // now let's try again using unsupported formats
            try (ClickHouseResponse response = client.connect(node).query(sql).format(ClickHouseFormat.CSV)
                    .executeAndWait()) {
                String results = new BufferedReader(
                        new InputStreamReader(response.getInputStream(), StandardCharsets.UTF_8)).lines()
                        .collect(Collectors.joining("\n"));
                Assert.assertEquals(results, "1,2");
            }

            try (ClickHouseResponse response = client.connect(node).query(sql).format(ClickHouseFormat.JSONEachRow)
                    .executeAndWait()) {
                String results = new BufferedReader(
                        new InputStreamReader(response.getInputStream(), StandardCharsets.UTF_8)).lines()
                        .collect(Collectors.joining("\n"));
                Assert.assertEquals(results, "{\"1\":1,\"2\":2}");
            }
        }
    }

    @Test(groups = "integration")
    public void testNonExistDb() throws Exception {
        ClickHouseNode server = getServer();

        try {
            ClickHouseClient.send(server, "drop database non_exist_db").get();
            Assert.fail("Exception is excepted");
        } catch (ExecutionException e) {
            ClickHouseException ce = ClickHouseException.of(e.getCause(), server);
            Assert.assertEquals(ce.getErrorCode(), 81);
        }

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = client.connect(server).use("non_exist_db").query("select 1").execute()
                        .get()) {
            Assert.fail("Exception is excepted");
        } catch (ExecutionException e) {
            ClickHouseException ce = ClickHouseException.of(e.getCause(), server);
            Assert.assertEquals(ce.getErrorCode(), 81);
        }

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = client.connect(server).use("").query("select 1").execute().get()) {
            Assert.assertEquals(resp.firstRecord().getValue(0).asInteger(), 1);
        } catch (Exception e) {
            Assert.fail("Should not have exception");
        }

        try (ClickHouseClient client = getClient()) {
            String db = new StringBuilder().append('`').append(UUID.randomUUID().toString()).append('`').toString();
            try (ClickHouseResponse resp = client.connect(server).use("").query("create database " + db).execute()
                    .get()) {
            }
            try (ClickHouseResponse resp = client.connect(server).use("").query("drop database " + db).execute()
                    .get()) {
            }
        } catch (Exception e) {
            Assert.fail("Should not have exception");
        }
    }

    @Test(groups = { "integration" })
    public void testQueryWithNoResult() throws Exception {
        String sql = "select * from system.numbers limit 0";

        try (ClickHouseClient client = getClient()) {
            // header without row
            try (ClickHouseResponse response = client.connect(getServer())
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).query(sql).execute().get()) {
                Assert.assertEquals(response.getColumns().size(), 1);
                Assert.assertNotEquals(response.getColumns(), ClickHouseDataProcessor.DEFAULT_COLUMNS);
                for (ClickHouseRecord record : response.records()) {
                    Assert.fail(ClickHouseUtils.format("Should have no record, but we got: %s", record));
                }
            }

            // no header and row
            try (ClickHouseResponse response = client.connect(getServer()).format(ClickHouseFormat.RowBinary).query(sql)
                    .execute().get()) {
                Assert.assertEquals(response.getColumns(), Collections.emptyList());
                for (ClickHouseRecord record : response.records()) {
                    Assert.fail(ClickHouseUtils.format("Should have no record, but we got: %s", record));
                }
            }

            // custom header and row
            try (ClickHouseResponse response = client.connect(getServer()).format(ClickHouseFormat.RowBinary).query(sql)
                    .execute().get()) {
                Assert.assertEquals(response.getColumns(), Collections.emptyList());
                for (ClickHouseRecord record : response.records()) {
                    Assert.fail(ClickHouseUtils.format("Should have no record, but we got: %s", record));
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testQuery() throws Exception {
        ClickHouseNode server = getServer();

        try (ClickHouseClient client = getClient()) {
            // "select * from system.data_type_families"
            int limit = 10000;
            String sql = "select number, toString(number) from system.numbers limit " + limit;

            try (ClickHouseResponse response = client.connect(server)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .set("send_logs_level", "trace")
                    .set("enable_optimize_predicate_expression", 1)
                    .set("log_queries_min_type", "EXCEPTION_WHILE_PROCESSING")
                    .query(sql).execute().get()) {
                List<ClickHouseColumn> columns = response.getColumns();
                int index = 0;
                for (ClickHouseRecord record : response.records()) {
                    String col1 = String.valueOf(record.getValue(0).asBigInteger());
                    String col2 = record.getValue(1).asString();
                    Assert.assertEquals(record.size(), columns.size());
                    Assert.assertEquals(col1, col2);
                    Assert.assertEquals(col1, String.valueOf(index++));
                }

                // int counter = 0;
                // for (ClickHouseValue value : response.values()) {
                // Assert.assertEquals(value.asString(), String.valueOf(index));
                // index += counter++ % 2;
                // }
                Assert.assertEquals(index, limit);
                // Thread.sleep(30000);
                /*
                 * while (response.hasError()) { int index = 0; for (ClickHouseColumn c :
                 * columns) { // RawValue v = response.getRawValue(index++); // String v =
                 * response.getValue(index++, String.class) }
                 * 
                 * } byte[] bytes = in.readAllBytes(); String str = new String(bytes);
                 */
            } catch (Exception e) {
                Assert.fail("Query failed", e);
            }
        }
    }

    @Test(groups = "integration")
    public void testQueryInSameThread() throws Exception {
        ClickHouseNode server = getServer();

        try (ClickHouseClient client = ClickHouseClient.builder().option(ClickHouseClientOption.ASYNC, false).build()) {
            CompletableFuture<ClickHouseResponse> future = client.connect(server)
                    .format(ClickHouseFormat.TabSeparatedWithNamesAndTypes).query("select 1,2").execute();
            // Assert.assertTrue(future instanceof ClickHouseImmediateFuture);
            Assert.assertTrue(future.isDone());
            try (ClickHouseResponse resp = future.get()) {
                Assert.assertEquals(resp.getColumns().size(), 2);
                for (ClickHouseRecord record : resp.records()) {
                    Assert.assertEquals(record.size(), 2);
                    Assert.assertEquals(record.getValue(0).asInteger(), 1);
                    Assert.assertEquals(record.getValue(1).asInteger(), 2);
                }

                // ClickHouseResponseSummary summary = resp.getSummary();
                // Assert.assertEquals(summary.getStatistics().getRows(), 1);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testMutation() throws Exception {
        ClickHouseNode node = getServer();

        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> request = client.connect(node)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .set("send_logs_level", "trace")
                    .set("enable_optimize_predicate_expression", 1)
                    .set("log_queries_min_type", "EXCEPTION_WHILE_PROCESSING");
            execute(request, "drop table if exists test_mutation;");
            execute(request, "create table if not exists test_mutation(a String, b UInt32) engine = Memory;");
            execute(request, "insert into test_mutation values('a', 1)('b', 2)");
        }
    }

    @Test(groups = "integration")
    public void testQueryIntervalTypes() throws Exception {
        ClickHouseNode server = getServer();

        try (ClickHouseClient client = getClient()) {
            for (ClickHouseDataType type : new ClickHouseDataType[] { ClickHouseDataType.IntervalYear,
                    ClickHouseDataType.IntervalQuarter, ClickHouseDataType.IntervalMonth,
                    ClickHouseDataType.IntervalWeek, ClickHouseDataType.IntervalDay, ClickHouseDataType.IntervalHour,
                    ClickHouseDataType.IntervalMinute, ClickHouseDataType.IntervalSecond }) {
                try (ClickHouseResponse resp = client.connect(server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query(ClickHouseUtils.format(
                                "select to%1$s(0), to%1$s(-1), to%1$s(1), to%1$s(%2$d), to%1$s(%3$d)", type.name(),
                                Long.MIN_VALUE, Long.MAX_VALUE))
                        .execute().get()) {
                    List<ClickHouseRecord> records = new ArrayList<>();
                    for (ClickHouseRecord record : resp.records()) {
                        records.add(record);
                    }

                    Assert.assertEquals(records.size(), 1);
                    ClickHouseRecord r = records.get(0);
                    Assert.assertEquals(r.getValue(0).asString(), "0");
                    Assert.assertEquals(r.getValue(1).asString(), "-1");
                    Assert.assertEquals(r.getValue(2).asString(), "1");
                    Assert.assertEquals(r.getValue(3).asString(), String.valueOf(Long.MIN_VALUE));
                    Assert.assertEquals(r.getValue(4).asString(), String.valueOf(Long.MAX_VALUE));
                }
            }
        }
    }

    @Test(groups = "integration")
    public void testReadWriteDateTimeTypes() throws Exception {
        ClickHouseNode server = getServer();

        ClickHouseClient.send(server, "drop table if exists test_datetime_types",
                "create table test_datetime_types(no UInt8, d0 DateTime32, d1 DateTime64(5), d2 DateTime(3), d3 DateTime64(3, 'Asia/Chongqing')) engine=Memory")
                .get();
        ClickHouseClient.send(server, "insert into test_datetime_types values(:no, :d0, :d1, :d2, :d3)",
                new ClickHouseValue[] { ClickHouseIntegerValue.ofNull(),
                        ClickHouseDateTimeValue.ofNull(0, ClickHouseValues.UTC_TIMEZONE),
                        ClickHouseDateTimeValue.ofNull(3, ClickHouseValues.UTC_TIMEZONE),
                        ClickHouseDateTimeValue.ofNull(9, ClickHouseValues.UTC_TIMEZONE),
                        ClickHouseOffsetDateTimeValue.ofNull(3, TimeZone.getTimeZone("Asia/Chongqing")) },
                new Object[] { 0, "1970-01-01 00:00:00", "1970-01-01 00:00:00.123456",
                        "1970-01-01 00:00:00.123456789", "1970-02-01 12:34:56.789" },
                new Object[] { 1, -1, -1, -1, -1 }, new Object[] { 2, 1, 1, 1, 1 },
                new Object[] { 3, 2.1, 2.1, 2.1, 2.1 }).get();

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query("select * except(no) from test_datetime_types order by no").execute().get()) {
            List<ClickHouseRecord> list = new ArrayList<>();
            for (ClickHouseRecord record : resp.records()) {
                list.add(record);
            }

            Assert.assertEquals(list.size(), 4);
        }
    }

    @Test(groups = "integration")
    public void testReadWriteDomains() throws Exception {
        ClickHouseNode server = getServer();

        ClickHouseClient.send(server, "drop table if exists test_domain_types",
                "create table test_domain_types(no UInt8, ipv4 IPv4, nipv4 Nullable(IPv4), ipv6 IPv6, nipv6 Nullable(IPv6)) engine=Memory")
                .get();

        ClickHouseClient.send(server, "insert into test_domain_types values(:no, :i0, :i1, :i2, :i3)",
                new ClickHouseValue[] { ClickHouseIntegerValue.ofNull(), ClickHouseIpv4Value.ofNull(),
                        ClickHouseIpv4Value.ofNull(), ClickHouseIpv6Value.ofNull(), ClickHouseIpv6Value.ofNull() },
                new Object[] { 0,
                        (Inet4Address) InetAddress.getByAddress(new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 0 }),
                        null,
                        Inet6Address.getByAddress(null,
                                new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                                        (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                                        (byte) 0 },
                                null),
                        null },
                new Object[] { 1,
                        (Inet4Address) InetAddress.getByAddress(new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 1 }),
                        (Inet4Address) InetAddress
                                .getByAddress(new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF }),
                        Inet6Address.getByAddress(null,
                                new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                                        (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                                        (byte) 1 },
                                null),
                        Inet6Address.getByAddress(null,
                                new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF },
                                null) })
                .get();
        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query("select * except(no) from test_domain_types order by no").execute().get()) {
            List<ClickHouseRecord> list = new ArrayList<>();
            for (ClickHouseRecord record : resp.records()) {
                list.add(record);
            }

            Assert.assertEquals(list.size(), 2);
        }
    }

    @Test(groups = "integration")
    public void testReadWriteEnumTypes() throws Exception {
        ClickHouseNode server = getServer();

        ClickHouseClient.send(server, "drop table if exists test_enum_types",
                "create table test_enum_types(no UInt8, e01 Nullable(Enum8('a'=-1,'b'=2,'c'=0)), e1 Enum8('a'=-1,'b'=2,'c'=0), "
                        + "e02 Nullable(Enum16('a'=-1,'b'=2,'c'=0)), e2 Enum16('a'=-1,'b'=2,'c'=0)) engine=Memory")
                .get();
        ClickHouseClient.send(server, "insert into test_enum_types values(:no, :e01, :e1, :e02, :e2)",
                new ClickHouseValue[] { ClickHouseByteValue.ofNull(),
                        ClickHouseEnumValue
                                .ofNull(ClickHouseColumn.of("column", "Enum8('dunno'=-1)").getEnumConstants()),
                        ClickHouseEnumValue
                                .ofNull(ClickHouseColumn.of("column", "Enum8('a'=-1,'b'=2,'c'=0)").getEnumConstants()),
                        ClickHouseEnumValue
                                .ofNull(ClickHouseColumn.of("column", "Enum16('a'=-1,'b'=2,'c'=0)").getEnumConstants()),
                        ClickHouseEnumValue
                                .ofNull(ClickHouseColumn.of("column", "Enum16('dunno'=2)").getEnumConstants()), },
                new Object[] { 0, null, "b", null, "dunno" },
                new Object[] { 1, "dunno", 2, "a", 2 }).get();

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query("select * except(no) from test_enum_types order by no").execute().get()) {
            int count = 0;
            for (ClickHouseRecord r : resp.records()) {
                if (count++ == 0) {
                    Assert.assertEquals(r.getValue(0).asShort(), (short) 0);
                    Assert.assertEquals(r.getValue(0).asString(), null);
                    Assert.assertEquals(r.getValue(0).asObject(), null);
                    Assert.assertEquals(r.getValue(1).asInteger(), 2);
                    Assert.assertEquals(r.getValue(1).asString(), "b");
                    Assert.assertEquals(r.getValue(1).asObject(), "b");
                    Assert.assertEquals(r.getValue(2).asLong(), 0L);
                    Assert.assertEquals(r.getValue(2).asString(), null);
                    Assert.assertEquals(r.getValue(2).asObject(), null);
                    Assert.assertEquals(r.getValue(3).asByte(), (byte) 2);
                    Assert.assertEquals(r.getValue(3).asString(), "b");
                    Assert.assertEquals(r.getValue(3).asObject(), "b");
                } else {
                    Assert.assertEquals(r.getValue(0).asByte(), (byte) -1);
                    Assert.assertEquals(r.getValue(0).asString(), "a");
                    Assert.assertEquals(r.getValue(0).asObject(), "a");
                    Assert.assertEquals(r.getValue(1).asShort(), (short) 2);
                    Assert.assertEquals(r.getValue(1).asString(), "b");
                    Assert.assertEquals(r.getValue(1).asObject(), "b");
                    Assert.assertEquals(r.getValue(2).asInteger(), -1);
                    Assert.assertEquals(r.getValue(2).asString(), "a");
                    Assert.assertEquals(r.getValue(2).asObject(), "a");
                    Assert.assertEquals(r.getValue(3).asLong(), 2L);
                    Assert.assertEquals(r.getValue(3).asString(), "b");
                    Assert.assertEquals(r.getValue(3).asObject(), "b");
                }
            }

            Assert.assertEquals(count, 2);
        }
    }

    @Test(groups = "integration")
    public void testReadWriteGeoTypes() throws Exception {
        ClickHouseNode server = getServer();

        ClickHouseClient.send(server, "set allow_experimental_geo_types=1", "drop table if exists test_geo_types",
                "create table test_geo_types(no UInt8, p Point, r Ring, pg Polygon, mp MultiPolygon) engine=Memory")
                .get();

        // write
        ClickHouseClient.send(server,
                "insert into test_geo_types values(0, (0,0), " + "[(0,0),(0,0)], [[(0,0),(0,0)],[(0,0),(0,0)]], "
                        + "[[[(0,0),(0,0)],[(0,0),(0,0)]],[[(0,0),(0,0)],[(0,0),(0,0)]]])",
                "insert into test_geo_types values(1, (-1,-1), "
                        + "[(-1,-1),(-1,-1)], [[(-1,-1),(-1,-1)],[(-1,-1),(-1,-1)]], "
                        + "[[[(-1,-1),(-1,-1)],[(-1,-1),(-1,-1)]],[[(-1,-1),(-1,-1)],[(-1,-1),(-1,-1)]]])",
                "insert into test_geo_types values(2, (1,1), " + "[(1,1),(1,1)], [[(1,1),(1,1)],[(1,1),(1,1)]], "
                        + "[[[(1,1),(1,1)],[(1,1),(1,1)]],[[(1,1),(1,1)],[(1,1),(1,1)]]])")
                .get();

        // read
        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query("select * except(no) from test_geo_types order by no").execute().get()) {
            List<String[]> records = new ArrayList<>();
            for (ClickHouseRecord record : resp.records()) {
                String[] values = new String[record.size()];
                int index = 0;
                for (ClickHouseValue v : record) {
                    values[index++] = v.asString();
                }
                records.add(values);
            }

            Assert.assertEquals(records.size(), 3);
            Assert.assertEquals(records.get(0)[0], "(0.0,0.0)");
            Assert.assertEquals(records.get(0)[1], "[(0.0,0.0),(0.0,0.0)]");
            Assert.assertEquals(records.get(0)[2], "[[(0.0,0.0),(0.0,0.0)],[(0.0,0.0),(0.0,0.0)]]");
            Assert.assertEquals(records.get(0)[3],
                    "[[[(0.0,0.0),(0.0,0.0)],[(0.0,0.0),(0.0,0.0)]],[[(0.0,0.0),(0.0,0.0)],[(0.0,0.0),(0.0,0.0)]]]");
            Assert.assertEquals(records.get(1)[0], "(-1.0,-1.0)");
            Assert.assertEquals(records.get(1)[1], "[(-1.0,-1.0),(-1.0,-1.0)]");
            Assert.assertEquals(records.get(1)[2], "[[(-1.0,-1.0),(-1.0,-1.0)],[(-1.0,-1.0),(-1.0,-1.0)]]");
            Assert.assertEquals(records.get(1)[3],
                    "[[[(-1.0,-1.0),(-1.0,-1.0)],[(-1.0,-1.0),(-1.0,-1.0)]],[[(-1.0,-1.0),(-1.0,-1.0)],[(-1.0,-1.0),(-1.0,-1.0)]]]");
            Assert.assertEquals(records.get(2)[0], "(1.0,1.0)");
            Assert.assertEquals(records.get(2)[1], "[(1.0,1.0),(1.0,1.0)]");
            Assert.assertEquals(records.get(2)[2], "[[(1.0,1.0),(1.0,1.0)],[(1.0,1.0),(1.0,1.0)]]");
            Assert.assertEquals(records.get(2)[3],
                    "[[[(1.0,1.0),(1.0,1.0)],[(1.0,1.0),(1.0,1.0)]],[[(1.0,1.0),(1.0,1.0)],[(1.0,1.0),(1.0,1.0)]]]");
        }
    }

    @Test(dataProvider = "simpleTypeProvider", groups = "integration")
    public void testReadWriteSimpleTypes(String dataType, String zero, String negativeOne, String positiveOne)
            throws Exception {
        ClickHouseNode server = getServer();

        String typeName = dataType;
        String columnName = typeName.toLowerCase();
        int currIdx = columnName.indexOf('(');
        if (currIdx > 0) {
            columnName = columnName.substring(0, currIdx);
        }
        String dropTemplate = "drop table if exists test_%s";
        String createTemplate = "create table test_%1$s(no UInt8, %1$s %2$s, n%1$s Nullable(%2$s)) engine=Memory";
        String insertTemplate = "insert into table test_%s values(%s, %s, %s)";

        String negativeOneValue = "-1";
        String zeroValue = "0";
        String positiveOneValue = "1";
        if (dataType.startsWith(ClickHouseDataType.FixedString.name())) {
            negativeOneValue = "'-1'";
            zeroValue = "'0'";
            positiveOneValue = "'1'";
        } else if (dataType.startsWith(ClickHouseDataType.UUID.name())) {
            negativeOneValue = ClickHouseUtils.format("'%s'", ClickHouseIntegerValue.of(-1).asUuid());
            zeroValue = ClickHouseUtils.format("'%s'", ClickHouseIntegerValue.of(0).asUuid());
            positiveOneValue = ClickHouseUtils.format("'%s'", ClickHouseIntegerValue.of(1).asUuid());
        }

        try {
            ClickHouseClient
                    .send(server, ClickHouseUtils.format(dropTemplate, columnName),
                            ClickHouseUtils.format(createTemplate, columnName, typeName),
                            ClickHouseUtils.format(insertTemplate, columnName, 0, zeroValue, null),
                            ClickHouseUtils.format(insertTemplate, columnName, 1, zeroValue, zeroValue),
                            ClickHouseUtils.format(insertTemplate, columnName, 2, negativeOneValue, negativeOneValue),
                            ClickHouseUtils.format(insertTemplate, columnName, 3, positiveOneValue, positiveOneValue))
                    .get();
        } catch (ExecutionException e) {
            // maybe the type is just not supported, for example: Date32
            Throwable cause = e.getCause();
            Assert.assertTrue(cause instanceof ClickHouseException);
            return;
        }

        ClickHouseVersion version = null;
        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = client
                        .connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes).query(ClickHouseUtils
                                .format("select * except(no), version() from test_%s order by no", columnName))
                        .execute().get()) {
            List<String[]> records = new ArrayList<>();
            for (ClickHouseRecord record : resp.records()) {
                String[] values = new String[record.size()];
                int index = 0;
                for (ClickHouseValue v : record) {
                    values[index++] = v.asString();
                }
                records.add(values);
            }

            Assert.assertEquals(records.size(), 4);
            Assert.assertEquals(records.get(0)[0], zero);
            Assert.assertEquals(records.get(0)[1], null);
            if (version == null) {
                version = ClickHouseVersion.of(records.get(0)[2]);
            }

            Assert.assertEquals(records.get(1)[0], zero);
            Assert.assertEquals(records.get(1)[1], zero);
            Assert.assertEquals(records.get(3)[0], positiveOne);
            Assert.assertEquals(records.get(3)[1], positiveOne);

            if ((ClickHouseDataType.DateTime.name().equals(dataType)
                    || ClickHouseDataType.DateTime32.name().equals(dataType)) && version.getMajorVersion() == 21
                    && version.getMinorVersion() == 3) {
                // skip DateTime and DateTime32 negative test on 21.3 since it's not doing well
                // see https://github.com/ClickHouse/ClickHouse/issues/29835 for more
            } else {
                Assert.assertEquals(records.get(2)[0], negativeOne);
                Assert.assertEquals(records.get(2)[1], negativeOne);
            }
        }
    }

    @Test(groups = "integration")
    public void testReadWriteMap() throws Exception {
        ClickHouseNode server = getServer();

        try {
            ClickHouseClient
                    .send(server, "drop table if exists test_map_types",
                            "create table test_map_types(no UInt32, m Map(LowCardinality(String), Int32), n Map(String, Array(Nullable(DateTime64(3, 'Asia/Chongqing')))))engine=Memory")
                    .get();
        } catch (ExecutionException e) {
            // looks like LowCardinality(String) as key is not supported even in 21.8
            Throwable cause = e.getCause();
            Assert.assertTrue(cause instanceof ClickHouseException);
            return;
        }

        // write
        ClickHouseClient.send(server, "insert into test_map_types values (1, {'key1' : 1}, {'a' : [], 'b' : [null]})")
                .get();
        ClickHouseClient.send(server, "insert into test_map_types values (:n,:m,:x)",
                new String[][] {
                        new String[] { "-1", "{'key-1' : -1}",
                                "{'a' : [], 'b' : [ '2022-03-30 00:00:00.123', null ]}" },
                        new String[] { "-2", "{'key-2' : -2}", "{'key-2' : [null]}" } })
                .get();
        ClickHouseClient.send(server, "insert into test_map_types values (3, :m, {})",
                Collections.singletonMap("m", "{'key3' : 3}")).get();

        // read
        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query("select * except(no) from test_map_types order by no").execute().get()) {
            List<Object[]> records = new ArrayList<>();
            for (ClickHouseRecord r : resp.records()) {
                Object[] values = new Object[r.size()];
                int index = 0;
                for (ClickHouseValue v : r) {
                    values[index++] = v.asObject();
                }
                records.add(values);
            }

            Assert.assertEquals(records.size(), 4);
        }
    }

    @Test(groups = "integration")
    public void testReadWriteUInt64() throws Exception {
        ClickHouseNode server = getServer();

        // INSERT INTO test_table VALUES (10223372036854775100)
        ClickHouseClient.send(server, "drop table if exists test_uint64_values",
                "create table test_uint64_values(no UInt8, v0 UInt64, v1 UInt64, v2 UInt64, v3 UInt64) engine=Memory")
                .get();
        ClickHouseClient.send(server, "insert into test_uint64_values values(:no, :v0, :v1, :v2, :v3)",
                new ClickHouseValue[] { ClickHouseIntegerValue.ofNull(),
                        ClickHouseLongValue.ofNull(true), ClickHouseStringValue.ofNull(),
                        ClickHouseBigIntegerValue.ofNull(), ClickHouseBigDecimalValue.ofNull() },
                new Object[] { 0, 0L, "0", BigInteger.ZERO, BigDecimal.ZERO },
                new Object[] { 1, 1L, "1", BigInteger.ONE, BigDecimal.ONE },
                new Object[] { 2, Long.MAX_VALUE, Long.toString(Long.MAX_VALUE), BigInteger.valueOf(Long.MAX_VALUE),
                        BigDecimal.valueOf(Long.MAX_VALUE) },
                new Object[] { 3, -8223372036854776516L, "10223372036854775100", new BigInteger("10223372036854775100"),
                        new BigDecimal("10223372036854775100") })
                .get();

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query("select * except(no) from test_uint64_values order by no").execute().get()) {
            int count = 0;
            for (ClickHouseRecord r : resp.records()) {
                if (count == 0) {
                    Assert.assertEquals(r.getValue(0).asLong(), 0L);
                    Assert.assertEquals(r.getValue(1).asLong(), 0L);
                    Assert.assertEquals(r.getValue(2).asLong(), 0L);
                    Assert.assertEquals(r.getValue(3).asLong(), 0L);
                } else if (count == 1) {
                    Assert.assertEquals(r.getValue(0).asLong(), 1L);
                    Assert.assertEquals(r.getValue(1).asLong(), 1L);
                    Assert.assertEquals(r.getValue(2).asLong(), 1L);
                    Assert.assertEquals(r.getValue(3).asLong(), 1L);
                } else if (count == 2) {
                    Assert.assertEquals(r.getValue(0).asLong(), Long.MAX_VALUE);
                    Assert.assertEquals(r.getValue(1).asLong(), Long.MAX_VALUE);
                    Assert.assertEquals(r.getValue(2).asLong(), Long.MAX_VALUE);
                    Assert.assertEquals(r.getValue(3).asLong(), Long.MAX_VALUE);
                } else if (count == 3) {
                    Assert.assertEquals(r.getValue(0).asString(), "10223372036854775100");
                    Assert.assertEquals(r.getValue(1).asBigInteger(), new BigInteger("10223372036854775100"));
                    Assert.assertEquals(r.getValue(2).asBigDecimal(), new BigDecimal("10223372036854775100"));
                    Assert.assertEquals(r.getValue(3).asLong(), -8223372036854776516L);
                }
                count++;
            }

            Assert.assertEquals(count, 4);
        }
    }

    @Test(groups = "integration")
    public void testQueryWithMultipleExternalTables() throws Exception {
        ClickHouseNode server = getServer();

        int tables = 30;
        int rows = 10;
        try (ClickHouseClient client = getClient()) {
            try (ClickHouseResponse resp = client.connect(server).query("drop table if exists test_ext_data_query")
                    .execute().get()) {
            }

            String ddl = "create table test_ext_data_query (\n" + "   Cb String,\n" + "   CREATETIME DateTime64(3),\n"
                    + "   TIMESTAMP UInt64,\n" + "   Cc String,\n" + "   Ca1 UInt64,\n" + "   Ca2 UInt64,\n"
                    + "   Ca3 UInt64\n" + ") engine = MergeTree()\n" + "PARTITION BY toYYYYMMDD(CREATETIME)\n"
                    + "ORDER BY (Cb, CREATETIME, Cc);";
            try (ClickHouseResponse resp = client.connect(server).query(ddl).execute().get()) {
            }
        }

        String template = "avgIf(Ca1, Cb in L%1$d) as avgCa1%2$d, sumIf(Ca1, Cb in L%1$d) as sumCa1%2$d, minIf(Ca1, Cb in L%1$d) as minCa1%2$d, maxIf(Ca1, Cb in L%1$d) as maxCa1%2$d, anyIf(Ca1, Cb in L%1$d) as anyCa1%2$d, avgIf(Ca2, Cb in L%1$d) as avgCa2%2$d, sumIf(Ca2, Cb in L%1$d) as sumCa2%2$d, minIf(Ca2, Cb in L%1$d) as minCa2%2$d, maxIf(Ca2, Cb in L%1$d) as maxCa2%2$d, anyIf(Ca2, Cb in L%1$d) as anyCa2%2$d, avgIf(Ca3, Cb in L%1$d) as avgCa3%2$d, sumIf(Ca3, Cb in L%1$d) as sumCa3%2$d, minIf(Ca3, Cb in L%1$d) as minCa3%2$d, maxIf(Ca3, Cb in L%1$d) as maxCa3%2$d, anyIf(Ca3, Cb in L%1$d) as anyCa3%2$d";
        StringBuilder sql = new StringBuilder().append("select ");
        List<ClickHouseExternalTable> extTableList = new ArrayList<>(tables);
        for (int i = 0; i < tables; i++) {
            sql.append(ClickHouseUtils.format(template, i, i + 1)).append(',');
            List<String> valueList = new ArrayList<>(rows);
            for (int j = i, size = i + rows; j < size; j++) {
                valueList.add(String.valueOf(j));
            }
            String dnExtString = String.join("\n", valueList);
            InputStream inputStream = new ByteArrayInputStream(dnExtString.getBytes(StandardCharsets.UTF_8));
            ClickHouseExternalTable extTable = ClickHouseExternalTable.builder().name("L" + i).content(inputStream)
                    .addColumn("Cb", "String").build();
            extTableList.add(extTable);
        }

        if (tables > 0) {
            sql.deleteCharAt(sql.length() - 1);
        } else {
            sql.append('*');
        }
        sql.append(
                " from test_ext_data_query where TIMESTAMP >= 1625796480 and TIMESTAMP < 1625796540 and Cc = 'eth0'");

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = client.connect(server).query(sql.toString())
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).external(extTableList).execute().get()) {
            Assert.assertNotNull(resp.getColumns());
            Assert.assertTrue(tables <= 0 || resp.records().iterator().hasNext());
        }
    }

    @Test(groups = { "integration" })
    public void testCustomRead() throws Exception {
        long limit = 1000L;
        long count = 0L;
        ClickHouseNode server = getServer();
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> request = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .query("select * from numbers(:limit)").params(String.valueOf(limit));
            ClickHouseConfig config = request.getConfig();
            try (ClickHouseResponse response = request.executeAndWait()) {
                ClickHouseInputStream input = response.getInputStream();
                List<ClickHouseColumn> list = response.getColumns();
                ClickHouseColumn[] columns = list.toArray(new ClickHouseColumn[0]);
                ClickHouseValue[] values = ClickHouseValues.newValues(config, columns);
                ClickHouseDataProcessor processor = ClickHouseDataStreamFactory.getInstance()
                        .getProcessor(config, input, null, null, list);
                int len = columns.length;
                while (input.available() > 0) {
                    for (int i = 0; i < len; i++) {
                        Assert.assertEquals(processor.read(values[i], columns[i]).asLong(), count++);
                    }
                }
            }
        }

        Assert.assertEquals(count, 1000L);
    }

    @Test(groups = { "integration" })
    public void testDumpAndLoadFile() throws Exception {
        // super.testLoadRawData();
        ClickHouseNode server = getServer();
        ClickHouseClient.send(server, "drop table if exists test_dump_load_file",
                "create table test_dump_load_file(a UInt64, b Nullable(String)) engine=MergeTree() order by tuple()")
                .get();

        final int rows = 10000;
        final Path tmp = Path.of(System.getProperty("java.io.tmpdir"), "file.json");
        ClickHouseFile file = ClickHouseFile.of(tmp);
        ClickHouseClient.dump(server,
                ClickHouseUtils.format(
                        "select number a, if(modulo(number, 2) = 0, null, toString(number)) b from numbers(%d)",
                        rows),
                file).get();
        Assert.assertTrue(Files.exists(tmp), ClickHouseUtils.format("File [%s] should exist", tmp));
        Assert.assertTrue(Files.size(tmp) > 0, ClickHouseUtils.format("File [%s] should have content", tmp));

        ClickHouseClient.load(server, "test_dump_load_file", file).get();

        try (ClickHouseClient client = getClient();
                ClickHouseResponse response = client.connect(server).query("select count(1) from test_dump_load_file")
                        .executeAndWait()) {
            Assert.assertEquals(response.firstRecord().getValue(0).asInteger(), rows);
        }

        try (ClickHouseClient client = getClient();
                ClickHouseResponse response = client.connect(server)
                        .query("select count(1) from test_dump_load_file where b is null")
                        .executeAndWait()) {
            Assert.assertEquals(response.firstRecord().getValue(0).asInteger(), rows / 2);
        }
    }

    @Test(groups = { "integration" })
    public void testDump() throws Exception {
        ClickHouseNode server = getServer();

        Path temp = Files.createTempFile("dump", ".tsv");
        Assert.assertEquals(Files.size(temp), 0L);

        int lines = 10000;
        ClickHouseResponseSummary summary = ClickHouseClient.dump(server,
                ClickHouseUtils.format("select * from numbers(%d)", lines),
                ClickHouseFormat.TabSeparated, ClickHouseCompression.NONE, temp.toString()).get();
        Assert.assertNotNull(summary);
        // Assert.assertEquals(summary.getReadRows(), lines);

        int counter = 0;
        for (String line : Files.readAllLines(temp)) {
            Assert.assertEquals(String.valueOf(counter++), line);
        }
        Assert.assertEquals(counter, lines);

        Files.delete(temp);
    }

    @Test(groups = { "integration" })
    public void testCustomLoad() throws Exception {
        ClickHouseNode server = getServer();

        ClickHouseClient.send(server, "drop table if exists test_custom_load",
                "create table test_custom_load(n UInt32, s Nullable(String)) engine = Memory").get();

        ClickHouseClient.load(server, "test_custom_load", ClickHouseFormat.TabSeparated,
                ClickHouseCompression.NONE, new ClickHouseWriter() {
                    @Override
                    public void write(ClickHouseOutputStream output) throws IOException {
                        output.write("1\t\\N\n".getBytes(StandardCharsets.US_ASCII));
                        output.write("2\t123".getBytes(StandardCharsets.US_ASCII));
                    }
                }).get();

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = client.connect(server).query("select * from test_custom_load order by n")
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).execute().get()) {
            Assert.assertNotNull(resp.getColumns());
            List<String[]> values = new ArrayList<>();
            for (ClickHouseRecord record : resp.records()) {
                String[] arr = new String[2];
                arr[0] = record.getValue(0).asString();
                arr[1] = record.getValue(1).asString();
                values.add(arr);
            }

            Assert.assertEquals(values.size(), 2);
            Assert.assertEquals(values.get(0), new String[] { "1", null });
            Assert.assertEquals(values.get(1), new String[] { "2", "123" });
        }
    }

    @Test(groups = { "integration" })
    public void testLoadCsv() throws Exception {
        ClickHouseNode server = getServer();

        List<ClickHouseResponseSummary> summaries = ClickHouseClient
                .send(server, "drop table if exists test_load_csv",
                        "create table test_load_csv(n UInt32) engine = Memory")
                .get();
        Assert.assertNotNull(summaries);
        Assert.assertEquals(summaries.size(), 2);

        Path temp = Files.createTempFile("data", ".tsv");
        Assert.assertEquals(Files.size(temp), 0L);

        int lines = 10000;
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < lines; i++) {
            builder.append(i).append('\n');
        }
        Files.write(temp, builder.toString().getBytes(StandardCharsets.US_ASCII));
        Assert.assertTrue(Files.size(temp) > 0L);

        ClickHouseResponseSummary summary = ClickHouseClient.load(server, "test_load_csv",
                ClickHouseFormat.TabSeparated, ClickHouseCompression.NONE, temp.toString()).get();
        Assert.assertNotNull(summary);
        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = client.connect(server).query("select count(1) from test_load_csv").execute()
                        .get()) {
            Assert.assertEquals(resp.firstRecord().getValue(0).asInteger(), lines);
        }

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = client.connect(server)
                        .query("select min(n), max(n), count(1), uniqExact(n) from test_load_csv")
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).execute().get()) {
            Assert.assertNotNull(resp.getColumns());
            for (ClickHouseRecord record : resp.records()) {
                Assert.assertNotNull(record);
                Assert.assertEquals(record.getValue(0).asLong(), 0L);
                Assert.assertEquals(record.getValue(1).asLong(), lines - 1);
                Assert.assertEquals(record.getValue(2).asLong(), lines);
                Assert.assertEquals(record.getValue(3).asLong(), lines);
            }
        } finally {
            Files.delete(temp);
        }
    }

    @Test(groups = { "integration" })
    public void testLoadRawData() throws Exception {
        ClickHouseNode server = getServer();
        ClickHouseClient.send(server, "drop table if exists test_load_raw_data",
                "create table test_load_raw_data(a Int64)engine=Memory").get();
        int rows = 10;

        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest.Mutation request = client.connect(server).write()
                    .table("test_load_raw_data")
                    .format(ClickHouseFormat.RowBinary)
                    // this is needed to get meaningful response summary
                    .set("send_progress_in_http_headers", 1);
            ClickHouseConfig config = request.getConfig();

            CompletableFuture<ClickHouseResponse> future;
            // single producer → single consumer
            // important to close the stream *before* retrieving response
            try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                    .createPipedOutputStream(config, null)) {
                // start the worker thread which transfer data from the input into ClickHouse
                future = request.data(stream.getInputStream()).send();
                // write bytes into the piped stream
                for (int i = 0; i < rows; i++) {
                    BinaryStreamUtils.writeInt64(stream, i);
                }
            }

            ClickHouseResponseSummary summary;
            try (ClickHouseResponse response = future.get()) {
                summary = response.getSummary();
            }

            Assert.assertEquals(summary.getWrittenRows(), rows);
        }
    }

    @Test(groups = { "integration" })
    public void testMultipleQueries() throws Exception {
        ClickHouseNode server = getServer();
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> req = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes);

            int result1 = 1;
            int result2 = 2;
            ClickHouseResponse queryResp = req.copy().query("select 1").execute().get();

            try (ClickHouseResponse resp = req.copy().query("select 2").execute().get()) {
                Assert.assertEquals(resp.firstRecord().getValue(0).asInteger(), result2);
            }

            result2 = 0;
            for (ClickHouseRecord r : queryResp.records()) {
                Assert.assertEquals(r.getValue(0).asInteger(), result1);
                result2++;
            }
            Assert.assertEquals(result2, 1, "Should have only one record");
        }
    }

    @Test(groups = { "integration" })
    public void testExternalTableAsParameter() throws Exception {
        ClickHouseNode server = getServer();
        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query("select toString(number) as query_id from numbers(100) "
                                + "where query_id not in (select query_id from ext_table) limit 10")
                        .external(ClickHouseExternalTable.builder().name("ext_table")
                                .columns("query_id String, a_num Nullable(Int32)").format(ClickHouseFormat.CSV)
                                .content(new ByteArrayInputStream(
                                        "\"1,2,3\",\\N\n2,333".getBytes(StandardCharsets.US_ASCII)))
                                .build())
                        .execute().get()) {
            for (ClickHouseRecord r : resp.records()) {
                Assert.assertNotNull(r);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testInsertWithInputFunction() throws Exception {
        ClickHouseNode server = getServer();
        ClickHouseClient.send(server, "drop table if exists test_input_function",
                "create table test_input_function(name String, value Nullable(Int32))engine=Memory").get();

        try (ClickHouseClient client = getClient()) {
            // default format ClickHouseFormat.TabSeparated
            ClickHouseRequest<?> req = client.connect(server);
            try (ClickHouseResponse resp = req.write().query(
                    "insert into test_input_function select col2, col3 from "
                            + "input('col1 UInt8, col2 String, col3 Int32')")
                    .data(new ByteArrayInputStream("1\t2\t33\n2\t3\t333".getBytes(StandardCharsets.US_ASCII))).execute()
                    .get()) {

            }

            List<Object[]> values = new ArrayList<>();
            try (ClickHouseResponse resp = req.query("select * from test_input_function").execute().get()) {
                for (ClickHouseRecord r : resp.records()) {
                    values.add(new Object[] { r.getValue(0).asObject() });
                }
            }
            Assert.assertEquals(values.toArray(new Object[0][]),
                    new Object[][] { new Object[] { "2\t33" }, new Object[] { "3\t333" } });
        }
    }

    @Test(dataProvider = "renameMethods", groups = "integration")
    public void testRenameResponseColumns(ClickHouseRenameMethod m, String col1, String col2, String col3)
            throws Exception {
        ClickHouseNode server = getServer();
        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = client.connect(server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .option(ClickHouseClientOption.RENAME_RESPONSE_COLUMN, m)
                        .query("select 1 `a b c`, 2 ` `, 3 `d.E_f`").execute().get()) {
            Assert.assertEquals(resp.getColumns().get(0).getColumnName(), col1);
            Assert.assertEquals(resp.getColumns().get(1).getColumnName(), col2);
            Assert.assertEquals(resp.getColumns().get(2).getColumnName(), col3);
        }
    }

    @Test(groups = "integration")
    public void testTempTable() throws Exception {
        ClickHouseNode server = getServer();
        String sessionId = UUID.randomUUID().toString();
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> request = client.connect(server).format(ClickHouseFormat.RowBinary)
                    .session(sessionId);
            request.query("drop temporary table if exists my_temp_table").execute().get();
            request.query("create temporary table my_temp_table(a Int8)").execute().get();
            request.query("insert into my_temp_table values(2)").execute().get();
            request.write().table("my_temp_table").data(new ByteArrayInputStream(new byte[] { 3 })).execute().get();
            int count = 0;
            try (ClickHouseResponse resp = request.format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .query("select * from my_temp_table order by a").execute().get()) {
                for (ClickHouseRecord r : resp.records()) {
                    Assert.assertEquals(r.getValue(0).asInteger(), count++ == 0 ? 2 : 3);
                }
            }
            Assert.assertEquals(count, 2);
        }
    }
}
