package com.clickhouse.client.grpc;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCompression;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;
import com.clickhouse.client.ClickHouseVersion;
import com.clickhouse.client.ClickHouseWriter;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.data.ClickHouseDateTimeValue;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.client.data.ClickHouseIntegerValue;
import com.clickhouse.client.data.ClickHouseIpv4Value;
import com.clickhouse.client.data.ClickHouseIpv6Value;
import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseDataProcessor;
import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseFormat;

public class ClickHouseGrpcClientTest extends BaseIntegrationTest {
    @DataProvider(name = "simpleTypeProvider")
    public Object[][] getSimpleTypes() {
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

    private void execute(ClickHouseRequest<?> request, String sql) throws Exception {
        try (ClickHouseResponse response = request.query(sql).execute().get()) {
            for (ClickHouseRecord record : response.records()) {
                for (ClickHouseValue value : record) {
                    Assert.assertNotNull(value);
                }
            }
        }
    }

    @Test(groups = { "unit" })
    public void testInit() throws Exception {
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.GRPC)) {
            Assert.assertTrue(client instanceof ClickHouseGrpcClient);
        }
    }

    @Test(groups = { "integration" })
    public void testOpenCloseConnection() throws Exception {
        for (int i = 0; i < 100; i++) {
            Assert.assertTrue(ClickHouseClient.newInstance(ClickHouseProtocol.GRPC)
                    .ping(getServer(ClickHouseProtocol.GRPC), 3000));
        }
    }

    @Test(groups = { "integration" })
    public void testQueryWithNoResult() throws Exception {
        String sql = "select * from system.numbers limit 0";

        try (ClickHouseClient client = ClickHouseClient.builder().build()) {
            // header without row
            try (ClickHouseResponse response = client.connect(getServer(ClickHouseProtocol.GRPC))
                    .option(ClickHouseClientOption.MAX_BUFFER_SIZE, 8)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).query(sql).execute().get()) {
                Assert.assertEquals(response.getColumns().size(), 1);
                Assert.assertNotEquals(response.getColumns(), ClickHouseDataProcessor.DEFAULT_COLUMNS);
                for (ClickHouseRecord record : response.records()) {
                    Assert.fail(ClickHouseUtils.format("Should have no record, but we got: %s", record));
                }
            }

            // no header and row
            try (ClickHouseResponse response = client.connect(getServer(ClickHouseProtocol.GRPC))
                    .option(ClickHouseClientOption.MAX_BUFFER_SIZE, 8).format(ClickHouseFormat.RowBinary).query(sql)
                    .execute().get()) {
                Assert.assertEquals(response.getColumns(), Collections.emptyList());
                for (ClickHouseRecord record : response.records()) {
                    Assert.fail(ClickHouseUtils.format("Should have no record, but we got: %s", record));
                }
            }

            // custom header and row
            try (ClickHouseResponse response = client.connect(getServer(ClickHouseProtocol.GRPC))
                    .option(ClickHouseClientOption.MAX_BUFFER_SIZE, 8).format(ClickHouseFormat.RowBinary).query(sql)
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
        ClickHouseNode node = getServer(ClickHouseProtocol.GRPC);

        try (ClickHouseClient client = ClickHouseClient.builder().build()) {
            Assert.assertTrue(client instanceof ClickHouseGrpcClient);

            // "select * from system.data_type_families"
            int limit = 10000;
            String sql = "select number, toString(number) from system.numbers limit " + limit;

            try (ClickHouseResponse response = client.connect(node).option(ClickHouseClientOption.MAX_BUFFER_SIZE, 8)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).set("send_logs_level", "trace")
                    .set("enable_optimize_predicate_expression", 1)
                    .set("log_queries_min_type", "EXCEPTION_WHILE_PROCESSING").query(sql).execute().get()) {
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
        ClickHouseNode server = getServer(ClickHouseProtocol.GRPC);

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

                ClickHouseResponseSummary summary = resp.getSummary();
                Assert.assertEquals(summary.getStatistics().getRows(), 1);
            }
        }
    }

    @Test(groups = "integration")
    public void testQueryIntervalTypes() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.GRPC);

        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.GRPC)) {
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
        ClickHouseNode server = getServer(ClickHouseProtocol.GRPC);

        ClickHouseClient.send(server, "drop table if exists test_datetime_types",
                "create table test_datetime_types(no UInt8, d0 DateTime32, d1 DateTime64(5), d2 DateTime(3)) engine=Memory")
                .get();
        ClickHouseClient.send(server, "insert into test_datetime_types values(:no, :d0, :d1, :d2)",
                new ClickHouseValue[] { ClickHouseIntegerValue.ofNull(),
                        ClickHouseDateTimeValue.ofNull(0, ClickHouseValues.UTC_TIMEZONE),
                        ClickHouseDateTimeValue.ofNull(3, ClickHouseValues.UTC_TIMEZONE),
                        ClickHouseDateTimeValue.ofNull(9, ClickHouseValues.UTC_TIMEZONE) },
                new Object[] { 0, "1970-01-01 00:00:00", "1970-01-01 00:00:00.123456",
                        "1970-01-01 00:00:00.123456789" },
                new Object[] { 1, -1, -1, -1 }, new Object[] { 2, 1, 1, 1 }, new Object[] { 3, 2.1, 2.1, 2.1 }).get();

        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.GRPC);
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
    public void testDropNonExistDb() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.GRPC);

        try {
            ClickHouseClient.send(server, "drop database non_exist_db").get();
            Assert.fail("Exception is excepted");
        } catch (ExecutionException e) {
            ClickHouseException ce = (ClickHouseException) e.getCause();
            Assert.assertEquals(ce.getErrorCode(), 81);
        }
    }

    @Test(groups = "integration")
    public void testReadWriteDomains() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.GRPC);

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
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.GRPC);
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
    public void testReadWriteGeoTypes() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.GRPC);

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
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.GRPC);
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
        ClickHouseNode server = getServer(ClickHouseProtocol.GRPC);

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
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.GRPC);
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
        ClickHouseNode server = getServer(ClickHouseProtocol.GRPC);

        try {
            ClickHouseClient
                    .send(server, "drop table if exists test_map_types",
                            "create table test_map_types(no UInt32, m Map(LowCardinality(String), Int32))engine=Memory")
                    .get();
        } catch (ExecutionException e) {
            // looks like LowCardinality(String) as key is not supported even in 21.8
            Throwable cause = e.getCause();
            Assert.assertTrue(cause instanceof ClickHouseException);
            return;
        }

        // write
        ClickHouseClient.send(server, "insert into test_map_types values (1, {'key1' : 1})").get();
        ClickHouseClient.send(server, "insert into test_map_types values (:n,:m)",
                new String[][] { new String[] { "-1", "{'key-1' : -1}" }, new String[] { "-2", "{'key-2' : -2}" } })
                .get();
        ClickHouseClient.send(server, "insert into test_map_types values (3, :m)",
                Collections.singletonMap("m", "{'key3' : 3}")).get();

        // read
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.GRPC);
                ClickHouseResponse resp = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query("select * except(no) from test_map_types order by no").execute().get()) {
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
        }
    }

    @Test(groups = "integration")
    public void testQueryWithMultipleExternalTables() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.GRPC);

        int tables = 30;
        int rows = 10;
        try (ClickHouseClient client = ClickHouseClient.builder().build()) {
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
            InputStream inputStream = new ByteArrayInputStream(dnExtString.getBytes(Charset.forName("UTF-8")));
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

        try (ClickHouseClient client = ClickHouseClient.builder().build();
                ClickHouseResponse resp = client.connect(server).query(sql.toString())
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).external(extTableList).execute().get()) {
            Assert.assertNotNull(resp.getColumns());
            Assert.assertTrue(tables <= 0 || resp.records().iterator().hasNext());
        }
    }

    @Test(groups = { "integration" })
    public void testMutation() throws Exception {
        ClickHouseNode node = getServer(ClickHouseProtocol.GRPC);

        try (ClickHouseClient client = ClickHouseClient.builder().build()) {
            Assert.assertTrue(client instanceof ClickHouseGrpcClient);

            ClickHouseRequest<?> request = client.connect(node).option(ClickHouseClientOption.MAX_BUFFER_SIZE, 8)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).set("send_logs_level", "trace")
                    .set("enable_optimize_predicate_expression", 1)
                    .set("log_queries_min_type", "EXCEPTION_WHILE_PROCESSING");
            execute(request, "drop table if exists test_grpc_mutation;");
            execute(request, "create table if not exists test_grpc_mutation(a String, b UInt32) engine = Memory;");
            execute(request, "insert into test_grpc_mutation values('a', 1)('b', 2)");
        }
    }

    @Test(groups = { "integration" })
    public void testFormat() throws Exception {
        String sql = "select 1, 2";
        ClickHouseNode node = getServer(ClickHouseProtocol.GRPC);

        try (ClickHouseClient client = ClickHouseClient.builder().build()) {
            try (ClickHouseResponse response = client.connect(node).option(ClickHouseClientOption.MAX_BUFFER_SIZE, 8)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).query(sql).execute().get()) {
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
            try (ClickHouseResponse response = client.connect(node).query(sql).format(ClickHouseFormat.CSV).execute()
                    .get()) {
                String results = new BufferedReader(
                        new InputStreamReader(response.getInputStream(), StandardCharsets.UTF_8)).lines()
                                .collect(Collectors.joining("\n"));
                Assert.assertEquals(results, "1,2");
            }

            try (ClickHouseResponse response = client.connect(node).query(sql).format(ClickHouseFormat.JSONEachRow)
                    .execute().get()) {
                String results = new BufferedReader(
                        new InputStreamReader(response.getInputStream(), StandardCharsets.UTF_8)).lines()
                                .collect(Collectors.joining("\n"));
                Assert.assertEquals(results, "{\"1\":1,\"2\":2}");
            }
        }
    }

    @Test(groups = { "integration" })
    public void testDump() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.GRPC);

        Path temp = Files.createTempFile("dump", ".tsv");
        Assert.assertEquals(Files.size(temp), 0L);

        int lines = 10000;
        ClickHouseResponseSummary summary = ClickHouseClient.dump(server, "select * from system.numbers limit " + lines,
                ClickHouseFormat.TabSeparated, ClickHouseCompression.BROTLI, temp.toString()).get();
        Assert.assertNotNull(summary);
        Assert.assertEquals(summary.getReadRows(), lines);

        int counter = 0;
        for (String line : Files.readAllLines(temp)) {
            Assert.assertEquals(String.valueOf(counter++), line);
        }
        Assert.assertEquals(counter, lines);

        Files.delete(temp);
    }

    @Test(groups = { "integration" })
    public void testCustomLoad() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.GRPC);

        ClickHouseClient.send(server, "drop table if exists test_grpc_custom_load",
                "create table if not exists test_grpc_custom_load(n UInt32, s Nullable(String)) engine = Memory").get();

        ClickHouseClient.load(server, "test_grpc_custom_load", ClickHouseFormat.TabSeparated,
                ClickHouseCompression.NONE, new ClickHouseWriter() {
                    @Override
                    public void write(OutputStream output) throws IOException {
                        output.write("1\t\\N\n".getBytes());
                        output.write("2\t123".getBytes());
                    }
                }).get();

        try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol());
                ClickHouseResponse resp = client.connect(server).query("select * from test_grpc_custom_load order by n")
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
        ClickHouseNode server = getServer(ClickHouseProtocol.GRPC);

        List<ClickHouseResponseSummary> summaries = ClickHouseClient
                .send(server, "drop table if exists test_grpc_load_data",
                        "create table if not exists test_grpc_load_data(n UInt32) engine = Memory")
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
        Files.write(temp, builder.toString().getBytes());
        Assert.assertTrue(Files.size(temp) > 0L);

        ClickHouseResponseSummary summary = ClickHouseClient.load(server, "test_grpc_load_data",
                ClickHouseFormat.TabSeparated, ClickHouseCompression.NONE, temp.toString()).get();
        Assert.assertNotNull(summary);
        Assert.assertEquals(summary.getWrittenRows(), lines);

        try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol());
                ClickHouseResponse resp = client.connect(server)
                        .query("select min(n), max(n), count(1), uniqExact(n) from test_grpc_load_data")
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
}
