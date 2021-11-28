package com.clickhouse.client.http;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseParameterizedQuery;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.client.data.ClickHouseStringValue;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseHttpClientTest extends BaseIntegrationTest {
    @Test(groups = { "integration" })
    public void testPing() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            Assert.assertTrue(client.ping(server, 1000));
        }
    }

    @Test(groups = { "integration" })
    public void testMutation() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);
        ClickHouseClient.send(server, "drop table if exists test_data_load",
                "create table test_data_load(a String, b Nullable(Int64))engine=Memory").get();
        try (ClickHouseClient client = ClickHouseClient.newInstance();
                ClickHouseResponse response = client.connect(server).set("send_progress_in_http_headers", 1)
                        .query("insert into test_data_load select toString(number), number from numbers(1)").execute()
                        .get()) {
            ClickHouseResponseSummary summary = response.getSummary();
            Assert.assertEquals(summary.getWrittenRows(), 1);
        }
    }

    @Test(groups = { "integration" })
    public void testMultipleQueries() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);
        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseRequest<?> req = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes);

            ClickHouseResponse queryResp = req.copy().query("select 1").execute().get();

            try (ClickHouseResponse resp = req.copy().query("select 2").execute().get()) {
            }

            for (ClickHouseRecord r : queryResp.records()) {
                continue;
            }
        }
    }

    @Test(groups = { "integration" })
    public void testExternalTableAsParameter() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);
        try (ClickHouseClient client = ClickHouseClient.newInstance();
                ClickHouseResponse resp = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query("select toString(number) as query_id from numbers(100) where query_id not in (select query_id from ext_table) limit 10")
                        .external(ClickHouseExternalTable.builder().name("ext_table")
                                .columns("query_id String, a_num Nullable(Int32)").format(ClickHouseFormat.CSV)
                                .content(new ByteArrayInputStream("\"1,2,3\",\\N\n2,333".getBytes())).build())
                        .execute().get()) {
            for (ClickHouseRecord r : resp.records()) {
                Assert.assertNotNull(r);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testInsertWithInputFunction() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);
        ClickHouseClient.send(server, "drop table if exists test_input_function",
                "create table test_input_function(name String, value Nullable(Int32))engine=Memory").get();

        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            // default format ClickHouseFormat.TabSeparated
            ClickHouseRequest<?> req = client.connect(server);
            try (ClickHouseResponse resp = req.write().query(
                    "insert into test_input_function select col2, col3 from input('col1 UInt8, col2 String, col3 Int32')")
                    .data(new ByteArrayInputStream("1\t2\t33\n2\t3\t333".getBytes())).execute().get()) {

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

    @Test(groups = { "integration" })
    public void testLogComment() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);
        String uuid = UUID.randomUUID().toString();
        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseRequest<?> request = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes);
            try (ClickHouseResponse resp = request
                    .option(ClickHouseClientOption.LOG_LEADING_COMMENT, true)
                    .query("-- select something\r\nselect 1", uuid).execute().get()) {
            }

            try (ClickHouseResponse resp = request
                    .option(ClickHouseClientOption.LOG_LEADING_COMMENT, true)
                    .query("SYSTEM FLUSH LOGS", uuid).execute().get()) {
            }

            try (ClickHouseResponse resp = request
                    .option(ClickHouseClientOption.LOG_LEADING_COMMENT, true)
                    .query(ClickHouseParameterizedQuery
                            .of("select log_comment from system.query_log where query_id = :qid"))
                    .params(ClickHouseStringValue.of(uuid)).execute().get()) {
                int counter = 0;
                for (ClickHouseRecord r : resp.records()) {
                    Assert.assertEquals(r.getValue(0).asString(), "select something");
                    counter++;
                }
                Assert.assertEquals(counter, 2);
            }
        }
    }

    @Test(groups = "integration")
    public void testQueryWithMultipleExternalTables() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

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
    public void testPost() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (ClickHouseClient client = ClickHouseClient.builder()
                .defaultCredentials(ClickHouseCredentials.fromUserAndPassword("foo", "bar")).build()) {
            // why no detailed error message for this: "select 1，2"
            try (ClickHouseResponse resp = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .query("select 1,2").execute().get()) {
                int count = 0;
                for (ClickHouseRecord r : resp.records()) {
                    Assert.assertEquals(r.getValue(0).asInteger(), 1);
                    Assert.assertEquals(r.getValue(1).asInteger(), 2);
                    count++;
                }

                Assert.assertEquals(count, 1);
            }

            // reuse connection
            try (ClickHouseResponse resp = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .query("select 3,4").execute().get()) {
                int count = 0;
                for (ClickHouseRecord r : resp.records()) {
                    Assert.assertEquals(r.getValue(0).asInteger(), 3);
                    Assert.assertEquals(r.getValue(1).asInteger(), 4);
                    count++;
                }

                Assert.assertEquals(count, 1);
            }
        }
    }
}
