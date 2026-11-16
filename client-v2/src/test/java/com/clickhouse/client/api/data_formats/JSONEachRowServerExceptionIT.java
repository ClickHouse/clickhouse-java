package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.internal.ClientUtils;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/**
 * Reads a real JSONEachRow body in which the server appends {@code {"exception":"Code: ..."}}
 * after the response has started ({@code http_write_exception_in_output_format}).
 */
@Test(groups = {"integration"})
public class JSONEachRowServerExceptionIT extends BaseIntegrationTest {

    /**
     * Enough rows that the server flushes data before the failure. A short result is buffered and
     * sometimes comes back as HTTP 500 with only {@code {"exception":...}}, which is the same shape
     * as a query that selects a single {@code exception} column and is not this bug.
     */
    private static final String FAILING_SQL =
            "SELECT toInt32(number) AS id FROM numbers(100) "
                    + "WHERE throwIf(number = 50, 'in-band-boom') = 0 "
                    + "SETTINGS http_write_exception_in_output_format = 1, max_block_size = 1 "
                    + "FORMAT JSONEachRow";

    private Client client;

    @BeforeMethod(groups = {"integration"})
    public void setUp() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .build();
    }

    @AfterMethod(groups = {"integration"})
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @DataProvider(name = "parsers")
    public static Object[][] parsers() {
        return new Object[][]{
                {new GsonJsonParserFactory()},
                {new JacksonJsonParserFactory()},
        };
    }

    @Test(groups = {"integration"}, dataProvider = "parsers")
    public void testOptInThrowsTheServerCodeAfterDataRows(JsonParserFactory factory) throws Exception {
        byte[] body = bodyWithDataRowThenException(FAILING_SQL);
        try (JSONEachRowFormatReader reader = reader(factory, body, true)) {
            int dataRows = 0;
            ServerException thrown = null;
            while (reader.hasNext()) {
                try {
                    Map<String, Object> row = reader.next();
                    if (row == null) {
                        break;
                    }
                    Assert.assertTrue(row.containsKey("id"),
                            "data row keys: " + row.keySet() + " body: " + text(body));
                    dataRows++;
                } catch (ServerException e) {
                    thrown = e;
                    break;
                }
            }
            Assert.assertTrue(dataRows >= 1, "rows before the error: " + dataRows + " body: " + text(body));
            Assert.assertNotNull(thrown, "the exception row must not be returned as data: " + text(body));
            Assert.assertTrue(thrown.getMessage().contains("in-band-boom"), thrown.getMessage());
            Assert.assertEquals(thrown.getCode(), ClientUtils.parseServerErrorCode(thrown.getMessage()));
            Assert.assertTrue(thrown.getCode() > 0, "code " + thrown.getCode());
        }
    }

    @Test(groups = {"integration"}, dataProvider = "parsers")
    public void testDetectionOffReturnsTheExceptionRowAsData(JsonParserFactory factory) throws Exception {
        byte[] body = bodyWithDataRowThenException(FAILING_SQL);
        try (JSONEachRowFormatReader reader = reader(factory, body, false)) {
            int rows = 0;
            boolean sawException = false;
            while (reader.hasNext()) {
                Map<String, Object> row = reader.next();
                if (row == null) {
                    break;
                }
                rows++;
                if (row.containsKey("exception")) {
                    sawException = true;
                    Assert.assertTrue(String.valueOf(row.get("exception")).contains("in-band-boom"),
                            String.valueOf(row.get("exception")));
                }
            }
            Assert.assertTrue(sawException, "the exception row must stay data when detection is off: " + text(body));
            Assert.assertTrue(rows >= 2, "data row plus exception row, got " + rows + " body: " + text(body));
        }
    }

    @Test(groups = {"integration"}, dataProvider = "parsers")
    public void testSingleExceptionColumnStaysData(JsonParserFactory factory) throws Exception {
        String sql = "SELECT 'Code: 60. DB::Exception: missing. (UNKNOWN_TABLE)' AS exception";
        try (QueryResponse response = client.query(sql, jsonEachRow()).get();
             JSONEachRowFormatReader reader = reader(factory, response, true)) {
            Map<String, Object> row = reader.next();
            Assert.assertNotNull(row);
            Assert.assertTrue(String.valueOf(row.get("exception")).startsWith("Code: 60"),
                    String.valueOf(row.get("exception")));
            Assert.assertFalse(reader.hasNext());
        }
    }

    private static QuerySettings jsonEachRow() {
        return new QuerySettings()
                .setFormat(ClickHouseFormat.JSONEachRow)
                .serverSetting("http_write_exception_in_output_format", "1")
                .serverSetting("max_block_size", "1");
    }

    private static JSONEachRowFormatReader reader(JsonParserFactory factory, QueryResponse response,
                                                  boolean detectServerException) throws Exception {
        return new JSONEachRowFormatReader(
                factory.createJsonParser(response.getInputStream()), detectServerException);
    }

    /**
     * {@code Client.query()} throws when the response carries {@code X-ClickHouse-Exception-Code},
     * so the caller never sees the body. The reader is used on that body, so the test reads it
     * over HTTP.
     */
    /**
     * Retries while the server answers with only the exception object. That response is HTTP 500
     * with {@code X-ClickHouse-Exception-Code} and is already handled before a caller receives a
     * stream. The reader bug is the HTTP 200 body that starts with data rows.
     */
    private byte[] bodyWithDataRowThenException(String sql) throws Exception {
        byte[] last = new byte[0];
        for (int attempt = 0; attempt < 5; attempt++) {
            last = httpBody(sql);
            String raw = text(last);
            if (raw.contains("\"id\"") && raw.contains("\"exception\"")) {
                return last;
            }
        }
        Assert.fail("no data row before the exception row: " + text(last));
        return last;
    }

    private byte[] httpBody(String sql) throws Exception {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        HttpURLConnection conn = (HttpURLConnection) new URL(
                "http://" + node.getHost() + ":" + node.getPort() + "/").openConnection();
        conn.setRequestMethod("POST");
        String creds = "default:" + ClickHouseServerForTest.getPassword();
        conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
                .encodeToString(creds.getBytes(StandardCharsets.UTF_8)));
        conn.setDoOutput(true);
        byte[] payload = sql.getBytes(StandardCharsets.UTF_8);
        conn.getOutputStream().write(payload);
        int status = conn.getResponseCode();
        InputStream in = status >= 400 ? conn.getErrorStream() : conn.getInputStream();
        Assert.assertNotNull(in, "no body, HTTP " + status);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int n;
        while ((n = in.read(buf)) >= 0) {
            out.write(buf, 0, n);
        }
        in.close();
        return out.toByteArray();
    }

    private static JSONEachRowFormatReader reader(JsonParserFactory factory, byte[] body,
                                                  boolean detectServerException) throws Exception {
        return new JSONEachRowFormatReader(
                factory.createJsonParser(new ByteArrayInputStream(body)), detectServerException);
    }

    private static String text(byte[] body) {
        return new String(body, StandardCharsets.UTF_8);
    }
}
