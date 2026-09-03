package com.clickhouse.client.api;

import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class RequestFormatUnitTest {

    private static final String TSKV_RESPONSE =
            "name=id\ttype=Int32\tdefault_type=\tdefault_expression=\tcomment=\tcodec_expression=\tttl_expression=\n";

    private WireMockServer mockServer;

    private Client client;

    @BeforeMethod
    public void setUp() {
        mockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        mockServer.start();
        mockServer.stubFor(WireMock.post(WireMock.anyUrl())
                .willReturn(WireMock.aResponse().withStatus(200)
                        .withHeader("Content-Type", "text/plain")
                        .withBody(TSKV_RESPONSE)));
        client = new Client.Builder()
                .addEndpoint("http://localhost:" + mockServer.port())
                .setUsername("default")
                .setPassword("")
                .setDefaultDatabase("default")
                .compressServerResponse(false)
                .build();
    }

    @AfterMethod
    public void tearDown() {
        if (client != null) {
            client.close();
        }
        if (mockServer != null) {
            mockServer.stop();
        }
    }

    @Test(dataProvider = "requestFormatData")
    public void testFormatIsRequestedWithHeaderOnly(Consumer<Client> operation, String expectedStatement,
                                                    ClickHouseFormat expectedFormat) {
        operation.accept(client);

        LoggedRequest request = findRequest(expectedStatement);
        Assert.assertEquals(request.getBodyAsString().trim(), expectedStatement);
        Assert.assertEquals(request.getHeader("X-ClickHouse-Format"), expectedFormat.name());
    }

    @DataProvider(name = "requestFormatData")
    public static Object[][] requestFormatData() {
        return new Object[][]{
                {(Consumer<Client>) c -> Assert.assertEquals(
                        c.getTableSchema("test_table", "test_db").getColumns().size(), 1),
                        "DESCRIBE TABLE test_table", ClickHouseFormat.TSKV},
                {(Consumer<Client>) c -> Assert.assertEquals(
                        c.getTableSchemaFromQuery("SELECT id FROM test_table").getColumns().size(), 1),
                        "DESC (SELECT id FROM test_table)", ClickHouseFormat.TSKV},
                {(Consumer<Client>) c -> Assert.assertTrue(c.ping()),
                        "SELECT 1", ClickHouseFormat.TabSeparated},
                // Formats a caller asks for keep flowing through unchanged
                {(Consumer<Client>) c -> runQuery(c, "SELECT 2", null),
                        "SELECT 2", ClickHouseFormat.RowBinaryWithNamesAndTypes},
                {(Consumer<Client>) c -> runQuery(c, "SELECT 3",
                        new QuerySettings().setFormat(ClickHouseFormat.JSONEachRow)),
                        "SELECT 3", ClickHouseFormat.JSONEachRow},
        };
    }

    private static void runQuery(Client client, String sql, QuerySettings settings) {
        try (QueryResponse response = client.query(sql, settings).get(10, TimeUnit.SECONDS)) {
            Assert.assertNotNull(response);
        } catch (Exception e) {
            throw new AssertionError("query failed: " + sql, e);
        }
    }

    private LoggedRequest findRequest(String statement) {
        List<LoggedRequest> requests = mockServer.findAll(WireMock.postRequestedFor(WireMock.anyUrl()));
        for (LoggedRequest request : requests) {
            if (request.getBodyAsString().trim().equals(statement)) {
                return request;
            }
        }
        throw new AssertionError("no request was sent with statement '" + statement + "', sent: " + requests);
    }
}
