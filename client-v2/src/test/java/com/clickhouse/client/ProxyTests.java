package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.enums.ProxyType;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.insert.SamplePOJO;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static org.testng.Assert.fail;

public class ProxyTests extends BaseIntegrationTest{
    private Client client;

    private WireMockServer proxy = new WireMockServer(8666);
    @BeforeMethod(groups = { "integration" })
    public void setUp() throws IOException {
        proxy.start();
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        proxy.addStubMapping(post(urlMatching("/.*"))
                .willReturn(aResponse().proxiedFrom("http://localhost:" + node.getPort())).build());
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, "clickhouse", 8123, false)
                .setUsername("default")
                .setPassword("")
//                .useNewImplementation(true)
                .addProxy(ProxyType.HTTP, "localhost", 8666)
                .build();
    }

    @AfterMethod(groups = { "integration" })
    public void teardown() {
        proxy.stop();
    }

    private void createTable(String tableQuery) throws ClickHouseException {
        try (ClickHouseClient client = ClickHouseClient.builder().config(new ClickHouseConfig())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build()) {
            client.read(getServer(ClickHouseProtocol.HTTP)).query(tableQuery).executeAndWait().close();
        }
    }
    
    @Test(groups = { "integration" })
    public void testSimpleQuery() throws Exception {
        List<GenericRecord> records = client.queryAll("select timezone()");
        Assert.assertEquals(records.stream().findFirst().get().getString(1), "UTC");
    }

    @Test(groups = { "integration" })
    public void testInsert() throws Exception {
        String tableName = "simple_pojo_disable_proxy_table";
        String createSQL = SamplePOJO.generateTableCreateSQL(tableName);
        System.out.println(createSQL);
        createTable(createSQL);

        client.register(SamplePOJO.class, client.getTableSchema(tableName, "default"));
        List<Object> simplePOJOs = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            simplePOJOs.add(new SamplePOJO());
        }

        try {
            InsertResponse response = client.insert(tableName, simplePOJOs).get(120, TimeUnit.SECONDS);
            Assert.assertEquals(response.getWrittenRows(), 1000);
        } catch (Exception e) {
            fail("Should not have thrown exception.", e);
        }
    }
}
