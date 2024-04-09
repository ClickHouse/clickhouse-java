package com.clickhouse.client.query;


import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.RowBinaryReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseRecord;
import lombok.SneakyThrows;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class QueryTests extends BaseIntegrationTest {


    private Client client;

    @BeforeMethod(groups = { "unit" }, enabled = false)
    public void setUp() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = new Client.Builder()
                .addEndpoint(node.getHost() + ":" + node.getPort())
                .addUsername("default")
                .addPassword("")
                .build();
    }

    @Test(groups = { "unit" }, enabled = false)
    public void testSelectQueryOpenDataFormat() {
        QuerySettings settings = new QuerySettings()
                .setFormat("JSON")
                .setSetting("format_json_quote_64bit_integers", "true");

        Map<String, Object> qparams = new HashMap<>();
        qparams.put("param1", "value1");
        qparams.put("param2", "value2");

        Future<QueryResponse> response = client.query("SELECT * FROM default.mytable WHERE param1 = :param1 AND param2 = :param2",
                qparams, settings);


        /// var dataSet = JSONParser.parse(response.getInputStream());

    }
    @Test(groups = { "integration" }, enabled = false)
    public void testSelectQueryClosedBinaryFormat() {
        prepareDataSet();

        QuerySettings settings = new QuerySettings()
                .setFormat("JSON")
                .setSetting("format_json_quote_64bit_integers", "true");

        Map<String, Object> qparams = new HashMap<>();
        qparams.put("param1", "value1");
        qparams.put("param2", "value2");

        Future<QueryResponse> response = client.query("SELECT * FROM default.mytable WHERE param1 = :param1 AND param2 = :param2",
                qparams, settings);

        List<ClickHouseRecord> records = new ArrayList<>();
        try {
            RowBinaryReader reader = new RowBinaryReader(response.get().getInputStream());
            reader.readBatch(100, records::add, System.out::println);
        } catch (InterruptedException | ExecutionException e) {
            Assert.fail("failed to get response", e);
        }
    }

    @SneakyThrows
    private void prepareDataSet() {

        ClickHouseClient client = ClickHouseClient.builder().config(new ClickHouseConfig())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();

        ClickHouseRequest request = client.read(getServer(ClickHouseProtocol.HTTP))
                .query("CREATE TABLE IF NOT EXISTS default.query_test_table (param1 UInt32, param2 UInt32) ENGINE = Memory");
        request.executeAndWait();

        request = client.write(getServer(ClickHouseProtocol.HTTP))
                .query("INSERT INTO default.query_test_table VALUES (1, 2), (3, 4), (5, 6)");
        request.executeAndWait();

        ClickHouseResponse response = client.read(getServer(ClickHouseProtocol.HTTP))
                .query("SELECT * FROM default.query_test_table")
                .executeAndWait();

        Iterator<ClickHouseRecord> iter = response.records().iterator();
        while (iter.hasNext()) {
            ClickHouseRecord r = iter.next();
            if (r.size() > 0) {
                System.out.println(r.getValue(0).asString());
            }
        }
    }
}
