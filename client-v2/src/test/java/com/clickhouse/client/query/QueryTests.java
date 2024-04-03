package com.clickhouse.client.query;


import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.RowBinaryReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseRecord;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryTests {


    private Client client;

    @BeforeMethod(groups = { "unit" }, enabled = false)
    public void setUp() {
        client = new Client.Builder()
                .addEndpoint("http://localhost:8123")
                .addUsername("default")
                .addPassword("password")
                .build();
    }

    @Test(groups = { "unit" }, enabled = false)
    public void testSelectQueryOpenDataFormat() {
        QuerySettings settings = new QuerySettings.Builder()
                .compressAlgorithm("LZ4")
                .format("JSON")
                .addSetting("format_json_quote_64bit_integers", "true")
                .build();

        Map<String, Object> qparams = Map.of("param1", "value1", "param2", "value2");

        QueryResponse response = client.query("SELECT * FROM default.mytable WHERE param1 = :param1 AND param2 = :param2",
                qparams, settings);


        /// var dataSet = JSONParser.parse(response.getInputStream());

    }
    @Test(groups = { "unit" }, enabled = false)
    public void testSelectQueryClosedBinaryFormat() {
        QuerySettings settings = new QuerySettings.Builder()
                .compressAlgorithm("LZ4")
                .format("RowBinary")
                .build();


        Map<String, Object> qparams = Map.of("param1", "value1", "param2", "value2");

        QueryResponse response = client.query("SELECT * FROM default.mytable WHERE param1 = :param1 AND param2 = :param2",
                qparams, settings);

        List<ClickHouseRecord> records = new ArrayList<>();
        RowBinaryReader reader = new RowBinaryReader(response.getInputStream());
        reader.readBatch(100, records::add, System.out::println);
    }
}
