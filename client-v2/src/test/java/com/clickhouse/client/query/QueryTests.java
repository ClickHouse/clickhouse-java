package com.clickhouse.client.query;


import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.JSON;
import com.clickhouse.client.api.data_formats.RowBinary;
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

    @BeforeMethod(groups = { "unit" })
    public void setUp() {
        client = new Client.Builder()
                .addEndpoint("http://localhost:8123")
                .addUsername("default")
                .addPassword("password")
                .build();
    }

    @Test(groups = { "unit" })
    public void testSelectQueryOpenDataFormat() {
        QuerySettings settings = QuerySettings.builder()
                .compressionMethod(QuerySettings.CompressionMethod.LZ4)
                .readTimeout(1000)
                .build();

        JSON textFormat = new JSON();
        textFormat.setSetting("format_json_quote_64bit_integers", "true");

        Map<String, Object> qparams = Map.of("param1", "value1", "param2", "value2");

        QueryResponse<JSON> response = client.query("SELECT * FROM default.mytable WHERE param1 = :param1 AND param2 = :param2",
                qparams, textFormat, settings);


        /// var dataSet = JSONParser.parse(response.getInputStream());

    }
    @Test(groups = { "unit" })
    public void testSelectQueryClosedBinaryFormat() {
        QuerySettings settings = QuerySettings.builder()
                .compressionMethod(QuerySettings.CompressionMethod.LZ4)
                .readTimeout(1000)
                .build();

        RowBinary binaryFormat = new RowBinary();

        Map<String, Object> qparams = Map.of("param1", "value1", "param2", "value2");

        QueryResponse<RowBinary> response = client.query("SELECT * FROM default.mytable WHERE param1 = :param1 AND param2 = :param2",
                qparams, binaryFormat, settings);

        List<ClickHouseRecord> records = new ArrayList<>();
        binaryFormat.createReader(response).readBatch(100, records::add, System.out::println);
    }
}
