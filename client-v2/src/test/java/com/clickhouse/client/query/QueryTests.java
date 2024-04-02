package com.clickhouse.client.query;


import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.DataFormat;
import com.clickhouse.client.api.data_formats.JSON;
import com.clickhouse.client.api.data_formats.RowBinary;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
    public void testSelectQuery() {
        QuerySettings<?>[] settings = new QuerySettings[] {
                new QuerySettings.Compression(QuerySettings.Compression.Method.LZ4),
                new QuerySettings.ReadTimeout(3000),
                new QuerySettings.QueryID("123456"),
                new QuerySettings<>("experimental.adaptive_super_compression", "true") // not yet supported by the client
        };

        JSON textFormat = new JSON();
        textFormat.setSetting("format_json_quote_64bit_integers", "true");
        RowBinary binaryFormat = new RowBinary();


        QueryResponse<JSON> response = client.query("SELECT * FROM default.mytable",textFormat, settings);

    }

}
