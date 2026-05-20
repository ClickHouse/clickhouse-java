package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.data.ClickHouseFormat;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.Map;

@Test(groups = {"integration"})
public class GsonJSONEachRowFormatReaderTests extends AbstractJSONEachRowFormatReaderTests {

    private static Gson gson = new GsonBuilder().create();

    private JsonParserFactory parserFactory = new GsonJsonParserFactory();

    @Override
    protected ClickHouseTextFormatReader createReader(QueryResponse response) throws IOException {
        return new JSONEachRowFormatReader(parserFactory.createJsonParser(response.getInputStream()));
    }

    @Test(groups = {"integration"})
    public void testRowToObject() throws Exception {

        TestDTO_1[] data = new TestDTO_1[] {
                new TestDTO_1("key1", 0.2, -0.2, Collections.singletonMap("p1", 10)),
                new TestDTO_1("key2", 0.3, -0.5, Collections.singletonMap("p1", 9)),
        };

        final String table = "test_row_to_object_json";
        final String createStmt = "CREATE TABLE IF NOT EXISTS " + table + " (key String, sensor1 Decimal, sensor2 Decimal, params JSON) Engine MergeTree Order By (key)";
        client.execute(createStmt).get().close();
        client.execute("TRUNCATE " + table).get().close();

        try (InsertResponse response = client.insert(table, out -> {
            try (JsonWriter jsonWriter = gson.newJsonWriter(new OutputStreamWriter(out))) {
                jsonWriter.setLenient(true);
                for (TestDTO_1 value : data) {
                    gson.toJson(value, TestDTO_1.class, jsonWriter);
                }
            }
        }, ClickHouseFormat.JSONEachRow, new InsertSettings()).get()) {
            System.out.println("inserted rows" + response.getWrittenRows() );
        }
    }

    @Data
    @AllArgsConstructor
    public static class TestDTO_1 {

        private String key;

        private Double sensor1;

        private Double sensor2;

        private Map<String, Integer> params;
    }
}
