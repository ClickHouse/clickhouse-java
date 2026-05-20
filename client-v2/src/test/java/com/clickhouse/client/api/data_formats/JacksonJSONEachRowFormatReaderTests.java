package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.query.QueryResponse;
import org.testng.annotations.Test;

import java.io.IOException;

@Test(groups = {"integration"})
public class JacksonJSONEachRowFormatReaderTests extends AbstractJSONEachRowFormatReaderTests {

    private JsonParserFactory parserFactory = new JacksonJsonParserFactory();

    @Override
    protected ClickHouseTextFormatReader createReader(QueryResponse response) throws IOException {
        return new JSONEachRowFormatReader(parserFactory.createJsonParser(response.getInputStream()));
    }
}
