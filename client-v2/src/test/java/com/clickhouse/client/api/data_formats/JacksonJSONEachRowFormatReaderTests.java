package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.query.QueryResponse;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;

@Test(groups = {"integration"})
public class JacksonJSONEachRowFormatReaderTests extends AbstractJSONEachRowFormatReaderTests {

    private JsonParserFactory parserFactory = new JacksonJsonParserFactory();

    @Override
    protected ClickHouseTextFormatReader createReader(QueryResponse response) throws IOException {
        return new JSONEachRowFormatReader(parserFactory.createJsonParser(response.getInputStream()));
    }

    @Override
    protected ClickHouseTextFormatReader createReader(InputStream input) throws IOException {
        return new JSONEachRowFormatReader(parserFactory.createJsonParser(input));
    }
}
