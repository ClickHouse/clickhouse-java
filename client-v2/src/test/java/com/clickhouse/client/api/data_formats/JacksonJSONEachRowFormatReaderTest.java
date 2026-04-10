package com.clickhouse.client.api.data_formats;

public class JacksonJSONEachRowFormatReaderTest extends AbstractJSONEachRowFormatReaderTest {
    @Override
    protected String getProcessor() {
        return "JACKSON";
    }
}
