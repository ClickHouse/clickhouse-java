package com.clickhouse.client.api.data_formats;

public class GsonJSONEachRowFormatReaderTest extends AbstractJSONEachRowFormatReaderTest {
    @Override
    protected String getProcessor() {
        return "GSON";
    }
}
