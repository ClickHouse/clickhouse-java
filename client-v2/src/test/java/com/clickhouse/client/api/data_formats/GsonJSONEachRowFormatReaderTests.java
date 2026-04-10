package com.clickhouse.client.api.data_formats;

import org.testng.annotations.Test;

@Test(groups = {"integration"})
public class GsonJSONEachRowFormatReaderTests extends AbstractJSONEachRowFormatReaderTests {
    @Override
    protected String getProcessor() {
        return "GSON";
    }
}
