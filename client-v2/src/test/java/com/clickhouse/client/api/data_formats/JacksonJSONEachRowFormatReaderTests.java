package com.clickhouse.client.api.data_formats;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"integration"})
public class JacksonJSONEachRowFormatReaderTests extends AbstractJSONEachRowFormatReaderTests {

    @Override
    protected String getProcessor() {
        return "JACKSON";
    }
}
