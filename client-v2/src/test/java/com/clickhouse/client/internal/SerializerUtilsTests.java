package com.clickhouse.client.internal;

import com.clickhouse.client.api.data_formats.RowBinaryFormatSerializer;
import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import com.clickhouse.data.ClickHouseColumn;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;

import org.testng.Assert;

public class SerializerUtilsTests {

    @Test
    public void testConvertToInteger() {
        int expected = 1640995199; // Unix timestamp for the given date
        Assert.assertEquals(SerializerUtils.convertToInteger("1640995199").intValue(), expected);
        Assert.assertEquals(SerializerUtils.convertToInteger(false).intValue(), 0);
    }

    @Test
    public void testDynamicNullWithDefaultsWritesPreambleAndNothingTag() throws Exception {
        ClickHouseColumn column = ClickHouseColumn.of("dyn", "Dynamic");
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        Assert.assertTrue(RowBinaryFormatSerializer.writeValuePreamble(out, true, column, null));
        SerializerUtils.serializeData(out, null, column);

        Assert.assertEquals(out.toByteArray(), new byte[]{0, 0});
    }
}
